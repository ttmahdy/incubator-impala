// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exprs/timezone_db.h"

#include <dirent.h>
#include <libgen.h>

#include <iostream>
#include <string>
#include <boost/algorithm/string.hpp>

#include "common/logging.h"
#include "kudu/util/path_util.h"
#include "gutil/strings/ascii_ctype.h"
#include "gutil/strings/substitute.h"
#include "runtime/hdfs-fs-cache.h"
#include "util/filesystem-util.h"
#include "util/hdfs-util.h"
#include "util/string-parser.h"

#include "common/names.h"

using std::ios_base;
using std::istream;
using boost::algorithm::trim;
using kudu::JoinPathSegments;

DEFINE_string(hdfs_zoneinfo_dir, "",
    "HDFS/S3A/ADLS path to load IANA time-zone database from.");
DEFINE_string(hdfs_zoneabbrev_config, "",
    "HDFS/S3A/ADLS path to config file defining non-standard time-zone abbreviations.");
DECLARE_string(local_library_dir);

namespace impala {

static const int HDFS_READ_SIZE = 64 * 1024; // bytes

const string TimezoneDatabase::ZONEINFO_DIR = "/usr/share/zoneinfo";
const string TimezoneDatabase::TIMEZONE_ABBREVIATIONS = \
"#\n"
"# Java supports these non-standard time-zone abbreviations\n"
"#\n"
"ACT: Australia/Darwin\n"
"AET: Australia/Sydney\n"
"AGT: America/Argentina/Buenos_Aires\n"
"ART: Africa/Cairo\n"
"AST: America/Anchorage\n"
"BET: America/Sao_Paulo\n"
"BST: Asia/Dhaka\n"
"CAT: Africa/Harare\n"
"CNT: America/St_Johns\n"
"CST: America/Chicago\n"
"CTT: Asia/Shanghai\n"
"EAT: Africa/Addis_Ababa\n"
"ECT: Europe/Paris\n"
"IET: America/Indiana/Indianapolis\n"
"IST: Asia/Kolkata\n"
"JST: Asia/Tokyo\n"
"MIT: Pacific/Apia\n"
"NET: Asia/Yerevan\n"
"NST: Pacific/Auckland\n"
"PLT: Asia/Karachi\n"
"PNT: America/Phoenix\n"
"PRT: America/Puerto_Rico\n"
"PST: America/Los_Angeles\n"
"SST: Pacific/Guadalcanal\n"
"VST: Asia/Ho_Chi_Minh\n"
"#\n"
"# Additional abbreviations for backward compatibility:\n"
"#\n"
"AEST: Australia/Sydney\n"
"CDT:  America/Chicago\n"
"CEST: CET\n"
"EDT:  EST5EDT\n"
"ICT:  Asia/Ho_Chi_Minh\n"
"KST:  Asia/Seoul\n"
"MDT:  MST7MDT\n"
"PHT:  Asia/Manila\n"
"PDT:  America/Los_Angeles\n";

const cctz::time_zone TimezoneDatabase::UTC_TIMEZONE_ = cctz::utc_time_zone();

TimezoneDatabase::TZ_MAP TimezoneDatabase::tz_name_map_;
string TimezoneDatabase::tz_db_path_;

namespace {

// Returns 'true' if path 'a' starts with path 'b'. If 'relative' is not nullptr, it will
// be set to the part of path 'a' relative to 'b'.
bool PathStartsWith(const string& a, const string& b, string *relative) {
  DCHECK(!a.empty());
  DCHECK(!b.empty() && b.back() != '/');
  if (b.length() + 1 < a.length() && strncmp(b.c_str(), a.c_str(), b.length()) == 0 &&
      a[b.length()] == '/') {
    if (relative != nullptr) *relative = a.substr(b.length() + 1);
    return true;
  }
  return false;
}

// Returns 'true' if 'tz_seg' is a valid time-zone name segment. Time-zone name segments
// can have letters, digits and '_', '-', '+' characters only. Name segments must begin
// with an uppercase letter.
bool IsTimezoneNameSegmentValid(const string& tz_seg) {
  return !tz_seg.empty() && ascii_isupper(tz_seg[0]) &&
      find_if(
          tz_seg.begin() + 1,
          tz_seg.end(),
          [](char c) { return !isalnum(c) && c != '_' && c != '-' && c != '+'; }) == \
              tz_seg.end();
}

// Time-zone abbreviations must be valid time-zone name segments.
const auto IsTimezoneAbbrevValid = IsTimezoneNameSegmentValid;

// Returns 'true' if 'tz_name' is a valid time-zone name. Time-zone names must be valid
// time-zone name segments delimited by '/'.
bool IsTimezoneNameValid(const string& tz_name) {
  size_t beg = 0;
  size_t end = tz_name.find('/', beg);
  while (end != string::npos) {
    if (!IsTimezoneNameSegmentValid(tz_name.substr(beg, end - beg))) return false;
    beg = end + 1;
    end = tz_name.find('/', beg);
  }
  if (!IsTimezoneNameSegmentValid(tz_name.substr(beg))) return false;
  return true;
}

// Parses the UTC offset in 'tz_offset' and returns 'true' if it is valid. If a valid
// offset was found, 'offset_sec' is set to the parsed value.
bool IsTimezoneOffsetValid(const string& tz_offset, int64_t* offset_sec) {
  if (tz_offset.empty()) return false;
  // The absolute value of the offset_sec cannot be greater than or equal to 24 hours.
  constexpr int64_t max_abs_offset_sec = 24*60*60 - 1;
  StringParser::ParseResult result;
  *offset_sec = StringParser::StringToInt<int64_t>(
      tz_offset.c_str(), tz_offset.length(), &result);
  return result == StringParser::PARSE_SUCCESS &&
      *offset_sec <= max_abs_offset_sec && *offset_sec >= -max_abs_offset_sec;
}

}

// The implementation here was adapted from
// https://github.com/HowardHinnant/date/blob/040eed838bb1f695c31c6016dbe74bddc0302bb8/
// src/tz.cpp#L3652
// available under MIT license.
string TimezoneDatabase::LocalZoneName() {
  {
    // Allow ${TZ} to override the default zone.
    const char* zone = ":localtime";

    char* tz_env = nullptr;
    tz_env = getenv("TZ");
    if (tz_env) zone = tz_env;

    // We only support the "[:]<zone-name>" form.
    if (*zone == ':') ++zone;

    if (strcmp(zone, "localtime") != 0) return string(zone);

    // Fall through to try other means.
  }

  {
    // Check /etc/localtime.
    // - If it exists and is a symlink it should point to the current timezone file in the
    // zoneinfo directory.
    // - If it doesn't exist or is not a symlink we want to try other means.
    const char* localtime = "/etc/localtime";
    bool is_symbolic_link;
    string real_path;
    Status status = FileSystemUtil::IsSymbolicLink(localtime, &is_symbolic_link,
        &real_path);
    if (!status.ok()) {
      LOG(WARNING) << status.GetDetail();
    } else if (is_symbolic_link) {
      string linked_tz;
      if (PathStartsWith(real_path, ZONEINFO_DIR, &linked_tz)) {
        return linked_tz;
      } else {
        LOG(WARNING) << "Symbolic link " << localtime << " resolved to the wrong path: "
                     << real_path;
      }
    }
    // Fall through to try other means.
  }

  {
    // On some versions of some linux distro's (e.g. Ubuntu), the current timezone might
    // be in the first line of the /etc/timezone file.
    ifstream timezone_file("/etc/timezone");
    if (timezone_file.is_open()) {
      string result;
      getline(timezone_file, result);
      if (!result.empty()) return result;
    }
    // Fall through to try other means.
  }

  {
    // On some versions of some linux distro's (e.g. Red Hat), the current timezone might
    // be in the first line of the /etc/sysconfig/clock file as: ZONE="US/Eastern"
    ifstream timezone_file("/etc/sysconfig/clock");
    string result;
    while (timezone_file) {
      getline(timezone_file, result);
      auto p = result.find("ZONE=\"");
      if (p != string::npos) {
        result.erase(p, p + 6);
        result.erase(result.rfind('"'));
        return result;
      }
    }
    // Fall through to try other means.
  }

  LOG(WARNING) << "Could not get local timezone.";
  return "";
}

Status TimezoneDatabase::LoadZoneInfoFromHdfs(const string& hdfs_zoneinfo_dir,
    const string& local_dir) {
  DCHECK(!hdfs_zoneinfo_dir.empty());

  hdfsFS hdfs_conn, local_conn;
  RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(hdfs_zoneinfo_dir, &hdfs_conn));
  RETURN_IF_ERROR(HdfsFsCache::instance()->GetLocalConnection(&local_conn));

  // Create a temporary directory to copy the timezone db to. The CCTZ interface only
  // loads timezone info from a directory. We abort the startup if this initialization
  // fails for some reason.
  string pathname = JoinPathSegments(local_dir, "impala.tzdb.XXXXXXX");

  // mkdtemp operates in place, so we need a mutable array.
  vector<char> local_path(pathname.c_str(), pathname.c_str() + pathname.size() + 1);
  if (mkdtemp(local_path.data()) == nullptr) {
    return Status(Substitute("Could not create temporary timezone directory: $0. Check "
        "that the directory $1 is writable by the user running Impala.",
        local_path.data(), local_dir));
  }

  Status copy_status = CopyHdfsFile(hdfs_conn, hdfs_zoneinfo_dir, local_conn,
      local_path.data());
  if (UNLIKELY(!copy_status.ok())) {
    (void)FileSystemUtil::RemovePaths({local_path.data()});
    return copy_status;
  }

  string subdir_name = GetBaseName(hdfs_zoneinfo_dir.c_str());
  string local_zoneinfo_path = JoinPathSegments(local_path.data(), subdir_name);
  LoadZoneInfo(local_zoneinfo_path);

  Status rm_status = FileSystemUtil::RemovePaths({local_path.data()});
  if (UNLIKELY(!rm_status.ok())) LOG(WARNING) << rm_status.GetDetail();
  return Status::OK();
}

void TimezoneDatabase::LoadZoneInfo(const string& zoneinfo_dir) {
  // Find canonical path for 'zoneinfo_dir'.
  char real_zoneinfo_dir[PATH_MAX];
  if (realpath(zoneinfo_dir.c_str(), real_zoneinfo_dir) == nullptr) {
    LOG(WARNING) << "Could not find real path for path " << zoneinfo_dir << ": "
                 << GetStrErrMsg();
    return;
  }

  // Load 'time_zone' objects into 'tz_name_path'.
  TZ_MAP tz_path_map;
  LoadZoneInfoHelper(real_zoneinfo_dir, real_zoneinfo_dir, tz_path_map);

  // Iterate through 'tz_path_map' and add 'time_zone' objects to 'tz_name_map_'.
  // Use time-zone names as keys.
  for (const auto& tz: tz_path_map) {
    string tz_name;
    if (PathStartsWith(tz.first, real_zoneinfo_dir, &tz_name) &&
        IsTimezoneNameValid(tz_name)) {
      tz_name_map_[tz_name] = tz.second;
    } else {
      LOG(WARNING) << "Skipped adding " << tz.first << " to timezone db.";
    }
  }
}

void TimezoneDatabase::LoadZoneInfoHelper(const string& path, const string& zoneinfo_dir,
    TZ_MAP& tz_path_map) {
  DIR *dir_stream = opendir(path.c_str());
  if (dir_stream != nullptr) {
    dirent *dir_entry;
    while ((dir_entry = readdir(dir_stream)) != nullptr) {
      if (dir_entry->d_name == nullptr ||
          !IsTimezoneNameSegmentValid(dir_entry->d_name)) {
        continue;
      }

      const string entry_path = JoinPathSegments(path, dir_entry->d_name);
      Status is_dir = FileSystemUtil::VerifyIsDirectory(entry_path);
      if (is_dir.ok()) {
        // 'entry_path' is a directory. Load 'time_zone' objects from the directory
        // recursively.
        LoadZoneInfoHelper(entry_path, zoneinfo_dir, tz_path_map);
      } else {
        // Load time-zone from 'entry_path'.
        LoadTimezone(entry_path, zoneinfo_dir, tz_path_map);
      }
    }
    closedir(dir_stream);
  }
}

void TimezoneDatabase::LoadTimezone(const string& path, const string& zoneinfo_dir,
    TZ_MAP& tz_path_map) {
  bool is_symbolic_link;
  string real_path;
  Status status = FileSystemUtil::IsSymbolicLink(path, &is_symbolic_link, &real_path);
  if (!status.ok()) {
    LOG(WARNING) << status.GetDetail();
    return;
  }

  // 'path' is not a symbolic link. Read 'time_zone' from 'path'.
  if (!is_symbolic_link) {
    shared_ptr<cctz::time_zone> tz = LoadTimezoneHelper(path);
    if (tz != nullptr) tz_path_map[path] = tz;
    return;
  }

  // 'path' is a symbolic link. Check that the real path is also under 'zoneinfo_dir'.
  if (!PathStartsWith(real_path, zoneinfo_dir, nullptr)) {
    LOG(WARNING) << "Symbolic link " << path << " resolved to the wrong path: "
                 << real_path;
    return;
  }

  // Check if 'real_path' has already been added as a key.
  auto it = tz_path_map.find(real_path);
  if (it != tz_path_map.end()) {
    tz_path_map[path] = it->second;
    return;
  }

  // 'real_path' hasn't been added as a key yet. Load 'time_zone' object and add it to
  // 'tz_path_map' as a value mapped both to 'path' and 'real_path'.
  shared_ptr<cctz::time_zone> tz = LoadTimezoneHelper(real_path);
  if (tz != nullptr) {
    tz_path_map[real_path] = tz_path_map[path] = tz;
  }
}

shared_ptr<cctz::time_zone> TimezoneDatabase::LoadTimezoneHelper(const string& path) {
  shared_ptr<cctz::time_zone> tz = make_shared<cctz::time_zone>();
  if (!cctz::load_time_zone(path, tz.get())) {
    LOG(WARNING) << "Could not load timezone: " << path;
    return nullptr;
  }
  return tz;
}

Status TimezoneDatabase::LoadZoneAbbreviationsFromHdfs(
    const string& hdfs_zoneabbrev_config) {
  DCHECK(!hdfs_zoneabbrev_config.empty());

  hdfsFS hdfs_conn;
  RETURN_IF_ERROR(
      HdfsFsCache::instance()->GetConnection(hdfs_zoneabbrev_config, &hdfs_conn));

  hdfsFile hdfs_file = hdfsOpenFile(
      hdfs_conn, hdfs_zoneabbrev_config.c_str(), O_RDONLY, 0, 0, 0);
  if (hdfs_file == nullptr) {
    return Status(GetHdfsErrorMsg("Failed to open HDFS file for reading: ",
        hdfs_zoneabbrev_config));
  }

  Status status = Status::OK();
  vector<char> buffer(HDFS_READ_SIZE);
  int current_bytes_read = -1;
  stringstream ss;
  while (true) {
    current_bytes_read = hdfsRead(hdfs_conn, hdfs_file, buffer.data(), buffer.size());
    if (current_bytes_read == 0) break;
    if (current_bytes_read < 0) {
      status = Status(TErrorCode::DISK_IO_ERROR,
          GetHdfsErrorMsg("Error reading from HDFS file: ", hdfs_zoneabbrev_config));
      break;
    }
    ss << string(buffer.data(), current_bytes_read);
  }

  int hdfs_ret = hdfsCloseFile(hdfs_conn, hdfs_file);
  if (hdfs_ret != 0) {
    status.MergeStatus(
        Status(
            ErrorMsg(TErrorCode::GENERAL,
            GetHdfsErrorMsg("Failed to close HDFS file: ", hdfs_zoneabbrev_config))));
  }

  if (status.ok()) {
    RETURN_IF_ERROR(LoadZoneAbbreviations(ss, hdfs_zoneabbrev_config.c_str()));
  }
  return status;
}

Status TimezoneDatabase::LoadZoneAbbreviations(istream &is,
    const char* path /* = nullptr */) {
  string line, abbrev, value;
  const string err_msg_path_part = (path == nullptr) ? "" :  string(" in ") + path;
  int i = 0;

  while (is.good() && !is.eof()) {
    i++;
    getline(is, line);

    // Strip off comments.
    size_t comment = line.find('#');
    if (comment != string::npos) {
      line.resize(comment);
    }
    trim(line);
    if (line.empty()) continue;

    // Parse lines formatted as "abbreviation : value".
    size_t colon = line.find(':');
    if (colon == string::npos) {
      return Status(Substitute("Error in line $0$1. ':' is missing.", i,
          err_msg_path_part));
    }

    // Check if abbreviation name is valid.
    abbrev = line.substr(0, colon);
    trim(abbrev);
    if (!IsTimezoneAbbrevValid(abbrev)) {
      return Status(Substitute("Error in line $0$1. Time-zone abbreviation name is "
          "missing or not valid: $2", i, err_msg_path_part, abbrev));
    }

    // Check if abbreviation is already in 'tz_name_map_'.
    if (tz_name_map_.find(abbrev) != tz_name_map_.end()) {
      LOG(WARNING) << "Skipping line " << i << err_msg_path_part
                   << ". Duplicate time-zone abbreviation: " << abbrev;
      continue;
    }

    // Value is either a fix offset in seconds or a time-zone name.
    value = line.substr(colon + 1, string::npos);
    trim(value);
    if (value.empty()) {
      return Status(Substitute("Error in line $0$1. Missing value.", i,
          err_msg_path_part));
    }

    int64_t offset_sec;
    if (IsTimezoneOffsetValid(value, &offset_sec)) {
      // Add time-zone with a fix offset to the map.
      shared_ptr<cctz::time_zone> tz = make_shared<cctz::time_zone>(
          cctz::fixed_time_zone(cctz::sys_seconds(offset_sec)));
      tz_name_map_[abbrev] = tz;
    } else {
      // Check if the value is already in the map.
      auto it_value = tz_name_map_.find(value);
      if (it_value == tz_name_map_.end()) {
        return Status(Substitute("Error in line $0$1. Unknown time-zone name or invalid "
            "offset: $2", i, err_msg_path_part, value));
      } else {
        tz_name_map_[abbrev] = it_value->second;
      }
    }
  }

  return Status::OK();
}

Status TimezoneDatabase::Initialize() {
  // Load 'time_zone' objects into 'tz_name_map_'. Use paths as keys.
  if (!FLAGS_hdfs_zoneinfo_dir.empty()) {
    tz_db_path_ = FLAGS_hdfs_zoneinfo_dir;
    RETURN_IF_ERROR(
        LoadZoneInfoFromHdfs(FLAGS_hdfs_zoneinfo_dir, FLAGS_local_library_dir));
  } else {
    tz_db_path_ = ZONEINFO_DIR;
    LoadZoneInfo(ZONEINFO_DIR);
  }

  // Sanity check.
  if (tz_name_map_.find("UTC") == tz_name_map_.end()) {
    return Status("Failed to load UTC timezone info.");
  }

  // Add timezone abbreviations.
  if (!FLAGS_hdfs_zoneabbrev_config.empty()) {
    RETURN_IF_ERROR(LoadZoneAbbreviationsFromHdfs(FLAGS_hdfs_zoneabbrev_config));
  } else {
    stringstream ss(TIMEZONE_ABBREVIATIONS, ios_base::in);
    RETURN_IF_ERROR(LoadZoneAbbreviations(ss));
  }

  return Status::OK();
}

}
