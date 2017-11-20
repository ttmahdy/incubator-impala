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

#ifndef IMPALA_EXPRS_TIMEZONE_DB_H
#define IMPALA_EXPRS_TIMEZONE_DB_H

#include <boost/unordered_map.hpp>

#include "common/status.h"
#include "cctz/time_zone.h"

namespace impala {

/// Functions to load and access the time-zone database. Initialize() should be called on
/// process startup to load every time-zone rule to memory. After it returned without
/// error, other functions can be safely called from multiple threads.
class TimezoneDatabase {
 public:
  /// Set up the static time-zone database.
  static Status Initialize() WARN_UNUSED_RESULT;

  /// Return path to time-zone database.
  static const string &GetPath() { return tz_db_path_; }

  /// Returns name of the local time-zone.
  static std::string LocalZoneName();

  /// Looks up 'time_zone' object by name. Returns pointer to the 'time_zone' object if
  /// the lookup was successful and nullptr otherwise.
  static const cctz::time_zone* FindTimezone(const std::string& tz_name) {
    auto it = tz_name_map_.find(tz_name);
    return (it == tz_name_map_.end()) ? nullptr : it->second.get();
  }

  static const cctz::time_zone& GetUtcTimezone() { return UTC_TIMEZONE_; }

 private:
  static const std::string ZONEINFO_DIR;
  static const std::string TIMEZONE_ABBREVIATIONS;
  static const cctz::time_zone UTC_TIMEZONE_;

  typedef boost::unordered_map<std::string, std::shared_ptr<cctz::time_zone> > TZ_MAP;

  static TZ_MAP tz_name_map_;
  static std::string tz_db_path_;

  /// Load 'time_zone' objects into 'tz_name_map_' from the shared 'hdfs_zoneinfo_dir'
  /// location.
  static Status LoadZoneInfoFromHdfs(const std::string& hdfs_zoneinfo_dir,
      const std::string& local_dir) WARN_UNUSED_RESULT;

  /// Load 'time_zone' objects into 'tz_name_map_' from 'zoneinfo_dir' path.
  static void LoadZoneInfo(const std::string& zoneinfo_dir);

  /// Recursive function to load 'time_zone' objects into 'tz_path_map' from 'path'.
  /// 'zoneinfo_dir' is the root directory of the time-zone db.
  static void LoadZoneInfoHelper(const std::string& path, const std::string& zoneinfo_dir,
      TZ_MAP& tz_path_map);

  /// Load 'time_zone' object from file 'path'. 'zoneinfo_dir' is the root directory of
  /// the time-zone db.
  /// - If 'path' is not a symbolic link, load 'time_zone' object from 'path' and add it
  /// to 'tz_path_map' as a value mapped to 'path'.
  /// - If 'path' is a symbolic link to another time-zone file, load 'time_zone' object
  /// from the linked path and add it to 'tz_path_map' as a value mapped both to 'path'
  /// and the linked path.
  static void LoadTimezone(const std::string& path, const std::string& zoneinfo_dir,
      TZ_MAP& tz_path_map);

  /// Load 'time_zone' object from file 'path'. If successful, return 'shared_ptr' to the
  /// 'time_zone' object and nullptr otherwise.
  static std::shared_ptr<cctz::time_zone> LoadTimezoneHelper(const std::string& path);

  /// Load custom time-zone abbreviations from 'hdfs_zoneabbrev_config' shared file and
  /// add them to 'tz_name_map_'.
  static Status LoadZoneAbbreviationsFromHdfs(
      const string& hdfs_zoneabbrev_config) WARN_UNUSED_RESULT;

  /// Load custom time-zone abbreviations from 'is' and add them to 'tz_name_map_'.
  static Status LoadZoneAbbreviations(
      std::istream &is, const char* path = nullptr) WARN_UNUSED_RESULT;
};

} // namespace impala

#endif
