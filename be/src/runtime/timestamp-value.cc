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

#include "runtime/timestamp-value.h"

#include <boost/date_time/posix_time/posix_time.hpp>

#include "exprs/timestamp-functions.h"
#include "exprs/timezone_db.h"
#include "runtime/timestamp-parse-util.h"

#include "common/names.h"

using boost::date_time::not_a_date_time;
using boost::gregorian::date;
using boost::gregorian::date_duration;
using boost::posix_time::from_time_t;
using boost::posix_time::nanoseconds;
using boost::posix_time::ptime;
using boost::posix_time::ptime_from_tm;
using boost::posix_time::time_duration;
using boost::posix_time::to_tm;

DEFINE_bool(use_local_tz_for_unix_timestamp_conversions, false,
    "When true, TIMESTAMPs are interpreted in the local time zone when converting to "
    "and from Unix times. When false, TIMESTAMPs are interpreted in the UTC time zone. "
    "Set to true for Hive compatibility.");

// Constants for use with Unix times. Leap-seconds do not apply.
const int32_t SECONDS_IN_MINUTE = 60;
const int32_t SECONDS_IN_HOUR = 60 * SECONDS_IN_MINUTE;
const int32_t SECONDS_IN_DAY = 24 * SECONDS_IN_HOUR;

// struct tm stores month/year data as an offset
const unsigned short TM_YEAR_OFFSET = 1900;
const unsigned short TM_MONTH_OFFSET = 1;

// Boost stores dates as an uint32_t. Since subtraction is needed, convert to signed.
const int64_t EPOCH_DAY_NUMBER =
    static_cast<int64_t>(date(1970, boost::gregorian::Jan, 1).day_number());

namespace impala {

const char* TimestampValue::LLVM_CLASS_NAME = "class.impala::TimestampValue";
const double TimestampValue::ONE_BILLIONTH = 0.000000001;

TimestampValue TimestampValue::Parse(const char* str, int len) {
  TimestampValue tv;
  TimestampParser::Parse(str, len, &tv.date_, &tv.time_);
  return tv;
}

TimestampValue TimestampValue::Parse(const string& str) {
  return Parse(str.c_str(), str.size());
}

TimestampValue TimestampValue::Parse(const char* str, int len,
    const DateTimeFormatContext& dt_ctx) {
  TimestampValue tv;
  TimestampParser::Parse(str, len, dt_ctx, &tv.date_, &tv.time_);
  return tv;
}

int TimestampValue::Format(const DateTimeFormatContext& dt_ctx, int len, char* buff)
    const {
  return TimestampParser::Format(dt_ctx, date_, time_, len, buff);
}

namespace {
// TODO: Duplicate code (be/src/exprs/timestamp-functions-ir.cc)
inline cctz::time_point<cctz::sys_seconds> FromUnixSeconds(time_t t) {
  return std::chrono::time_point_cast<cctz::sys_seconds>(
      std::chrono::system_clock::from_time_t(0)) + cctz::sys_seconds(t);
}

// TODO: Duplicate code (be/src/exprs/timestamp-functions.cc)
inline bool CheckIfDateOutOfRange(const cctz::civil_day& date) {
  static const cctz::civil_day max_date(TimestampFunctions::MAX_YEAR, 12, 31);
  static const cctz::civil_day min_date(TimestampFunctions::MIN_YEAR, 1, 1);
  return date < min_date || date > max_date;
}

}

void TimestampValue::UtcToLocal(const cctz::time_zone* local_tz) {
  DCHECK(HasDateAndTime());
  DCHECK(local_tz != nullptr);
  const cctz::civil_second from_cs(date_.year(), date_.month(), date_.day(),
      time_.hours(), time_.minutes(), time_.seconds());

  auto from_tp = cctz::convert(from_cs, TimezoneDatabase::GetUtcTimezone());
  auto to_cs = cctz::convert(from_tp, *local_tz);
  // boost::gregorian::date() throws boost::gregorian::bad_year if year is not in the
  // 1400..9999 range. Need to check validity before creating the date object.
  if (UNLIKELY(CheckIfDateOutOfRange(cctz::civil_day(to_cs)))) {
    date_ = boost::gregorian::date(boost::gregorian::not_a_date_time);
    time_ = boost::posix_time::time_duration(boost::posix_time::not_a_date_time);
  } else {
    date_ = boost::gregorian::date(to_cs.year(), to_cs.month(), to_cs.day());
    time_ = boost::posix_time::time_duration(to_cs.hour(), to_cs.minute(), to_cs.second(),
        time_.fractional_seconds());
  }
}

ostream& operator<<(ostream& os, const TimestampValue& timestamp_value) {
  return os << timestamp_value.ToString();
}

void TimestampValue::Validate() {
    if (HasDate() && UNLIKELY(!IsValidDate(date_))) {
      time_ = boost::posix_time::time_duration(boost::posix_time::not_a_date_time);
      date_ = boost::gregorian::date(boost::gregorian::not_a_date_time);
    }
}

/// Return a ptime representation of the given Unix time (seconds since the Unix epoch).
/// The time zone of the resulting ptime is local time. This is called by UnixTimeToPtime.
ptime TimestampValue::UnixTimeToLocalPtime(time_t unix_time,
    const cctz::time_zone* local_tz) {
  DCHECK(local_tz != nullptr);
  auto from_tp = FromUnixSeconds(unix_time);
  auto to_cs = cctz::convert(from_tp, *local_tz);
  // boost::gregorian::date() throws boost::gregorian::bad_year if year is not in the
  // 1400..9999 range. Need to check validity before creating the date object.
  if (UNLIKELY(CheckIfDateOutOfRange(cctz::civil_day(to_cs)))) {
    return ptime(not_a_date_time);
  } else {
    return ptime(
        boost::gregorian::date(to_cs.year(), to_cs.month(), to_cs.day()),
        boost::posix_time::time_duration(to_cs.hour(), to_cs.minute(), to_cs.second()));
  }
}

/// Return a ptime representation of the given Unix time (seconds since the Unix epoch).
/// The time zone of the resulting ptime is UTC.
/// In order to avoid a serious performance degredation using CCTZ, this function uses
/// boost to convert the time_t to a ptime. Unfortunately, because the boost conversion
/// relies on time_duration to represent the time_t and internally
/// time_duration stores nanosecond precision ticks, the 'fast path' conversion using
/// boost can only handle a limited range of dates (appx years 1677-2622, while Impala
/// supports years 1400-9999). For dates outside this range, the conversion will instead
/// use the CCTZ function convert which supports those dates. This is called by
/// UnixTimeToPtime.
ptime TimestampValue::UnixTimeToUtcPtime(time_t unix_time) {
  // Minimum Unix time that can be converted with from_time_t: 1677-Sep-21 00:12:44
  const int64_t MIN_BOOST_CONVERT_UNIX_TIME = -9223372036;
  // Maximum Unix time that can be converted with from_time_t: 2262-Apr-11 23:47:16
  const int64_t MAX_BOOST_CONVERT_UNIX_TIME = 9223372036;
  if (LIKELY(unix_time >= MIN_BOOST_CONVERT_UNIX_TIME &&
             unix_time <= MAX_BOOST_CONVERT_UNIX_TIME)) {
    try {
      return from_time_t(unix_time);
    } catch (std::exception&) {
      return ptime(not_a_date_time);
    }
  }

  return UnixTimeToLocalPtime(unix_time, &TimezoneDatabase::GetUtcTimezone());
}

ptime TimestampValue::UnixTimeToPtime(time_t unix_time,
    const cctz::time_zone* local_tz) {
  DCHECK(local_tz != nullptr);
  if (FLAGS_use_local_tz_for_unix_timestamp_conversions) {
    return UnixTimeToLocalPtime(unix_time, local_tz);
  } else {
    return UnixTimeToUtcPtime(unix_time);
  }
}

string TimestampValue::ToString() const {
  stringstream ss;
  if (HasDate()) {
    ss << boost::gregorian::to_iso_extended_string(date_);
  }
  if (HasTime()) {
    if (HasDate()) ss << " ";
    ss << boost::posix_time::to_simple_string(time_);
  }
  return ss.str();
}

}
