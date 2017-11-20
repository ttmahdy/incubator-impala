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

#include <chrono>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <thread>
#include <utility>
#include <vector>

#include <boost/date_time/compiler_config.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/local_time/local_time.hpp>

#include <time.h>
#include <stdlib.h>
#include <stdio.h>

#include "cctz/civil_time.h"
#include "cctz/time_zone.h"
#include "exprs/timezone_db.h"
#include "exprs/timestamp-functions.h"
#include "runtime/timestamp-parse-util.h"
#include "runtime/timestamp-value.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/pretty-printer.h"
#include "util/stopwatch.h"

#include "common/names.h"

using std::random_device;
using std::mt19937;
using std::uniform_int_distribution;
using std::thread;

using namespace impala;

// Benchmark tests for timestamp time-zone conversions
/*
Machine Info: Intel(R) Core(TM) i5-6600 CPU @ 3.30GHz

UtcToUnixTime:             Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
                            (glibc)                4.3      4.4     4.42         1X         1X         1X
                      (Google/CCTZ)                 10     10.1     10.2      2.32X      2.29X       2.3X
                            (boost)               72.5     76.2     78.5      38.2X      38.8X      38.5X


LocalToUnixTime:           Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
                            (glibc)              0.444    0.453    0.453         1X         1X         1X
                      (Google/CCTZ)               8.68     8.85     8.85      19.5X      19.5X      19.5X


FromUtc:                   Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
                            (boost)              0.868    0.885    0.885         1X         1X         1X
                      (Google/CCTZ)               3.94     4.02     4.02      4.54X      4.54X      4.54X


ToUtc:                     Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
                            (boost)              0.885    0.885    0.902         1X         1X         1X
                      (Google/CCTZ)               2.32     2.35     2.35      2.62X      2.66X      2.61X


UtcToLocal:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
                            (glibc)               1.42     1.44     1.44         1X         1X         1X
                      (Google/CCTZ)               4.02     4.02     4.02      2.84X      2.79X      2.79X


UnixTimeToLocalPtime:      Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
                            (glibc)               1.42     1.44     1.44         1X         1X         1X
                      (Google/CCTZ)               6.98     7.12     7.12      4.93X      4.93X      4.93X


UnixTimeToUtcPtime:        Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
                            (boost)               17.5     17.9     17.9         1X         1X         1X
                      (Google/CCTZ)               3.56     3.63     3.63     0.203X     0.203X     0.203X


Number of threads: 8

UtcToUnixTime:
             (glibc) elapsed time: 4s039ms
       (Google/CCTZ) elapsed time: 442ms
             (boost) elapsed time: 10ms
cctz speedup: 7.69888
boost speedup: 97.5301

LocalToUnixTime:
             (glibc) elapsed time: 1m9s
       (Google/CCTZ) elapsed time: 829ms
speedup: 84.1881

FromUtc:
             (boost) elapsed time: 5s513ms
       (Google/CCTZ) elapsed time: 1s450ms
speedup: 4.06223

ToUtc:
             (boost) elapsed time: 6s152ms
       (Google/CCTZ) elapsed time: 2s015ms
speedup: 3.19802

UtcToLocal:
             (glibc) elapsed time: 19s734ms
       (Google/CCTZ) elapsed time: 1s412ms
speedup: 13.6341

UnixTimeToLocalPtime:
             (glibc) elapsed time: 19s719ms
       (Google/CCTZ) elapsed time: 1s135ms
speedup: 19.0971

UnixTimeToUtcPtime:
             (boost) elapsed time: 275ms
       (Google/CCTZ) elapsed time: 1s178ms
speedup: 0.230531
*/

void AddTestDataDateTimes(vector<TimestampValue>& data, int n, const string& startstr) {
  DateTimeFormatContext dt_ctx;
  dt_ctx.Reset("yyyy-MMM-dd HH:mm:ss", 19);
  TimestampParser::ParseFormatTokens(&dt_ctx);

  random_device rd;
  mt19937 gen(rd());
  // Random days in a [0..99] days range.
  uniform_int_distribution<int64_t> dis_days(0, 99);
  // Random nanoseconds in a [0..15] minutes range.
  uniform_int_distribution<int64_t> dis_nanosec(0, 900000000000L);

  boost::posix_time::ptime start(boost::posix_time::time_from_string(startstr));
  for (int i = 0; i < n; ++i) {
    start += boost::gregorian::date_duration(dis_days(gen));
    start += boost::posix_time::nanoseconds(dis_nanosec(gen));
    string ts = to_simple_string(start);
    data.push_back(TimestampValue::Parse(ts.c_str(), ts.size(), dt_ctx));
  }
}

template <class FROM, class TO, TO (*converter)(const FROM &)>
class TestData {
public:
  TestData(const shared_ptr<vector<FROM> >& data) : data_(data) {
    result_.resize(data_->size());
  }

  void test_batch(int batch_size) {
    for (int i = 0; i < batch_size; ++i) {
      for (size_t j = 0; j < data_->size(); ++j) {
        result_[j] = converter((*data_)[j]);
      }
    }
  }

  const vector<TO> &get_result() const { return result_; }

  void add_to_benchmark(Benchmark& bm, const char* name) {
    bm.AddBenchmark(name, run_test, this);
  }

  static void run_test(int batch_size, void *d) {
    TestData<FROM, TO, converter>* data =
        reinterpret_cast<TestData<FROM, TO, converter>*>(d);
    data->test_batch(batch_size);
  }

  static uint64_t measure_multithreaded_elapsed_time(int num_of_threads, int batch_size,
      shared_ptr<vector<FROM> > data, const char* label) {
    // Create TestData for each thread.
    vector<unique_ptr<TestData<FROM, TO, converter> > > test_data;
    for (int i = 0; i < num_of_threads; ++i) {
      test_data.push_back(make_unique<TestData<FROM, TO, converter> >(data));
    }

    // Create and start threads.
    vector<unique_ptr<thread> > threads(num_of_threads);
    StopWatch sw;
    sw.Start();
    for (int i = 0; i < num_of_threads; ++i) {
      threads[i] = make_unique<thread>(
          run_test, batch_size, test_data[i].get());
    }

    // Wait until every thread terminates.
    for (auto& t: threads) t->join();
    uint64_t elapsed_time = sw.ElapsedTime();
    sw.Stop();

    cout << setw(20) << label << " elapsed time: "
         << PrettyPrinter::Print(elapsed_time, TUnit::CPU_TICKS)
         << endl;

    return elapsed_time;
  }

private:
  const shared_ptr<vector<FROM> > data_;
  vector<TO> result_;
};

//
// Test UtcToUnixTime (CCTZ is expected to be faster than glibc)
//

// CCTZ
const cctz::time_zone& CCTZ_UTC_TZ = TimezoneDatabase::GetUtcTimezone();
const cctz::time_zone* PTR_CCTZ_PST8PDT_TZ = nullptr;
const cctz::time_zone* PTR_CCTZ_EU_BUD_TZ = nullptr;

time_t cctz_utc_to_unix_time(const TimestampValue& ts) {
  const boost::gregorian::date& d = ts.date();
  const boost::posix_time::time_duration& t = ts.time();
  cctz::civil_second cs(d.year(), d.month(), d.day(), t.hours(), t.minutes(),
      t.seconds());
  auto tp = cctz::convert(cs, CCTZ_UTC_TZ);
  auto seconds = tp.time_since_epoch();
  return seconds.count();
}

// glibc
time_t glibc_utc_to_unix_time(const TimestampValue& ts) {
  const boost::posix_time::ptime temp(ts.date(), ts.time());
  tm temp_tm = boost::posix_time::to_tm(temp);
  return timegm(&temp_tm);
}

// boost
time_t boost_utc_to_unix_time(const TimestampValue& ts) {
  static const boost::gregorian::date epoch(1970,1,1);
  return (ts.date() - epoch).days() * 24 * 60 * 60 + ts.time().total_seconds();
}

//
// Test LocalToUnixTime (CCTZ is expected to be faster than glibc)
//

// CCTZ
time_t cctz_local_to_unix_time(const TimestampValue& ts) {
  const boost::gregorian::date& d = ts.date();
  const boost::posix_time::time_duration& t = ts.time();
  cctz::civil_second cs(d.year(), d.month(), d.day(), t.hours(), t.minutes(),
      t.seconds());
  auto tp = cctz::convert(cs, *PTR_CCTZ_EU_BUD_TZ);
  auto seconds = tp.time_since_epoch();
  return seconds.count();
}

// glibc
time_t glibc_local_to_unix_time(const TimestampValue& ts) {
  const boost::posix_time::ptime temp(ts.date(), ts.time());
  tm temp_tm = boost::posix_time::to_tm(temp);
  return mktime(&temp_tm);
}


//
// Test FromUtc (CCTZ is expected to be faster than boost)
//

// boost
const boost::local_time::time_zone_ptr BOOST_PST_TZ(
    new boost::local_time::posix_time_zone(string("PST-8PDT,M4.1.0,M10.1.0")));

void boost_throw_if_date_out_of_range(const boost::gregorian::date& date) {
  // Boost checks the ranges when instantiating the year/month/day representations.
  boost::gregorian::greg_year year = date.year();
  boost::gregorian::greg_month month = date.month();
  boost::gregorian::greg_day day = date.day();
  // Ensure Boost's validation is effective.
  DCHECK_GE(year, boost::gregorian::greg_year::min());
  DCHECK_LE(year, boost::gregorian::greg_year::max());
  DCHECK_GE(month, boost::gregorian::greg_month::min());
  DCHECK_LE(month, boost::gregorian::greg_month::max());
  DCHECK_GE(day, boost::gregorian::greg_day::min());
  DCHECK_LE(day, boost::gregorian::greg_day::max());
}

TimestampVal boost_from_utc(const TimestampVal& ts_val) {
  if (ts_val.is_null) return TimestampVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDateAndTime()) return TimestampVal::null();

  try {
    boost::posix_time::ptime temp;
    ts_value.ToPtime(&temp);
    boost::local_time::local_date_time lt(temp, BOOST_PST_TZ);
    boost::posix_time::ptime local_time = lt.local_time();
    boost_throw_if_date_out_of_range(local_time.date());
    TimestampVal return_val;
    TimestampValue(local_time).ToTimestampVal(&return_val);
    return return_val;
  } catch (boost::exception&) {
    return TimestampVal::null();
  }
}

// CCTZ
bool cctz_check_if_date_out_of_range(const cctz::civil_day& date) {
  static const cctz::civil_day max_date(9999, 12, 31);
  static const cctz::civil_day min_date(1400, 1, 1);
  return date < min_date || date > max_date;
}

TimestampVal cctz_from_utc(const TimestampVal& ts_val) {
  if (ts_val.is_null) return TimestampVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDateAndTime()) return TimestampVal::null();

  const boost::gregorian::date& d = ts_value.date();
  const boost::posix_time::time_duration& t = ts_value.time();
  const cctz::civil_second from_cs(d.year(), d.month(), d.day(), t.hours(), t.minutes(),
      t.seconds());

  auto from_tp = cctz::convert(from_cs, CCTZ_UTC_TZ);
  auto to_cs = cctz::convert(from_tp, *PTR_CCTZ_PST8PDT_TZ);

  // Check if resulting timestamp is within range
  if (UNLIKELY(cctz_check_if_date_out_of_range(cctz::civil_day(to_cs)))) {
    return TimestampVal::null();
  }

  auto return_date = boost::gregorian::date(to_cs.year(), to_cs.month(), to_cs.day());
  auto return_time = boost::posix_time::time_duration(to_cs.hour(), to_cs.minute(),
      to_cs.second(), t.fractional_seconds());
  TimestampVal return_val;
  TimestampValue(return_date, return_time).ToTimestampVal(&return_val);
  return return_val;
}


//
// Test ToUtc (CCTZ is expected to be faster than boost)
//

// boost
TimestampVal boost_to_utc(const TimestampVal& ts_val) {
  if (ts_val.is_null) return TimestampVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDateAndTime()) return TimestampVal::null();

  try {
    boost::local_time::local_date_time lt(ts_value.date(), ts_value.time(), BOOST_PST_TZ,
        boost::local_time::local_date_time::NOT_DATE_TIME_ON_ERROR);
    boost::posix_time::ptime utc_time = lt.utc_time();
    // The utc_time() conversion does not check ranges - need to explicitly check.
    boost_throw_if_date_out_of_range(utc_time.date());
    TimestampVal return_val;
    TimestampValue(utc_time).ToTimestampVal(&return_val);
    return return_val;
  } catch (boost::exception&) {
    return TimestampVal::null();
  }
}

// CCTZ
TimestampVal cctz_to_utc(const TimestampVal& ts_val) {
  if (ts_val.is_null) return TimestampVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDateAndTime()) return TimestampVal::null();

  const boost::gregorian::date& d = ts_value.date();
  const boost::posix_time::time_duration& t = ts_value.time();
  const cctz::civil_second from_cs(d.year(), d.month(), d.day(), t.hours(), t.minutes(),
      t.seconds());

  // In case or ambiguity return NULL
  const cctz::time_zone::civil_lookup from_cl = PTR_CCTZ_PST8PDT_TZ->lookup(from_cs);
  if (from_cl.kind != cctz::time_zone::civil_lookup::UNIQUE) {
    return TimestampVal::null();
  }
  auto from_tp = from_cl.pre;

  auto to_cs = cctz::convert(from_tp, CCTZ_UTC_TZ);

  // Check if resulting timestamp is within range
  if (UNLIKELY(cctz_check_if_date_out_of_range(cctz::civil_day(to_cs)))) {
    return TimestampVal::null();
  }

  auto return_date = boost::gregorian::date(to_cs.year(), to_cs.month(), to_cs.day());
  auto return_time = boost::posix_time::time_duration(to_cs.hour(), to_cs.minute(),
      to_cs.second(), t.fractional_seconds());
  TimestampVal return_val;
  TimestampValue(return_date, return_time).ToTimestampVal(&return_val);
  return return_val;
}


//
// Test UtcToLocal (CCTZ is expected to be faster than glibc)
//

// glibc
// Constants for use with Unix times. Leap-seconds do not apply.
const int32_t SECONDS_IN_MINUTE = 60;
const int32_t SECONDS_IN_HOUR = 60 * SECONDS_IN_MINUTE;
const int32_t SECONDS_IN_DAY = 24 * SECONDS_IN_HOUR;

// struct tm stores month/year data as an offset
const unsigned short TM_YEAR_OFFSET = 1900;
const unsigned short TM_MONTH_OFFSET = 1;

// Boost stores dates as an uint32_t. Since subtraction is needed, convert to signed.
const int64_t EPOCH_DAY_NUMBER =
    static_cast<int64_t>(
    boost::gregorian::date(1970, boost::gregorian::Jan, 1).day_number());

TimestampValue glibc_utc_to_local(const TimestampValue& ts_value) {
  DCHECK(ts_value.HasDateAndTime());
  try {
    boost::gregorian::date d = ts_value.date();
    boost::posix_time::time_duration t = ts_value.time();
    time_t utc =
        (static_cast<int64_t>(d.day_number()) - EPOCH_DAY_NUMBER) * SECONDS_IN_DAY +
        t.hours() * SECONDS_IN_HOUR +
        t.minutes() * SECONDS_IN_MINUTE +
        t.seconds();
    tm temp;
    if (UNLIKELY(localtime_r(&utc, &temp) == nullptr)) {
      return TimestampValue(boost::posix_time::ptime(boost::posix_time::not_a_date_time));
    }
    // Unlikely but a time zone conversion may push the value over the min/max
    // boundary resulting in an exception.
    d = boost::gregorian::date(
        static_cast<unsigned short>(temp.tm_year + TM_YEAR_OFFSET),
        static_cast<unsigned short>(temp.tm_mon + TM_MONTH_OFFSET),
        static_cast<unsigned short>(temp.tm_mday));
    t = boost::posix_time::time_duration(temp.tm_hour, temp.tm_min, temp.tm_sec,
        t.fractional_seconds());
    return TimestampValue(d, t);
  } catch (std::exception& /* from Boost */) {
    return TimestampValue(boost::posix_time::ptime(boost::posix_time::not_a_date_time));
  }
}

// CCTZ
TimestampValue cctz_utc_to_local(const TimestampValue& ts_value) {
  DCHECK(ts_value.HasDateAndTime());

  boost::gregorian::date d = ts_value.date();
  boost::posix_time::time_duration t = ts_value.time();
  const cctz::civil_second from_cs(d.year(), d.month(), d.day(),
      t.hours(), t.minutes(), t.seconds());

  auto from_tp = cctz::convert(from_cs, CCTZ_UTC_TZ);
  auto to_cs = cctz::convert(from_tp, *PTR_CCTZ_EU_BUD_TZ);
  // boost::gregorian::date() throws boost::gregorian::bad_year if year is not in the
  // 1400..9999 range. Need to check validity before creating the date object.
  if (UNLIKELY(cctz_check_if_date_out_of_range(cctz::civil_day(to_cs)))) {
    d = boost::gregorian::date(boost::gregorian::not_a_date_time);
    t = boost::posix_time::time_duration(boost::posix_time::not_a_date_time);
  } else {
    d = boost::gregorian::date(to_cs.year(), to_cs.month(), to_cs.day());
    t = boost::posix_time::time_duration(to_cs.hour(), to_cs.minute(), to_cs.second(),
        t.fractional_seconds());
  }
  return TimestampValue(d, t);
}


//
// Test UnixTimeToLocalPtime (CCTZ is expected to be faster than glibc)
//

// glibc
boost::posix_time::ptime glibc_unix_time_to_local_ptime(const time_t& unix_time) {
  tm temp_tm;
  if (UNLIKELY(localtime_r(&unix_time, &temp_tm) == nullptr)) {
    return boost::posix_time::ptime(boost::posix_time::not_a_date_time);
  }
  try {
    return boost::posix_time::ptime_from_tm(temp_tm);
  } catch (std::exception&) {
    return boost::posix_time::ptime(boost::posix_time::not_a_date_time);
  }
}

// CCTZ
inline cctz::time_point<cctz::sys_seconds> cctz_from_unix_seconds(time_t t) {
  return std::chrono::time_point_cast<cctz::sys_seconds>(
      std::chrono::system_clock::from_time_t(0)) + cctz::sys_seconds(t);
}

boost::posix_time::ptime cctz_unix_time_to_local_ptime(const time_t& unix_time) {
  auto from_tp = cctz_from_unix_seconds(unix_time);
  auto to_cs = cctz::convert(from_tp, *PTR_CCTZ_EU_BUD_TZ);
  // boost::gregorian::date() throws boost::gregorian::bad_year if year is not in the
  // 1400..9999 range. Need to check validity before creating the date object.
  if (UNLIKELY(cctz_check_if_date_out_of_range(cctz::civil_day(to_cs)))) {
    return boost::posix_time::ptime(boost::posix_time::not_a_date_time);
  } else {
    return boost::posix_time::ptime(
        boost::gregorian::date(to_cs.year(), to_cs.month(), to_cs.day()),
        boost::posix_time::time_duration(to_cs.hour(), to_cs.minute(), to_cs.second()));
  }
}


//
// Test UnixTimeToUtcPtime (boost is expected to be faster than CCTZ)
//

// boost
boost::posix_time::ptime boost_unix_time_to_utc_ptime(const time_t& unix_time) {
  // Minimum Unix time that can be converted with from_time_t: 1677-Sep-21 00:12:44
  const int64_t MIN_BOOST_CONVERT_UNIX_TIME = -9223372036;
  // Maximum Unix time that can be converted with from_time_t: 2262-Apr-11 23:47:16
  const int64_t MAX_BOOST_CONVERT_UNIX_TIME = 9223372036;
  if (LIKELY(unix_time >= MIN_BOOST_CONVERT_UNIX_TIME &&
             unix_time <= MAX_BOOST_CONVERT_UNIX_TIME)) {
    try {
      return boost::posix_time::from_time_t(unix_time);
    } catch (std::exception&) {
      return boost::posix_time::ptime(boost::posix_time::not_a_date_time);
    }
  }

  tm temp_tm;
  if (UNLIKELY(gmtime_r(&unix_time, &temp_tm) == nullptr)) {
    return boost::posix_time::ptime(boost::posix_time::not_a_date_time);
  }
  try {
    return boost::posix_time::ptime_from_tm(temp_tm);
  } catch (std::exception&) {
    return boost::posix_time::ptime(boost::posix_time::not_a_date_time);
  }
}

// CCTZ
boost::posix_time::ptime cctz_unix_time_to_utc_ptime(const time_t& unix_time) {
  auto from_tp = cctz_from_unix_seconds(unix_time);
  auto to_cs = cctz::convert(from_tp, CCTZ_UTC_TZ);
  // boost::gregorian::date() throws boost::gregorian::bad_year if year is not in the
  // 1400..9999 range. Need to check validity before creating the date object.
  if (UNLIKELY(cctz_check_if_date_out_of_range(cctz::civil_day(to_cs)))) {
    return boost::posix_time::ptime(boost::posix_time::not_a_date_time);
  } else {
    return boost::posix_time::ptime(
        boost::gregorian::date(to_cs.year(), to_cs.month(), to_cs.day()),
        boost::posix_time::time_duration(to_cs.hour(), to_cs.minute(), to_cs.second()));
  }
}


int main(int argc, char* argv[]) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  tzset();

  TimezoneDatabase::Initialize();
  PTR_CCTZ_PST8PDT_TZ = TimezoneDatabase::FindTimezone("PST8PDT");
  PTR_CCTZ_EU_BUD_TZ = TimezoneDatabase::FindTimezone("Europe/Budapest");
  DCHECK(PTR_CCTZ_PST8PDT_TZ != nullptr);
  DCHECK(PTR_CCTZ_EU_BUD_TZ != nullptr);

  TimestampParser::Init();

  shared_ptr<vector<TimestampValue> > tsvalue_data =
      make_shared<vector<TimestampValue> >();
  AddTestDataDateTimes(*tsvalue_data, 1000, "1953-04-22 01:02:03");

  // Benchmark UtcToUnixTime conversion with glibc/cctz
  Benchmark bm_utc_to_unix("UtcToUnixTime");
  TestData<TimestampValue, time_t, glibc_utc_to_unix_time> glibc_utc_to_unix_data =
      tsvalue_data;
  TestData<TimestampValue, time_t, cctz_utc_to_unix_time> cctz_utc_to_unix_data =
      tsvalue_data;
  TestData<TimestampValue, time_t, boost_utc_to_unix_time> boost_utc_to_unix_data =
      tsvalue_data;

  glibc_utc_to_unix_data.add_to_benchmark(bm_utc_to_unix, "(glibc)");
  cctz_utc_to_unix_data.add_to_benchmark(bm_utc_to_unix, "(Google/CCTZ)");
  boost_utc_to_unix_data.add_to_benchmark(bm_utc_to_unix, "(boost)");
  cout << bm_utc_to_unix.Measure() << endl;

  if (cctz_utc_to_unix_data.get_result() != glibc_utc_to_unix_data.get_result()) {
    cerr << "cctz/glibc utc_to_unix results do not match!" << endl;
    return 1;
  }
  if (boost_utc_to_unix_data.get_result() != glibc_utc_to_unix_data.get_result()) {
    cerr << "boost/glibc utc_to_unix results do not match!" << endl;
    return 1;
  }

  // Benchmark LocalToUnixTime conversion with glibc/cctz
  Benchmark bm_local_to_unix("LocalToUnixTime");
  TestData<TimestampValue, time_t, glibc_local_to_unix_time> glibc_local_to_unix_data =
      tsvalue_data;
  TestData<TimestampValue, time_t, cctz_local_to_unix_time> cctz_local_to_unix_data =
      tsvalue_data;

  glibc_local_to_unix_data.add_to_benchmark(bm_local_to_unix, "(glibc)");
  cctz_local_to_unix_data.add_to_benchmark(bm_local_to_unix, "(Google/CCTZ)");
  cout << bm_local_to_unix.Measure() << endl;

  // Benchmark FromUtc with boost/cctz
  shared_ptr<vector<TimestampVal> > tsval_data = make_shared<vector<TimestampVal> >();
  for (const TimestampValue& tsvalue: *tsvalue_data) {
    TimestampVal tsval;
    tsvalue.ToTimestampVal(&tsval);
    tsval_data->push_back(tsval);
  }

  Benchmark bm_from_utc("FromUtc");
  TestData<TimestampVal, TimestampVal, boost_from_utc> boost_from_utc_data = tsval_data;
  TestData<TimestampVal, TimestampVal, cctz_from_utc> cctz_from_utc_data = tsval_data;

  boost_from_utc_data.add_to_benchmark(bm_from_utc, "(boost)");
  cctz_from_utc_data.add_to_benchmark(bm_from_utc, "(Google/CCTZ)");
  cout << bm_from_utc.Measure() << endl;

  // Benchmark ToUtc with boost/cctz
  Benchmark bm_to_utc("ToUtc");
  TestData<TimestampVal, TimestampVal, boost_to_utc> boost_to_utc_data = tsval_data;
  TestData<TimestampVal, TimestampVal, cctz_to_utc> cctz_to_utc_data = tsval_data;

  boost_to_utc_data.add_to_benchmark(bm_to_utc, "(boost)");
  cctz_to_utc_data.add_to_benchmark(bm_to_utc, "(Google/CCTZ)");
  cout << bm_to_utc.Measure() << endl;

  // Benchmark UtcToLocal with glibc/cctz
  Benchmark bm_utc_to_local("UtcToLocal");
  TestData<TimestampValue, TimestampValue, glibc_utc_to_local> glibc_utc_to_local_data =
      tsvalue_data;
  TestData<TimestampValue, TimestampValue, cctz_utc_to_local> cctz_utc_to_local_data =
      tsvalue_data;

  glibc_utc_to_local_data.add_to_benchmark(bm_utc_to_local, "(glibc)");
  cctz_utc_to_local_data.add_to_benchmark(bm_utc_to_local, "(Google/CCTZ)");
  cout << bm_utc_to_local.Measure() << endl;

  // Benchmark UnixTimeToLocalPtime with glibc/cctz
  shared_ptr<vector<time_t> > time_data = make_shared<vector<time_t> >();
  for (const TimestampValue& tsvalue: *tsvalue_data) {
    time_t unix_time;
    tsvalue.ToUnixTime(&CCTZ_UTC_TZ, &unix_time);
    time_data->push_back(unix_time);
  }

  Benchmark bm_unix_time_to_local_ptime("UnixTimeToLocalPtime");
  TestData<
      time_t,
      boost::posix_time::ptime,
      glibc_unix_time_to_local_ptime> glibc_unix_time_to_local_ptime_data = time_data;
  TestData<
    time_t,
    boost::posix_time::ptime,
    cctz_unix_time_to_local_ptime> cctz_unix_time_to_local_ptime_data = time_data;

  glibc_unix_time_to_local_ptime_data.add_to_benchmark(bm_unix_time_to_local_ptime,
      "(glibc)");
  cctz_unix_time_to_local_ptime_data.add_to_benchmark(bm_unix_time_to_local_ptime,
      "(Google/CCTZ)");
  cout << bm_unix_time_to_local_ptime.Measure() << endl;

  // Benchmark UnixTimeToUtcPtime with boost/cctz
  Benchmark bm_unix_time_to_utc_ptime("UnixTimeToUtcPtime");
  TestData<
      time_t,
      boost::posix_time::ptime,
      boost_unix_time_to_utc_ptime> boost_unix_time_to_utc_ptime_data = time_data;
  TestData<
      time_t,
      boost::posix_time::ptime,
      cctz_unix_time_to_utc_ptime> cctz_unix_time_to_utc_ptime_data = time_data;

  boost_unix_time_to_utc_ptime_data.add_to_benchmark(bm_unix_time_to_utc_ptime,
      "(boost)");
  cctz_unix_time_to_utc_ptime_data.add_to_benchmark(bm_unix_time_to_utc_ptime,
      "(Google/CCTZ)");
  cout << bm_unix_time_to_utc_ptime.Measure() << endl;

  // If number of threads is specified, run multithreaded tests.
  int num_of_threads = (argc < 2) ? 0 : atoi(argv[1]);
  if (num_of_threads >= 1) {
    const int BATCH_SIZE = 1000;
    cout << "Number of threads: " << num_of_threads << endl;

    uint64_t m1 = 0, m2 = 0, m3 = 0;

    // UtcToUnixTime
    cout << endl << "UtcToUnixTime:" << endl;
    m1 = glibc_utc_to_unix_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsvalue_data, "(glibc)");
    m2 = cctz_utc_to_unix_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsvalue_data, "(Google/CCTZ)");
    m3 = boost_utc_to_unix_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsvalue_data, "(boost)");
    cout << "cctz speedup: " << double(m1)/double(m2) << endl;
    cout << "boost speedup: " << double(m1)/double(m3) << endl;

    // LocalToUnixTime
    cout << endl << "LocalToUnixTime:" << endl;
    m1 = glibc_local_to_unix_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsvalue_data, "(glibc)");
    m2 = cctz_local_to_unix_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsvalue_data, "(Google/CCTZ)");
    cout << "speedup: " << double(m1)/double(m2) << endl;

    // FromUtc
    cout << endl << "FromUtc:" << endl;
    m1 = boost_from_utc_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsval_data, "(boost)");
    m2 = cctz_from_utc_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsval_data, "(Google/CCTZ)");
    cout << "speedup: " << double(m1)/double(m2) << endl;

    // ToUtc
    cout << endl << "ToUtc:" << endl;
    m1 = boost_to_utc_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsval_data, "(boost)");
    m2 = cctz_to_utc_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsval_data, "(Google/CCTZ)");
    cout << "speedup: " << double(m1)/double(m2) << endl;

    // UtcToLocal
    cout << endl <<  "UtcToLocal:" << endl;
    m1 = glibc_utc_to_local_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsvalue_data, "(glibc)");
    m2 = cctz_utc_to_local_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsvalue_data, "(Google/CCTZ)");
    cout << "speedup: " << double(m1)/double(m2) << endl;

    // UnixTimeToLocalPtime
    cout << endl << "UnixTimeToLocalPtime:" << endl;
    m1 = glibc_unix_time_to_local_ptime_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, time_data, "(glibc)");
    m2 = cctz_unix_time_to_local_ptime_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, time_data, "(Google/CCTZ)");
    cout << "speedup: " << double(m1)/double(m2) << endl;

    // UnixTimeToUtcPtime
    cout << endl << "UnixTimeToUtcPtime:" << endl;
    m1 = boost_unix_time_to_utc_ptime_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, time_data, "(boost)");
    m2 = cctz_unix_time_to_utc_ptime_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, time_data, "(Google/CCTZ)");
    cout << "speedup: " << double(m1)/double(m2) << endl;
  }
  return 0;
}
