//  Copyright (c) 2013-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#if !defined(GFLAGS)
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run db_bench\n");
  return 1;
}
#elif !defined(LISTDB_STRING_KEY) || !defined(LISTDB_WISCKEY)
#include <cstdio>
int main() {
  fprintf(stderr, "Please configure cmake with -DSTRING_KEY=ON -DWISCKEY=ON to run db_bench\n");
  return 1;
}
#else
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <sstream>

#include <gflags/gflags.h>

#include "listdb/db_client.h"
#include "listdb/env.h"
#include "listdb/listdb.h"
#include "listdb/lib/numa.h"
#include "listdb/monitoring/histogram.h"
#include "listdb/port/port_posix.h"
#include "listdb/util/clock.h"
#include "listdb/util/random.h"
#include "listdb/util/rate_limiter.h"

using DB = ListDB;

DEFINE_string(
    benchmarks,
    "fillseq,"
    "fillrandom,"
    "readseq,"
    "readrandom,"
    "mixgraph",
    "recoveryaftermixgraph"
    "Comma-separated list of operations to run in the specified order. Available benchmarks:\n"
    "\tfillseq       -- write N values in sequential key order in async mode\n"
    "\tfillrandom    -- write N values in random key order in async mode\n"
    "\treadseq       -- read N times sequentially\n"
    "\treadrandom    -- read N times in random order\n");

DEFINE_int32(write_threads, 0, "write_threads");

DEFINE_int64(num, 1000000, "Number of key/values to place in database");

DEFINE_int64(reads, -1, "Number of read operations to do.  "
             "If negative, do FLAGS_num reads.");

DEFINE_int64(seed, 0, "Seed base for random number generators. "
             "When 0 it is deterministic.");

DEFINE_int32(threads, 1, "Number of concurrent threads to run.");

DEFINE_int32(duration, 0, "Time in seconds for the random-ops tests to run."
             " When 0 then num & reads determine the test duration");

DEFINE_string(value_size_distribution_type, "fixed",
              "Value size distribution type: fixed, uniform, normal");

DEFINE_int32(value_size, 100, "Size of each value in fixed distribution");
static unsigned int value_size = 100;

DEFINE_int32(value_size_min, 100, "Min size of random value");

DEFINE_int32(value_size_max, 102400, "Max size of random value");

DEFINE_int32(seek_nexts, 0,
             "How many times to call Next() after Seek() in "
             "fillseekseq, seekrandom, seekrandomwhilewriting and "
             "seekrandomwhilemerging");

static int64_t FLAGS_batch_size = 1;
//DEFINE_int64(batch_size, 1, "Batch size");

DEFINE_int32(key_size, 16, "size of each key");

DEFINE_double(compression_ratio, 0.5, "Arrange to generate values that shrink"
              " to this fraction of their original size after compression");

DEFINE_double(read_random_exp_range, 0.0,
              "Read random's key will be generated using distribution of "
              "num * exp(-r) where r is uniform number from 0 to this value. "
              "The larger the number is, the more skewed the reads are. "
              "Only used in readrandom and multireadrandom benchmarks.");

DEFINE_bool(histogram, false, "Print histogram of operation timings");

static bool FLAGS_statistics = false;
//DEFINE_bool(statistics, false, "Database statistics");

DEFINE_int64(writes, -1, "Number of write operations to do. If negative, do"
             " --num reads.");

DEFINE_double(sine_a, 1,
             "A in f(x) = A sin(bx + c) + d");

DEFINE_double(sine_b, 1,
             "B in f(x) = A sin(bx + c) + d");

DEFINE_double(sine_c, 0,
             "C in f(x) = A sin(bx + c) + d");

DEFINE_double(sine_d, 1,
             "D in f(x) = A sin(bx + c) + d");

// the parameters of mix_graph
DEFINE_double(keyrange_dist_a, 0.0,
              "The parameter 'a' of prefix average access distribution "
              "f(x)=a*exp(b*x)+c*exp(d*x)");
DEFINE_double(keyrange_dist_b, 0.0,
              "The parameter 'b' of prefix average access distribution "
              "f(x)=a*exp(b*x)+c*exp(d*x)");
DEFINE_double(keyrange_dist_c, 0.0,
              "The parameter 'c' of prefix average access distribution"
              "f(x)=a*exp(b*x)+c*exp(d*x)");
DEFINE_double(keyrange_dist_d, 0.0,
              "The parameter 'd' of prefix average access distribution"
              "f(x)=a*exp(b*x)+c*exp(d*x)");
DEFINE_int64(keyrange_num, 1,
             "The number of key ranges that are in the same prefix "
             "group, each prefix range will have its key access "
             "distribution");
DEFINE_double(key_dist_a, 0.0,
              "The parameter 'a' of key access distribution model "
              "f(x)=a*x^b");
DEFINE_double(key_dist_b, 0.0,
              "The parameter 'b' of key access distribution model "
              "f(x)=a*x^b");
DEFINE_double(value_theta, 0.0,
              "The parameter 'theta' of Generized Pareto Distribution "
              "f(x)=(1/sigma)*(1+k*(x-theta)/sigma)^-(1/k+1)");
DEFINE_double(value_k, 0.0,
              "The parameter 'k' of Generized Pareto Distribution "
              "f(x)=(1/sigma)*(1+k*(x-theta)/sigma)^-(1/k+1)");
DEFINE_double(value_sigma, 0.0,
              "The parameter 'theta' of Generized Pareto Distribution "
              "f(x)=(1/sigma)*(1+k*(x-theta)/sigma)^-(1/k+1)");
DEFINE_double(iter_theta, 0.0,
              "The parameter 'theta' of Generized Pareto Distribution "
              "f(x)=(1/sigma)*(1+k*(x-theta)/sigma)^-(1/k+1)");
DEFINE_double(iter_k, 0.0,
              "The parameter 'k' of Generized Pareto Distribution "
              "f(x)=(1/sigma)*(1+k*(x-theta)/sigma)^-(1/k+1)");
DEFINE_double(iter_sigma, 0.0,
              "The parameter 'sigma' of Generized Pareto Distribution "
              "f(x)=(1/sigma)*(1+k*(x-theta)/sigma)^-(1/k+1)");
DEFINE_double(mix_get_ratio, 1.0,
              "The ratio of Get queries of mix_graph workload");
DEFINE_double(mix_put_ratio, 0.0,
              "The ratio of Put queries of mix_graph workload");
DEFINE_double(mix_seek_ratio, 0.0,
              "The ratio of Seek queries of mix_graph workload");
DEFINE_int64(mix_max_scan_len, 10000, "The max scan length of Iterator");
DEFINE_int64(mix_max_value_size, 1024, "The max value size of this workload");
DEFINE_double(
    sine_mix_rate_noise, 0.0,
    "Add the noise ratio to the sine rate, it is between 0.0 and 1.0");
DEFINE_bool(sine_mix_rate, false,
            "Enable the sine QPS control on the mix workload");
DEFINE_uint64(
    sine_mix_rate_interval_milliseconds, 10000,
    "Interval of which the sine wave read_rate_limit is recalculated");
DEFINE_int64(mix_accesses, -1,
             "The total query accesses of mix_graph workload");

DEFINE_int32(ops_between_duration_checks, 1000,
             "Check duration limit every x ops");

DEFINE_int64(stats_interval, 0, "Stats are reported every N operations when "
             "this is greater than zero. When 0 the interval grows over time.");

DEFINE_int64(stats_interval_seconds, 0, "Report stats every N seconds. This "
             "overrides stats_interval when both are > 0.");

DEFINE_int32(stats_per_interval, 0, "Reports additional stats per interval when"
             " this is greater than 0.");

DEFINE_int64(report_interval_seconds, 0,
             "If greater than zero, it will write simple stats in CSV format "
             "to --report_file every N seconds");

DEFINE_string(report_file, "report.csv",
              "Filename where some simple stats are reported to (if "
              "--report_interval_seconds is bigger than 0)");

DEFINE_int32(thread_status_per_interval, 0,
             "Takes and report a snapshot of the current status of each thread"
             " when this is greater than 0.");

DEFINE_bool(use_existing_db, false, "If true, do not destroy the existing"
            " database.  If you set this flag and also specify a benchmark that"
            " wants a fresh database, that benchmark will fail.");

static std::string FLAGS_db = "/pmem/wkim/listdb";
//DEFINE_string(db, "", "Use the db with the following name.");

enum DistributionType : unsigned char {
  kFixed = 0,
  kUniform,
  kNormal
};

static enum DistributionType FLAGS_value_size_distribution_type_e = kFixed;

static enum DistributionType StringToDistributionType(const char* ctype) {
  assert(ctype);

  if (!strcasecmp(ctype, "fixed"))
    return kFixed;
  else if (!strcasecmp(ctype, "uniform"))
    return kUniform;
  else if (!strcasecmp(ctype, "normal"))
    return kNormal;

  fprintf(stdout, "Cannot parse distribution type '%s'\n", ctype);
  return kFixed;  // default value
}

class BaseDistribution {
 public:
  BaseDistribution(unsigned int _min, unsigned int _max)
      : min_value_size_(_min), max_value_size_(_max) {}
  virtual ~BaseDistribution() {}

  unsigned int Generate() {
    auto val = Get();
    if (NeedTruncate()) {
      val = std::max(min_value_size_, val);
      val = std::min(max_value_size_, val);
    }
    return val;
  }
 private:
  virtual unsigned int Get() = 0;
  virtual bool NeedTruncate() {
    return true;
  }
  unsigned int min_value_size_;
  unsigned int max_value_size_;
};

class FixedDistribution : public BaseDistribution
{
 public:
  FixedDistribution(unsigned int size) :
    BaseDistribution(size, size),
    size_(size) {}
 private:
  virtual unsigned int Get() override {
    return size_;
  }
  virtual bool NeedTruncate() override {
    return false;
  }
  unsigned int size_;
};

class NormalDistribution
    : public BaseDistribution, public std::normal_distribution<double> {
 public:
  NormalDistribution(unsigned int _min, unsigned int _max)
      : BaseDistribution(_min, _max),
        // 99.7% values within the range [min, max].
        std::normal_distribution<double>(
            (double)(_min + _max) / 2.0 /*mean*/,
            (double)(_max - _min) / 6.0 /*stddev*/),
        gen_(rd_()) {}

 private:
  virtual unsigned int Get() override {
    return static_cast<unsigned int>((*this)(gen_));
  }
  std::random_device rd_;
  std::mt19937 gen_;
};

class UniformDistribution
    : public BaseDistribution,
      public std::uniform_int_distribution<unsigned int> {
 public:
  UniformDistribution(unsigned int _min, unsigned int _max)
      : BaseDistribution(_min, _max),
        std::uniform_int_distribution<unsigned int>(_min, _max),
        gen_(rd_()) {}

 private:
  virtual unsigned int Get() override {
    return (*this)(gen_);
  }
  virtual bool NeedTruncate() override {
    return false;
  }
  std::random_device rd_;
  std::mt19937 gen_;
};

static void CompressibleString(Random* rnd, double compressed_fraction,
                                int len, std::string* dst) {
  int raw = static_cast<int>(len * compressed_fraction);
  if (raw < 1) raw = 1;
  std::string raw_data = rnd->RandomString(raw);

  // Duplicate the random data until we have filled "len" bytes
  dst->clear();
  while (dst->size() < (unsigned int)len) {
    dst->append(raw_data);
  }
  dst->resize(len);
}

// Helper for quickly generating random data.
class RandomGenerator {
 private:
  std::string data_;
  unsigned int pos_;
  std::unique_ptr<BaseDistribution> dist_;

 public:

  RandomGenerator() {
    auto max_value_size = FLAGS_value_size_max;
    switch (FLAGS_value_size_distribution_type_e) {
      case kUniform:
        dist_.reset(new UniformDistribution(FLAGS_value_size_min,
                                            FLAGS_value_size_max));
        break;
      case kNormal:
        dist_.reset(new NormalDistribution(FLAGS_value_size_min,
                                           FLAGS_value_size_max));
        break;
      case kFixed:
      default:
        dist_.reset(new FixedDistribution(value_size));
        max_value_size = value_size;
    }
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < (unsigned)std::max(1048576, max_value_size)) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  std::string_view Generate(unsigned int len) {
    assert(len <= data_.size());
    if (pos_ + len > data_.size()) {
      pos_ = 0;
    }
    pos_ += len;
    return std::string_view(data_.data() + pos_ - len, len);
  }

  std::string_view Generate() {
    auto len = dist_->Generate();
    return Generate(len);
  }
};

inline void AppendWithSpace(std::string* str, const std::string_view& msg) {
  if (msg.empty()) return;
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data(), msg.size());
}

// a class that reports stats to CSV file
class ReporterAgent {
 public:
  ReporterAgent(const std::string& fname,
                uint64_t report_interval_secs)
      : total_ops_done_(0),
        last_report_(0),
        report_interval_secs_(report_interval_secs),
        stop_(false) {
    report_file_.open(fname);
    if (report_file_.good()) {
      report_file_ << Header() << std::endl;
      report_file_.flush();
    } else {
      fprintf(stderr, "Can't open %s: %s\n", fname.c_str(), std::strerror(errno));
      abort();
    }

    //reporting_thread_ = port::Thread([&]() { SleepAndReport(); });
    reporting_thread_ = std::thread([&]() { SleepAndReport(); });
  }

  ~ReporterAgent() {
    {
      std::unique_lock<std::mutex> lk(mutex_);
      stop_ = true;
      stop_cv_.notify_all();
    }
    reporting_thread_.join();
    report_file_.close();
  }

  // thread safe
  void ReportFinishedOps(int64_t num_ops) {
    total_ops_done_.fetch_add(num_ops);
  }

 private:
  std::string Header() const { return "secs_elapsed,interval_qps"; }
  void SleepAndReport() {
    auto time_started = Clock::NowMicros();
    while (true) {
      {
        std::unique_lock<std::mutex> lk(mutex_);
        if (stop_ ||
            stop_cv_.wait_for(lk, std::chrono::seconds(report_interval_secs_),
                              [&]() { return stop_; })) {
          // stopping
          break;
        }
        // else -> timeout, which means time for a report!
      }
      auto total_ops_done_snapshot = total_ops_done_.load();
      // round the seconds elapsed
      auto secs_elapsed =
          (Clock::NowMicros() - time_started + kMicrosInSecond / 2) /
          kMicrosInSecond;
      std::string report = std::to_string(secs_elapsed) + "," +
                           std::to_string(total_ops_done_snapshot - last_report_) +
                           "\n";
      report_file_ << report;
      report_file_.flush();
      if (!report_file_.good()) {
        fprintf(stderr, "Can't write to report file (%s), stopping the reporting\n", std::strerror(errno));
        break;
      }
      last_report_ = total_ops_done_snapshot;
    }
  }

  static constexpr uint64_t kMicrosInSecond = 1000U * 1000U;
  std::ofstream report_file_;
  std::atomic<int64_t> total_ops_done_;
  int64_t last_report_;
  const uint64_t report_interval_secs_;
  std::thread reporting_thread_;
  std::mutex mutex_;
  // will notify on stop
  std::condition_variable stop_cv_;
  bool stop_;
};

enum OperationType : unsigned char {
  kRead = 0,
  kWrite,
  kDelete,
  kSeek,
  kMerge,
  kUpdate,
  kCompress,
  kUncompress,
  kCrc,
  kHash,
  kOthers
};

static std::unordered_map<OperationType, std::string, std::hash<unsigned char>>
                          OperationTypeString = {
  {kRead, "read"},
  {kWrite, "write"},
  {kDelete, "delete"},
  {kSeek, "seek"},
  {kMerge, "merge"},
  {kUpdate, "update"},
  {kCompress, "compress"},
  {kCompress, "uncompress"},
  {kCrc, "crc"},
  {kHash, "hash"},
  {kOthers, "op"}
};

class CombinedStats;
class Stats {
 private:
  int id_;
  uint64_t start_ = 0;
  uint64_t sine_interval_;
  uint64_t finish_;
  double seconds_;
  uint64_t done_;
  uint64_t last_report_done_;
  uint64_t next_report_;
  uint64_t bytes_;
  uint64_t last_op_finish_;
  uint64_t last_report_finish_;
  std::unordered_map<OperationType, std::shared_ptr<HistogramImpl>,
                     std::hash<unsigned char>> hist_;
  std::string message_;
  bool exclude_from_merge_;
  ReporterAgent* reporter_agent_;  // does not own
  friend class CombinedStats;

 public:
  Stats() { Start(-1); }

  void SetReporterAgent(ReporterAgent* reporter_agent) {
    reporter_agent_ = reporter_agent;
  }

  void Start(int id) {
    id_ = id;
    next_report_ = FLAGS_stats_interval ? FLAGS_stats_interval : 100;
    last_op_finish_ = start_;
    hist_.clear();
    done_ = 0;
    last_report_done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = Clock::NowMicros();
    sine_interval_ = Clock::NowMicros();
    finish_ = start_;
    last_report_finish_ = start_;
    message_.clear();
    // When set, stats from this thread won't be merged with others.
    exclude_from_merge_ = false;
  }

  void Merge(const Stats& other) {
    if (other.exclude_from_merge_)
      return;

    for (auto it = other.hist_.begin(); it != other.hist_.end(); ++it) {
      auto this_it = hist_.find(it->first);
      if (this_it != hist_.end()) {
        this_it->second->Merge(*(other.hist_.at(it->first)));
      } else {
        hist_.insert({ it->first, it->second });
      }
    }

    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
    finish_ = Clock::NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void AddMessage(const std::string_view& msg) {
    AppendWithSpace(&message_, msg);
  }

  void SetId(int id) { id_ = id; }
  void SetExcludeFromMerge() { exclude_from_merge_ = true; }

#if 0
  void PrintThreadStatus() {
    std::vector<ThreadStatus> thread_list;
    FLAGS_env->GetThreadList(&thread_list);

    fprintf(stderr, "\n%18s %10s %12s %20s %13s %45s %12s %s\n",
        "ThreadID", "ThreadType", "cfName", "Operation",
        "ElapsedTime", "Stage", "State", "OperationProperties");

    //int64_t current_time = 0;
    //clock_->GetCurrentTime(&current_time).PermitUncheckedError();
    for (auto ts : thread_list) {
      fprintf(stderr, "%18lu %10s %12s %20s %13s %45s %12s",
          ts.thread_id,
          ThreadStatus::GetThreadTypeName(ts.thread_type).c_str(),
          ts.cf_name.c_str(),
          ThreadStatus::GetOperationName(ts.operation_type).c_str(),
          ThreadStatus::MicrosToString(ts.op_elapsed_micros).c_str(),
          ThreadStatus::GetOperationStageName(ts.operation_stage).c_str(),
          ThreadStatus::GetStateName(ts.state_type).c_str());

      auto op_properties = ThreadStatus::InterpretOperationProperties(
          ts.operation_type, ts.op_properties);
      for (const auto& op_prop : op_properties) {
        fprintf(stderr, " %s %" PRIu64" |",
            op_prop.first.c_str(), op_prop.second);
      }
      fprintf(stderr, "\n");
    }
  }
#endif

  void ResetSineInterval() { sine_interval_ = Clock::NowMicros(); }

  uint64_t GetSineInterval() {
    return sine_interval_;
  }

  uint64_t GetStart() {
    return start_;
  }

  void ResetLastOpTime() {
    // Set to now to avoid latency from calls to SleepForMicroseconds
    last_op_finish_ = Clock::NowMicros();
  }

  void FinishedOps(DB* db, int64_t num_ops, enum OperationType op_type = kOthers) {
    if (reporter_agent_) {
      reporter_agent_->ReportFinishedOps(num_ops);
    }
    if (FLAGS_histogram) {
      uint64_t now = Clock::NowMicros();
      uint64_t micros = now - last_op_finish_;

      if (hist_.find(op_type) == hist_.end())
      {
        auto hist_temp = std::make_shared<HistogramImpl>();
        hist_.insert({op_type, std::move(hist_temp)});
      }
      hist_[op_type]->Add(micros);

      if (micros > 20000 && !FLAGS_stats_interval) {
        fprintf(stderr, "long op: %lu micros%30s\r", micros, "");
        fflush(stderr);
      }
      last_op_finish_ = now;
    }

    done_ += num_ops;
    if (done_ >= next_report_) {
      if (!FLAGS_stats_interval) {
        if      (next_report_ < 1000)   next_report_ += 100;
        else if (next_report_ < 5000)   next_report_ += 500;
        else if (next_report_ < 10000)  next_report_ += 1000;
        else if (next_report_ < 50000)  next_report_ += 5000;
        else if (next_report_ < 100000) next_report_ += 10000;
        else if (next_report_ < 500000) next_report_ += 50000;
        else                            next_report_ += 100000;
        fprintf(stderr, "... finished %lu ops%30s\r", done_, "");
      } else {
        uint64_t now = Clock::NowMicros();
        int64_t usecs_since_last = now - last_report_finish_;

        // Determine whether to print status where interval is either
        // each N operations or each N seconds.

        if (FLAGS_stats_interval_seconds &&
            usecs_since_last < (FLAGS_stats_interval_seconds * 1000000)) {
          // Don't check again for this many operations
          next_report_ += FLAGS_stats_interval;

        } else {
          fprintf(stderr,
                  "%s ... thread %d: (%lu,%lu) ops and "
                  "(%.1f,%.1f) ops/second in (%.6f,%.6f) seconds\n",
                  Clock::TimeToString(now / 1000000).c_str(), id_,
                  done_ - last_report_done_, done_,
                  (done_ - last_report_done_) / (usecs_since_last / 1000000.0),
                  done_ / ((now - start_) / 1000000.0),
                  (now - last_report_finish_) / 1000000.0,
                  (now - start_) / 1000000.0);

          //if (id_ == 0 && FLAGS_stats_per_interval) {
          //  //std::string stats;
          //}

          next_report_ += FLAGS_stats_interval;
          last_report_finish_ = now;
          last_report_done_ = done_;
        }
      }
      //if (id_ == 0 && FLAGS_thread_status_per_interval) {
      //  PrintThreadStatus();
      //}
      fflush(stderr);
    }
  }

  void AddBytes(int64_t n) {
    bytes_ += n;
  }

  void Report(const std::string_view& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedOps().
    if (done_ < 1) done_ = 1;

    std::string extra;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      double elapsed = (finish_ - start_) * 1e-6;
      char rate[100];
      snprintf(rate, sizeof(rate), "%6.1f MB/s",
               (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }
    AppendWithSpace(&extra, message_);
    double elapsed = (finish_ - start_) * 1e-6;
    double throughput = (double)done_/elapsed;

    fprintf(stdout, "%-12s : %11.3f micros/op %ld ops/sec;%s%s\n",
            name.data(),
            seconds_ * 1e6 / done_,
            (long)throughput,
            (extra.empty() ? "" : " "),
            extra.c_str());
    if (FLAGS_histogram) {
      for (auto it = hist_.begin(); it != hist_.end(); ++it) {
        fprintf(stdout, "Microseconds per %s:\n%s\n",
                OperationTypeString[it->first].c_str(),
                it->second->ToString().c_str());
      }
    }
    fflush(stdout);
  }
};

class CombinedStats {
 public:
  void AddStats(const Stats& stat) {
    uint64_t total_ops = stat.done_;
    uint64_t total_bytes_ = stat.bytes_;
    double elapsed;

    if (total_ops < 1) {
      total_ops = 1;
    }

    elapsed = (stat.finish_ - stat.start_) * 1e-6;
    throughput_ops_.emplace_back(total_ops / elapsed);

    if (total_bytes_ > 0) {
      double mbs = (total_bytes_ / 1048576.0);
      throughput_mbs_.emplace_back(mbs / elapsed);
    }
  }

  void Report(const std::string& bench_name) {
    const char* name = bench_name.c_str();
    int num_runs = static_cast<int>(throughput_ops_.size());

    if (throughput_mbs_.size() == throughput_ops_.size()) {
      fprintf(stdout,
              "%s [AVG    %d runs] : %d ops/sec; %6.1f MB/sec\n"
              "%s [MEDIAN %d runs] : %d ops/sec; %6.1f MB/sec\n",
              name, num_runs, static_cast<int>(CalcAvg(throughput_ops_)),
              CalcAvg(throughput_mbs_), name, num_runs,
              static_cast<int>(CalcMedian(throughput_ops_)),
              CalcMedian(throughput_mbs_));
    } else {
      fprintf(stdout,
              "%s [AVG    %d runs] : %d ops/sec\n"
              "%s [MEDIAN %d runs] : %d ops/sec\n",
              name, num_runs, static_cast<int>(CalcAvg(throughput_ops_)), name,
              num_runs, static_cast<int>(CalcMedian(throughput_ops_)));
    }
  }

 private:
  double CalcAvg(std::vector<double> data) {
    double avg = 0;
    for (double x : data) {
      avg += x;
    }
    avg = avg / data.size();
    return avg;
  }

  double CalcMedian(std::vector<double> data) {
    assert(data.size() > 0);
    std::sort(data.begin(), data.end());

    size_t mid = data.size() / 2;
    if (data.size() % 2 == 1) {
      // Odd number of entries
      return data[mid];
    } else {
      // Even number of entries
      return (data[mid] + data[mid - 1]) / 2;
    }
  }

  std::vector<double> throughput_ops_;
  std::vector<double> throughput_mbs_;
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
  std::mutex mu;
  std::condition_variable cv;
  int total;
  std::shared_ptr<RateLimiter> write_rate_limiter;
  std::shared_ptr<RateLimiter> read_rate_limiter;

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  long num_initialized;
  long num_done;
  bool start;

  //SharedState() : cv(&mu), perf_level(FLAGS_perf_level) { }
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  int tid;             // 0..n-1 when running in n threads
  Random64 rand;         // Has different seeds for different threads
  Stats stats;
  SharedState* shared;
  DBClient* client;
  std::vector<std::pair<int, uint64_t>>* op_time_arr;

  explicit ThreadState(int index)
      : tid(index), rand((FLAGS_seed ? FLAGS_seed : 1000) + index) {}
};

class Duration {
 public:
  Duration(uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage = 0) {
    max_seconds_ = max_seconds;
    max_ops_= max_ops;
    ops_per_stage_ = (ops_per_stage > 0) ? ops_per_stage : max_ops;
    ops_ = 0;
    start_at_ = Clock::NowMicros();
  }

  int64_t GetStage() { return std::min(ops_, max_ops_ - 1) / ops_per_stage_; }

  bool Done(int64_t increment) {
    if (increment <= 0) increment = 1;    // avoid Done(0) and infinite loops
    ops_ += increment;

    if (max_seconds_) {
      // Recheck every appx 1000 ops (exact iff increment is factor of 1000)
      auto granularity = FLAGS_ops_between_duration_checks;
      if ((ops_ / granularity) != ((ops_ - increment) / granularity)) {
        uint64_t now = Clock::NowMicros();
        return ((now - start_at_) / 1000000) >= max_seconds_;
      } else {
        return false;
      }
    } else {
      return ops_ > max_ops_;
    }
  }

 private:
  uint64_t max_seconds_;
  int64_t max_ops_;
  int64_t ops_per_stage_;
  int64_t ops_;
  uint64_t start_at_;
};

class Benchmark {
 public:
  Benchmark()
      : db_(new ListDB()),
        num_(FLAGS_num),
        key_size_(FLAGS_key_size),
        reads_(FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads),
        read_random_exp_range_(0.0),
        writes_(FLAGS_writes < 0 ? FLAGS_num : FLAGS_writes) {
    if (!FLAGS_use_existing_db) {
      db_->Init();
    } else {
      abort();
      //db_->Open();
    }

    if (key_size_ > (int) kStringKeyLength) {
      fprintf(stderr, "Error!: kStringKeyLength < --key_size\n.");
      exit(1);
    }
  }

  ~Benchmark() {
  }

  std::string_view AllocateKey(std::unique_ptr<const char[]>* key_guard) {
    char* data = new char[key_size_];
    const char* const_data = data;
    key_guard->reset(const_data);
    return std::string_view(key_guard->get(), key_size_);
  }

  // Generate key according to the given specification and random number.
  // The resulting key will have the following format:
  //   - If keys_per_prefix_ is positive, extra trailing bytes are either cut
  //     off or padded with '0'.
  //     The prefix value is derived from key value.
  //     ----------------------------
  //     | prefix 00000 | key 00000 |
  //     ----------------------------
  //
  //   - If keys_per_prefix_ is 0, the key is simply a binary representation of
  //     random number followed by trailing '0's
  //     ----------------------------
  //     |        key 00000         |
  //     ----------------------------
  void GenerateKeyFromInt(uint64_t v, int64_t num_keys, std::string_view* key) {
    if (v == 0) v++;
    char* start = const_cast<char*>(key->data());
    char* pos = start;

    int bytes_to_fill = std::min(key_size_ - static_cast<int>(pos - start), 8);
    if (port::kLittleEndian) {
      for (int i = 0; i < bytes_to_fill; ++i) {
        pos[i] = (v >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
      }
    } else {
      memcpy(pos, static_cast<void*>(&v), bytes_to_fill);
    }
    pos += bytes_to_fill;
    if (key_size_ > pos - start) {
      memset(pos, '0', key_size_ - (pos - start));
    }
  }

  void Run() {
    //Open(&open_options_);
    PrintHeader();
    std::stringstream benchmark_stream(FLAGS_benchmarks);
    std::string name;
    //std::unique_ptr<ExpiredTimeFilter> filter;
    while (std::getline(benchmark_stream, name, ',')) {
      // Sanitize parameters
      num_ = FLAGS_num;
      reads_ = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
      writes_ = (FLAGS_writes < 0 ? FLAGS_num : FLAGS_writes);
      value_size = FLAGS_value_size;
      key_size_ = FLAGS_key_size;
      entries_per_batch_ = FLAGS_batch_size;
      //writes_before_delete_range_ = FLAGS_writes_before_delete_range;
      //writes_per_range_tombstone_ = FLAGS_writes_per_range_tombstone;
      //range_tombstone_width_ = FLAGS_range_tombstone_width;
      //max_num_range_tombstones_ = FLAGS_max_num_range_tombstones;
      //write_options_ = WriteOptions();
      read_random_exp_range_ = FLAGS_read_random_exp_range;
      //if (FLAGS_sync) {
      //  write_options_.sync = true;
      //}
      //write_options_.disableWAL = FLAGS_disable_wal;

      void (Benchmark::*method)(ThreadState*) = nullptr;
      void (Benchmark::*post_process_method)() = nullptr;

      bool fresh_db = false;
      int num_threads = FLAGS_threads;

      int num_repeat = 1;
      int num_warmup = 0;
      if (!name.empty() && *name.rbegin() == ']') {
        auto it = name.find('[');
        if (it == std::string::npos) {
          fprintf(stderr, "unknown benchmark arguments '%s'\n", name.c_str());
          ErrorExit();
        }
        std::string args = name.substr(it + 1);
        args.resize(args.size() - 1);
        name.resize(it);

        std::string bench_arg;
        std::stringstream args_stream(args);
        while (std::getline(args_stream, bench_arg, '-')) {
          if (bench_arg.empty()) {
            continue;
          }
          if (bench_arg[0] == 'X') {
            // Repeat the benchmark n times
            std::string num_str = bench_arg.substr(1);
            num_repeat = std::stoi(num_str);
          } else if (bench_arg[0] == 'W') {
            // Warm up the benchmark for n times
            std::string num_str = bench_arg.substr(1);
            num_warmup = std::stoi(num_str);
          }
        }
      }

      if (name == "fillseq") {
        fresh_db = true;
        method = &Benchmark::WriteSeq;
      } else if (name == "fillrandom") {
        fresh_db = true;
        method = &Benchmark::WriteRandom;
      } else if (name == "fill100K") {
        fresh_db = true;
        num_ /= 1000;
        value_size = 100 * 1000;
        method = &Benchmark::WriteRandom;
      } else if (name == "readseq") {
        method = &Benchmark::ReadSequential;
      } else if (name == "readrandom") {
        method = &Benchmark::ReadRandom;
      } else if (name == "mixgraph") {
        method = &Benchmark::MixGraph;
      } else if (name == "recoveryaftermixgraph") {
        method = &Benchmark::MixGraph;
      } else if (name == "compact") {
        abort();
        //method = &Benchmark::Compact;
      } else if (name == "compactall") {
        abort();
        //CompactAll();
      } else if (name == "compact0") {
        abort();
        //CompactLevel(0);
      } else if (name == "compact1") {
        abort();
        //CompactLevel(1);
      } else if (name == "waitforcompaction") {
        abort();
        //WaitForCompaction();
      } else if (name == "flush") {
        Flush();
      } else if (name == "stats") {
        abort();
        //PrintStats("rocksdb.stats");
      } else if (name == "resetstats") {
        abort();
        //ResetStats();
      } else if (name == "levelstats") {
        abort();
        //PrintStats("rocksdb.levelstats");
      } else if (name == "memstats") {
        abort();
        //std::vector<std::string> keys{"rocksdb.num-immutable-mem-table",
        //                              "rocksdb.cur-size-active-mem-table",
        //                              "rocksdb.cur-size-all-mem-tables",
        //                              "rocksdb.size-all-mem-tables",
        //                              "rocksdb.num-entries-active-mem-table",
        //                              "rocksdb.num-entries-imm-mem-tables"};
        //PrintStats(keys);
      } else if (name == "waitfor10sec") {
        std::this_thread::sleep_for(std::chrono::seconds(10));
      } else if (name == "waitfor30sec") {
        std::this_thread::sleep_for(std::chrono::seconds(30));
      } else if (!name.empty()) {  // No error message for empty name
        fprintf(stderr, "unknown benchmark '%s'\n", name.c_str());
        ErrorExit();
      }

      if (fresh_db) {
        if (FLAGS_use_existing_db) {
          fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n",
                  name.c_str());
          method = nullptr;
        } else {
          //db_->Init();
        }
        //Open(&open_options_);  // use open_options for the last accessed
      }

      if (method != nullptr) {
        fprintf(stdout, "DB path: [%s]\n", FLAGS_db.c_str());

        if (num_warmup > 0) {
          printf("Warming up benchmark by running %d times\n", num_warmup);
        }

        for (int i = 0; i < num_warmup; i++) {
          RunBenchmark(num_threads, name, method);
        }

        if (num_repeat > 1) {
          printf("Running benchmark for %d times\n", num_repeat);
        }

        CombinedStats combined_stats;
        for (int i = 0; i < num_repeat; i++) {
          Stats stats = RunBenchmark(num_threads, name, method);
          combined_stats.AddStats(stats);
        }
        if (num_repeat > 1) {
          combined_stats.Report(name);
        }
      }
      if (post_process_method != nullptr) {
        (this->*post_process_method)();
      }
    }

    if (FLAGS_statistics) {
      abort();
      //fprintf(stdout, "STATISTICS:\n%s\n", dbstats->ToString().c_str());
    }
  }

  DB* db() { return db_; }

 private:
  int listdb_Put(DBClient* client, const std::string_view& key, const std::string_view& value) {
    client->PutStringKV(key, value);
    return 0;
  }

  int listdb_Get(DBClient* client, const std::string_view& key, std::string* value) {
    uint64_t value_addr;
    bool ret = client->GetStringKV(key, &value_addr);
    if (ret) {
      char* p = (char*) value_addr;
      size_t val_len = *((uint64_t*) p);
      p += sizeof(size_t);
      value->assign(p, val_len);
      return 0;
    }
    return 1;
  }

  void Flush() {
    std::this_thread::sleep_for(std::chrono::seconds(3));
    for (int i = 0; i < kNumShards; i++) {
      db_->ManualFlushMemTable(i);
    }
  }

  void PrintHeader() {
    PrintEnvironment();
    fprintf(stdout,
            "Keys:       %d bytes each\n",
            FLAGS_key_size);
    auto avg_value_size = FLAGS_value_size;
    if (FLAGS_value_size_distribution_type_e == kFixed) {
      fprintf(stdout, "Values:     %d bytes each\n",
              avg_value_size);
    } else {
      avg_value_size = (FLAGS_value_size_min + FLAGS_value_size_max) / 2;
      fprintf(stdout, "Values:     %d avg bytes each\n",
              avg_value_size);
      fprintf(stdout, "Values Distribution: %s (min: %d, max: %d)\n",
              FLAGS_value_size_distribution_type.c_str(),
              FLAGS_value_size_min, FLAGS_value_size_max);
    }
    fprintf(stdout, "Entries:    %lu\n", num_);
    //fprintf(stdout, "Prefix:    %d bytes\n", FLAGS_prefix_size);
    //fprintf(stdout, "Keys per prefix:    %lu\n", keys_per_prefix_);
    fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
            ((static_cast<int64_t>(FLAGS_key_size + avg_value_size) * num_)
             / 1048576.0));
    fprintf(stdout, "FileSize:   %.1f MB (estimated)\n",
            (((FLAGS_key_size + avg_value_size)
              * num_)
             / 1048576.0));

    fprintf(stdout, "------------------------------------------------\n");
  }

// Current the following isn't equivalent to OS_LINUX.
#if defined(__linux)
  static std::string_view TrimSpace(const std::string_view& s) {
    unsigned int start = 0;
    while (start < s.size() && isspace(s[start])) {
      start++;
    }
    unsigned int limit = static_cast<unsigned int>(s.size());
    while (limit > start && isspace(s[limit-1])) {
      limit--;
    }
    return std::string_view(s.data() + start, limit - start);
  }
#endif

  void PrintEnvironment() {
    fprintf(stderr, "ListDB:    version %d.%d\n",
            0, 0);

#if defined(__linux) || defined(__APPLE__) || defined(__FreeBSD__)
    time_t now = time(nullptr);
    char buf[52];
    // Lint complains about ctime() usage, so replace it with ctime_r(). The
    // requirement is to provide a buffer which is at least 26 bytes.
    fprintf(stderr, "Date:       %s",
            ctime_r(&now, buf));  // ctime_r() adds newline

#if defined(__linux)
    FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
    if (cpuinfo != nullptr) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != nullptr) {
        const char* sep = strchr(line, ':');
        if (sep == nullptr) {
          continue;
        }
        std::string_view key = TrimSpace(std::string_view(line, sep - 1 - line));
        std::string_view val = TrimSpace(std::string_view(sep + 1));
        if (key == "model name") {
          ++num_cpus;
          cpu_type = val;
        } else if (key == "cache size") {
          cache_size = val;
        }
      }
      fclose(cpuinfo);
      fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#elif defined(__APPLE__)
    struct host_basic_info h;
    size_t hlen = HOST_BASIC_INFO_COUNT;
    if (host_info(mach_host_self(), HOST_BASIC_INFO, (host_info_t)&h,
                  (uint32_t*)&hlen) == KERN_SUCCESS) {
      std::string cpu_type;
      std::string cache_size;
      size_t hcache_size;
      hlen = sizeof(hcache_size);
      if (sysctlbyname("hw.cachelinesize", &hcache_size, &hlen, NULL, 0) == 0) {
        cache_size = std::to_string(hcache_size);
      }
      switch (h.cpu_type) {
        case CPU_TYPE_X86_64:
          cpu_type = "x86_64";
          break;
        case CPU_TYPE_ARM64:
          cpu_type = "arm64";
          break;
        default:
          break;
      }
      fprintf(stderr, "CPU:        %d * %s\n", h.max_cpus, cpu_type.c_str());
      fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#elif defined(__FreeBSD__)
    int ncpus;
    size_t len = sizeof(ncpus);
    int mib[2] = {CTL_HW, HW_NCPU};
    if (sysctl(mib, 2, &ncpus, &len, nullptr, 0) == 0) {
      char cpu_type[16];
      len = sizeof(cpu_type) - 1;
      mib[1] = HW_MACHINE;
      if (sysctl(mib, 2, cpu_type, &len, nullptr, 0) == 0) cpu_type[len] = 0;

      fprintf(stderr, "CPU:        %d * %s\n", ncpus, cpu_type);
      // no programmatic way to get the cache line size except on PPC
    }
#endif
#endif
  }
  //void Open(...);
  void ErrorExit() {
    //DeleteDBs();
    exit(1);
  }

  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
    size_t op_time_arr_size;
    void (Benchmark::*method)(ThreadState*);
  };

  static void ThreadBody(void* v) {
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;

    int id = thread->tid;
    SetAffinity(Numa::CpuSequenceRR(id));
    int r = GetChip();
    DBClient* client = new DBClient(arg->bm->db(), id, r);
    thread->client = client;

    if (arg->op_time_arr_size > 0) {
      thread->op_time_arr = new std::vector<std::pair<int, uint64_t>>();
      thread->op_time_arr->reserve(arg->op_time_arr_size + 1);
    }

    {
      std::unique_lock<std::mutex> lk(shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->cv.notify_all();
      }
      shared->cv.wait(lk, [&]{ return shared->start; });
    }

    //SetPerfLevel(static_cast<PerfLevel> (shared->perf_level));
    //perf_context.EnablePerLevelPerfContext();
    thread->stats.Start(thread->tid);
    (arg->bm->*(arg->method))(thread);
    thread->stats.Stop();

    {
      std::unique_lock<std::mutex> lk(shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->cv.notify_all();
      }
    }
  }

  Stats RunBenchmark(int n, const std::string& name,
                     void (Benchmark::*method)(ThreadState*)) {
    if (name == "fillrandom" && FLAGS_write_threads > 0) {
      n = FLAGS_write_threads;
    }
    SharedState shared;
    shared.total = n;
    shared.num_initialized = 0;
    shared.num_done = 0;
    shared.start = false;

    std::unique_ptr<ReporterAgent> reporter_agent;
    if (FLAGS_report_interval_seconds > 0) {
      reporter_agent.reset(new ReporterAgent(FLAGS_report_file,
                                             FLAGS_report_interval_seconds));
    }

    ThreadArg* arg = new ThreadArg[n];

    std::vector<std::thread> threads;
    for (int i = 0; i < n; i++) {
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      arg[i].thread = new ThreadState(i);
      arg[i].thread->stats.SetReporterAgent(reporter_agent.get());
      arg[i].thread->shared = &shared;
      if (name == "mixgraph") {
        arg[i].op_time_arr_size = reads_;
      } else {
        arg[i].op_time_arr_size = 0;
      }
      //FLAGS_env->StartThread(ThreadBody, &arg[i]);
      threads.emplace_back(ThreadBody, &arg[i]);
    }

    std::unique_lock<std::mutex> lk(shared.mu);
    shared.cv.wait(lk, [&]{ return shared.num_initialized == n; });

    shared.start = true;
    shared.cv.notify_all();
    shared.cv.wait(lk, [&]{ return shared.num_done == n; });
    lk.unlock();

    for (auto& t : threads) { if (t.joinable()) t.join(); }

    // RecoveryAfterMixGraph
    if (name == "recoveryaftermixgraph") {
      RecoveryAfterMixGraph();
    }


    // Stats for some threads can be excluded.
    Stats merge_stats;
    for (int i = 0; i < n; i++) {
      merge_stats.Merge(arg[i].thread->stats);
    }
    merge_stats.Report(name);

    // Op Time Array
    if (arg[0].thread->op_time_arr) {
      fs::remove_all("op_time_output");
      fs::create_directories("op_time_output");
      std::vector<std::thread> dump_threads;
      for (int i = 0; i < n; i++) {
        dump_threads.push_back(std::thread([&, i]{
          std::stringstream fname_ss;
          fname_ss << "op_time_output/" << arg[i].thread->tid << ".dat";
          std::string fname = fname_ss.str();
          std::fstream strm(fname, strm.out);
          for (auto& it : *arg[i].thread->op_time_arr) {
            strm << it.first << " " << it.second << std::endl;
          }
          strm.close();
        }));
      }
      for (auto& t : dump_threads) { if (t.joinable()) t.join(); }
    }

    for (int i = 0; i < n; i++) {
      delete arg[i].thread->client;
      if (arg[i].thread->op_time_arr) {
        delete arg[i].thread->op_time_arr;
      }
      delete arg[i].thread;
    }
    delete[] arg;

    return merge_stats;
  }

  void AcquireLoad(ThreadState* thread) {
    int dummy;
    std::atomic<void*> ap(&dummy);
    int count = 0;
    [[maybe_unused]] void *ptr = nullptr;
    thread->stats.AddMessage("(each op is 1000 loads)");
    while (count < 100000) {
      for (int i = 0; i < 1000; i++) {
        ptr = ap.load(std::memory_order_acquire);
      }
      count++;
      thread->stats.FinishedOps(nullptr, 1, kOthers);
    }
    //if (ptr == nullptr) exit(1);  // Disable unused variable warning.
  }
  enum WriteMode {
    RANDOM, SEQUENTIAL, UNIQUE_RANDOM
  };

  void WriteSeq(ThreadState* thread) {
    DoWrite(thread, SEQUENTIAL);
  }

  void WriteRandom(ThreadState* thread) {
    DoWrite(thread, RANDOM);
  }

  void WriteUniqueRandom(ThreadState* thread) {
    DoWrite(thread, UNIQUE_RANDOM);
  }

  class KeyGenerator {
   public:
    KeyGenerator(Random64* rand, WriteMode mode, uint64_t num,
                 uint64_t /*num_per_set*/ = 64 * 1024)
        : rand_(rand), mode_(mode), num_(num), next_(0) {
      if (mode_ == UNIQUE_RANDOM) {
        // NOTE: if memory consumption of this approach becomes a concern,
        // we can either break it into pieces and only random shuffle a section
        // each time. Alternatively, use a bit map implementation
        // (https://reviews.facebook.net/differential/diff/54627/)
        values_.resize(num_);
        for (uint64_t i = 0; i < num_; ++i) {
          values_[i] = i;
        }
        RandomShuffle(values_.begin(), values_.end(),
                      static_cast<uint32_t>(FLAGS_seed));
      }
    }

    uint64_t Next() {
      switch (mode_) {
        case SEQUENTIAL:
          return next_++;
        case RANDOM:
          return (rand_->Next() % (num_ - 1)) + 1;
        case UNIQUE_RANDOM:
          assert(next_ < num_);
          return values_[next_++];
      }
      assert(false);
      return std::numeric_limits<uint64_t>::max();
    }

    // Only available for UNIQUE_RANDOM mode.
    uint64_t Fetch(uint64_t index) {
      assert(mode_ == UNIQUE_RANDOM);
      assert(index < values_.size());
      return values_[index];
    }

   private:
    Random64* rand_;
    WriteMode mode_;
    const uint64_t num_;
    uint64_t next_;
    std::vector<uint64_t> values_;
  };

  double SineRate(double x) {
    return FLAGS_sine_a*sin((FLAGS_sine_b*x) + FLAGS_sine_c) + FLAGS_sine_d;
  }

  void DoWrite(ThreadState* thread, WriteMode write_mode) {
    const int test_duration = write_mode == RANDOM ? FLAGS_duration : 0;
    const int64_t num_ops = writes_ == 0 ? num_ : writes_;

    size_t num_key_gens = 1;
    std::vector<std::unique_ptr<KeyGenerator>> key_gens(num_key_gens);
    int64_t max_ops = num_ops * num_key_gens;
    int64_t ops_per_stage = max_ops;

    Duration duration(test_duration, max_ops, ops_per_stage);
    const uint64_t num_per_key_gen = num_/* + max_num_range_tombstones_*/;
    for (size_t i = 0; i < num_key_gens; i++) {
      key_gens[i].reset(new KeyGenerator(&(thread->rand), write_mode,
                                         num_per_key_gen, ops_per_stage));
    }

    if (num_ != FLAGS_num) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%lu ops)", num_);
      thread->stats.AddMessage(msg);
    }

    RandomGenerator gen;
    //WriteBatch batch(/*reserved_bytes=*/0, /*max_bytes=*/0,
    //                 user_timestamp_size_);
    int s;  // Status s;
    int64_t bytes = 0;

    std::unique_ptr<const char[]> key_guard;
    std::string_view key = AllocateKey(&key_guard);

    int64_t stage = 0;
    int64_t num_written = 0;
    while ((num_per_key_gen != 0) && !duration.Done(entries_per_batch_)) {
      if (duration.GetStage() != stage) {
        stage = duration.GetStage();
        //if (db_.db != nullptr) {
        //  db_.CreateNewCf(open_options_, stage);
        //} else {
        //  for (auto& db : multi_dbs_) {
        //    db.CreateNewCf(open_options_, stage);
        //  }
        //}
      }

      size_t id = thread->rand.Next() % num_key_gens;
      //DBWithColumnFamilies* db_with_cfh = SelectDBWithCfh(id);
      //batch.Clear();
      int64_t batch_bytes = 0;

      std::string_view val;
      /* for (int64_t j = 0; j < entries_per_batch_; j++) */ {
        int64_t rand_num = 0;
        rand_num = key_gens[id]->Next();
        GenerateKeyFromInt(rand_num, FLAGS_num, &key);
        //std::string_view val;
        val = gen.Generate();
        batch_bytes += val.size() + key_size_;
        bytes += val.size() + key_size_;
        ++num_written;
      }
      if (thread->shared->write_rate_limiter.get() != nullptr) {
        thread->shared->write_rate_limiter->Request(batch_bytes, Env::IO_HIGH, RateLimiter::OpType::kWrite);
        // Set time at which last op finished to Now() to hide latency and
        // sleep from rate limiter. Also, do the check once per batch, not
        // once per write.
        thread->stats.ResetLastOpTime();
      }
      s = listdb_Put(thread->client, key, val);
      thread->stats.FinishedOps(nullptr, entries_per_batch_, kWrite);
      if (s != 0/*!s.ok()*/) {
        fprintf(stderr, "put error\n");
        abort();
      }
    }
    thread->stats.AddBytes(bytes);
  }

  void ReadSequential(ThreadState* thread) {
    abort();
    //if (db_.db != nullptr) {
    //  ReadSequential(thread, db_.db);
    //} else {
    //  for (const auto& db_with_cfh : multi_dbs_) {
    //    ReadSequential(thread, db_with_cfh.db);
    //  }
    //}
  }

  void ReadSequential(ThreadState* thread, DB* db) {
#if 1
    abort();
#else
    ReadOptions options(FLAGS_verify_checksum, true);
    options.tailing = FLAGS_use_tailing_iterator;
    std::unique_ptr<char[]> ts_guard;
    Slice ts;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
      ts = mock_app_clock_->GetTimestampForRead(thread->rand, ts_guard.get());
      options.timestamp = &ts;
    }

    options.adaptive_readahead = FLAGS_adaptive_readahead;
    Iterator* iter = db->NewIterator(options);
    int64_t i = 0;
    int64_t bytes = 0;
    for (iter->SeekToFirst(); i < reads_ && iter->Valid(); iter->Next()) {
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedOps(nullptr, db, 1, kRead);
      ++i;

      if (thread->shared->read_rate_limiter.get() != nullptr &&
          i % 1024 == 1023) {
        thread->shared->read_rate_limiter->Request(1024, Env::IO_HIGH, RateLimiter::OpType::kRead);
      }
    }

    delete iter;
    thread->stats.AddBytes(bytes);
    if (FLAGS_perf_level > ROCKSDB_NAMESPACE::PerfLevel::kDisable) {
      thread->stats.AddMessage(std::string("PERF_CONTEXT:\n") +
                               get_perf_context()->ToString());
    }
#endif
  }

  int64_t GetRandomKey(Random64* rand) {
    uint64_t rand_int = rand->Next();
    int64_t key_rand;
    if (read_random_exp_range_ == 0) {
      key_rand = (rand_int % (FLAGS_num - 1)) + 1;
    } else {
      const uint64_t kBigInt = static_cast<uint64_t>(1U) << 62;
      long double order = -static_cast<long double>(rand_int % kBigInt) /
                          static_cast<long double>(kBigInt) *
                          read_random_exp_range_;
      long double exp_ran = std::exp(order);
      uint64_t rand_num =
          static_cast<int64_t>(exp_ran * static_cast<long double>(FLAGS_num));
      // Map to a different number to avoid locality.
      const uint64_t kBigPrime = 0x5bd1e995;
      // Overflow is like %(2^64). Will have little impact of results.
      key_rand = static_cast<int64_t>((rand_num * kBigPrime) % FLAGS_num);
    }
    return key_rand;
  }

  void ReadRandom(ThreadState* thread) {
    int64_t read = 0;
    int64_t found = 0;
    int64_t bytes = 0;
    //int num_keys = 0;
    int64_t key_rand = 0;
    //ReadOptions options(FLAGS_verify_checksum, true);
    //std::unique_ptr<const char[]> key_guard;
    //Slice key = AllocateKey(&key_guard);
    //PinnableSlice pinnable_val;
    //std::unique_ptr<char[]> ts_guard;
    //Slice ts;
    //if (user_timestamp_size_ > 0) {
    //  ts_guard.reset(new char[user_timestamp_size_]);
    //}
    std::unique_ptr<const char[]> key_guard;
    std::string_view key = AllocateKey(&key_guard);
    std::string value;
    value.reserve(value_size);

    Duration duration(FLAGS_duration, reads_);
    while (!duration.Done(1)) {
      key_rand = GetRandomKey(&thread->rand);
      GenerateKeyFromInt(key_rand, FLAGS_num, &key);
      read++;
      int s;  //Status s;
      //pinnable_val.Reset();
      //if (FLAGS_num_column_families > 1) {
      //  s = db_with_cfh->db->Get(options, db_with_cfh->GetCfh(key_rand), key,
      //                           &pinnable_val, ts_ptr);
      //} else {
      //  s = db_with_cfh->db->Get(options,
      //                           db_with_cfh->db->DefaultColumnFamily(), key,
      //                           &pinnable_val, ts_ptr);
      //}
      //if (s.ok()) {
      //  found++;
      //  bytes += key.size() + pinnable_val.size() + user_timestamp_size_;
      //} else if (!s.IsNotFound()) {
      //  fprintf(stderr, "Get returned an error: %s\n", s.ToString().c_str());
      //  abort();
      //}
      s = listdb_Get(thread->client, key, &value);

      if (s == 0/* s.ok()*/) {
        found++;
        bytes += key.size() + value.size();
      }/* else if (!s.IsNotFound()) {
        fprintf(stderr, "Get returned an error: %s\n", s.ToString().c_str());
        abort();
      }*/

      if (thread->shared->read_rate_limiter.get() != nullptr &&
          read % 256 == 255) {
        thread->shared->read_rate_limiter->Request(256, Env::IO_HIGH, RateLimiter::OpType::kRead);
      }

      thread->stats.FinishedOps(nullptr, 1, kRead);
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "(%lu of %lu found)\n",
             found, read);

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
  }

  // The inverse function of Pareto distribution
  int64_t ParetoCdfInversion(double u, double theta, double k, double sigma) {
    double ret;
    if (k == 0.0) {
      ret = theta - sigma * std::log(u);
    } else {
      ret = theta + sigma * (std::pow(u, -1 * k) - 1) / k;
    }
    return static_cast<int64_t>(ceil(ret));
  }
  // The inverse function of power distribution (y=ax^b)
  int64_t PowerCdfInversion(double u, double a, double b) {
    double ret;
    ret = std::pow((u / a), (1 / b));
    return static_cast<int64_t>(ceil(ret));
  }

  // Add the noice to the QPS
  double AddNoise(double origin, double noise_ratio) {
    if (noise_ratio < 0.0 || noise_ratio > 1.0) {
      return origin;
    }
    int band_int = static_cast<int>(FLAGS_sine_a);
    double delta = (rand() % band_int - band_int / 2) * noise_ratio;
    if (origin + delta < 0) {
      return origin;
    } else {
      return (origin + delta);
    }
  }

  // Decide the ratio of different query types
  // 0 Get, 1 Put, 2 Seek, 3 SeekForPrev, 4 Delete, 5 SingleDelete, 6 merge
  class QueryDecider {
   public:
    std::vector<int> type_;
    std::vector<double> ratio_;
    int range_;

    QueryDecider() {}
    ~QueryDecider() {}

    int Initiate(std::vector<double> ratio_input) {
      int range_max = 1000;
      double sum = 0.0;
      for (auto& ratio : ratio_input) {
        sum += ratio;
      }
      range_ = 0;
      for (auto& ratio : ratio_input) {
        range_ += static_cast<int>(ceil(range_max * (ratio / sum)));
        type_.push_back(range_);
        ratio_.push_back(ratio / sum);
      }
      return 0;
    }

    int GetType(int64_t rand_num) {
      if (rand_num < 0) {
        rand_num = rand_num * (-1);
      }
      assert(range_ != 0);
      int pos = static_cast<int>(rand_num % range_);
      for (int i = 0; i < static_cast<int>(type_.size()); i++) {
        if (pos < type_[i]) {
          return i;
        }
      }
      return 0;
    }
  };

  // KeyrangeUnit is the struct of a keyrange. It is used in a keyrange vector
  // to transfer a random value to one keyrange based on the hotness.
  struct KeyrangeUnit {
    int64_t keyrange_start;
    int64_t keyrange_access;
    int64_t keyrange_keys;
  };

  // From our observations, the prefix hotness (key-range hotness) follows
  // the two-term-exponential distribution: f(x) = a*exp(b*x) + c*exp(d*x).
  // However, we cannot directly use the inverse function to decide a
  // key-range from a random distribution. To achieve it, we create a list of
  // KeyrangeUnit, each KeyrangeUnit occupies a range of integers whose size is
  // decided based on the hotness of the key-range. When a random value is
  // generated based on uniform distribution, we map it to the KeyrangeUnit Vec
  // and one KeyrangeUnit is selected. The probability of a  KeyrangeUnit being
  // selected is the same as the hotness of this KeyrangeUnit. After that, the
  // key can be randomly allocated to the key-range of this KeyrangeUnit, or we
  // can based on the power distribution (y=ax^b) to generate the offset of
  // the key in the selected key-range. In this way, we generate the keyID
  // based on the hotness of the prefix and also the key hotness distribution.
  class GenerateTwoTermExpKeys {
   public:
    // Avoid uninitialized warning-as-error in some compilers
    int64_t keyrange_rand_max_ = 0;
    int64_t keyrange_size_ = 0;
    int64_t keyrange_num_ = 0;
    std::vector<KeyrangeUnit> keyrange_set_;

    // Initiate the KeyrangeUnit vector and calculate the size of each
    // KeyrangeUnit.
    int InitiateExpDistribution(int64_t total_keys, double prefix_a,
                                double prefix_b, double prefix_c,
                                double prefix_d) {
      int64_t amplify = 0;
      int64_t keyrange_start = 0;
      if (FLAGS_keyrange_num <= 0) {
        keyrange_num_ = 1;
      } else {
        keyrange_num_ = FLAGS_keyrange_num;
      }
      keyrange_size_ = total_keys / keyrange_num_;

      // Calculate the key-range shares size based on the input parameters
      for (int64_t pfx = keyrange_num_; pfx >= 1; pfx--) {
        // Step 1. Calculate the probability that this key range will be
        // accessed in a query. It is based on the two-term expoential
        // distribution
        double keyrange_p = prefix_a * std::exp(prefix_b * pfx) +
                            prefix_c * std::exp(prefix_d * pfx);
        if (keyrange_p < std::pow(10.0, -16.0)) {
          keyrange_p = 0.0;
        }
        // Step 2. Calculate the amplify
        // In order to allocate a query to a key-range based on the random
        // number generated for this query, we need to extend the probability
        // of each key range from [0,1] to [0, amplify]. Amplify is calculated
        // by 1/(smallest key-range probability). In this way, we ensure that
        // all key-ranges are assigned with an Integer that  >=0
        if (amplify == 0 && keyrange_p > 0) {
          amplify = static_cast<int64_t>(std::floor(1 / keyrange_p)) + 1;
        }

        // Step 3. For each key-range, we calculate its position in the
        // [0, amplify] range, including the start, the size (keyrange_access)
        KeyrangeUnit p_unit;
        p_unit.keyrange_start = keyrange_start;
        if (0.0 >= keyrange_p) {
          p_unit.keyrange_access = 0;
        } else {
          p_unit.keyrange_access =
              static_cast<int64_t>(std::floor(amplify * keyrange_p));
        }
        p_unit.keyrange_keys = keyrange_size_;
        keyrange_set_.push_back(p_unit);
        keyrange_start += p_unit.keyrange_access;
      }
      keyrange_rand_max_ = keyrange_start;

      // Step 4. Shuffle the key-ranges randomly
      // Since the access probability is calculated from small to large,
      // If we do not re-allocate them, hot key-ranges are always at the end
      // and cold key-ranges are at the begin of the key space. Therefore, the
      // key-ranges are shuffled and the rand seed is only decide by the
      // key-range hotness distribution. With the same distribution parameters
      // the shuffle results are the same.
      Random64 rand_loca(keyrange_rand_max_);
      for (int64_t i = 0; i < FLAGS_keyrange_num; i++) {
        int64_t pos = rand_loca.Next() % FLAGS_keyrange_num;
        assert(i >= 0 && i < static_cast<int64_t>(keyrange_set_.size()) &&
               pos >= 0 && pos < static_cast<int64_t>(keyrange_set_.size()));
        std::swap(keyrange_set_[i], keyrange_set_[pos]);
      }

      // Step 5. Recalculate the prefix start postion after shuffling
      int64_t offset = 0;
      for (auto& p_unit : keyrange_set_) {
        p_unit.keyrange_start = offset;
        offset += p_unit.keyrange_access;
      }

      return 0;
    }

    // Generate the Key ID according to the input ini_rand and key distribution
    int64_t DistGetKeyID(int64_t ini_rand, double key_dist_a,
                         double key_dist_b) {
      int64_t keyrange_rand = ini_rand % keyrange_rand_max_;

      // Calculate and select one key-range that contains the new key
      int64_t start = 0, end = static_cast<int64_t>(keyrange_set_.size());
      while (start + 1 < end) {
        int64_t mid = start + (end - start) / 2;
        assert(mid >= 0 && mid < static_cast<int64_t>(keyrange_set_.size()));
        if (keyrange_rand < keyrange_set_[mid].keyrange_start) {
          end = mid;
        } else {
          start = mid;
        }
      }
      int64_t keyrange_id = start;

      // Select one key in the key-range and compose the keyID
      int64_t key_offset = 0, key_seed;
      if (key_dist_a == 0.0 || key_dist_b == 0.0) {
        key_offset = ini_rand % keyrange_size_;
      } else {
        double u =
            static_cast<double>(ini_rand % keyrange_size_) / keyrange_size_;
        key_seed = static_cast<int64_t>(
            ceil(std::pow((u / key_dist_a), (1 / key_dist_b))));
        Random64 rand_key(key_seed);
        key_offset = rand_key.Next() % keyrange_size_;
      }
      return keyrange_size_ * keyrange_id + key_offset;
    }
  };

  // The social graph workload mixed with Get, Put, Iterator queries.
  // The value size and iterator length follow Pareto distribution.
  // The overall key access follow power distribution. If user models the
  // workload based on different key-ranges (or different prefixes), user
  // can use two-term-exponential distribution to fit the workload. User
  // needs to decide the ratio between Get, Put, Iterator queries before
  // starting the benchmark.
  void MixGraph(ThreadState* thread) {
    int64_t read = 0;  // including single gets and Next of iterators
    int64_t gets = 0;
    int64_t puts = 0;
    int64_t found = 0;
    int64_t seek = 0;
    //int64_t seek_found = 0;
    int64_t bytes = 0;
    const int64_t default_value_max = 1 * 1024 * 1024;
    int64_t value_max = default_value_max;
    //int64_t scan_len_max = FLAGS_mix_max_scan_len;
    double write_rate = 1000000.0;
    double read_rate = 1000000.0;
    bool use_prefix_modeling = false;
    bool use_random_modeling = false;
    GenerateTwoTermExpKeys gen_exp;
    std::vector<double> ratio{FLAGS_mix_get_ratio, FLAGS_mix_put_ratio,
                              FLAGS_mix_seek_ratio};
    //char value_buffer[default_value_max];
    QueryDecider query;
    RandomGenerator gen;
    int s;  //Status s;
    if (value_max > FLAGS_mix_max_value_size) {
      value_max = FLAGS_mix_max_value_size;
    }

    //ReadOptions options(FLAGS_verify_checksum, true);
    std::unique_ptr<const char[]> key_guard;
    //Slice key = AllocateKey(&key_guard);
    std::string_view key = AllocateKey(&key_guard);
    //PinnableSlice pinnable_val;
    std::string value;
    value.reserve(value_max);
    query.Initiate(ratio);

    // the limit of qps initiation
    if (FLAGS_sine_mix_rate) {
      thread->shared->read_rate_limiter.reset(
          NewGenericRateLimiter(static_cast<int64_t>(read_rate)));
      thread->shared->write_rate_limiter.reset(
          NewGenericRateLimiter(static_cast<int64_t>(write_rate)));
    }

    // Decide if user wants to use prefix based key generation
    if (FLAGS_keyrange_dist_a != 0.0 || FLAGS_keyrange_dist_b != 0.0 ||
        FLAGS_keyrange_dist_c != 0.0 || FLAGS_keyrange_dist_d != 0.0) {
      use_prefix_modeling = true;
      gen_exp.InitiateExpDistribution(
          FLAGS_num, FLAGS_keyrange_dist_a, FLAGS_keyrange_dist_b,
          FLAGS_keyrange_dist_c, FLAGS_keyrange_dist_d);
    }
    if (FLAGS_key_dist_a == 0 || FLAGS_key_dist_b == 0) {
      use_random_modeling = true;
    }

    Duration duration(FLAGS_duration, reads_);
    if (thread->op_time_arr) {
      thread->op_time_arr->emplace_back(-1, Clock::NowNanos());
    }
    while (!duration.Done(1)) {
      int64_t ini_rand, rand_v, key_rand, key_seed;
      ini_rand = GetRandomKey(&thread->rand);
      rand_v = ini_rand % FLAGS_num;
      double u = static_cast<double>(rand_v) / FLAGS_num;

      // Generate the keyID based on the key hotness and prefix hotness
      if (use_random_modeling) {
        key_rand = ini_rand;
      } else if (use_prefix_modeling) {
        key_rand =
            gen_exp.DistGetKeyID(ini_rand, FLAGS_key_dist_a, FLAGS_key_dist_b);
      } else {
        key_seed = PowerCdfInversion(u, FLAGS_key_dist_a, FLAGS_key_dist_b);
        Random64 rand(key_seed);
        key_rand = static_cast<int64_t>(rand.Next()) % FLAGS_num;
      }
      GenerateKeyFromInt(key_rand, FLAGS_num, &key);
      int query_type = query.GetType(rand_v);

      // change the qps
      uint64_t now = Clock::NowMicros();
      uint64_t usecs_since_last;
      if (now > thread->stats.GetSineInterval()) {
        usecs_since_last = now - thread->stats.GetSineInterval();
      } else {
        usecs_since_last = 0;
      }

      if (FLAGS_sine_mix_rate &&
          usecs_since_last >
              (FLAGS_sine_mix_rate_interval_milliseconds * uint64_t{1000})) {
        double usecs_since_start =
            static_cast<double>(now - thread->stats.GetStart());
        thread->stats.ResetSineInterval();
        double mix_rate_with_noise = AddNoise(
            SineRate(usecs_since_start / 1000000.0), FLAGS_sine_mix_rate_noise);
        read_rate = mix_rate_with_noise * (query.ratio_[0] + query.ratio_[2]);
        write_rate = mix_rate_with_noise * query.ratio_[1];

        if (read_rate > 0) {
          thread->shared->read_rate_limiter->SetBytesPerSecond(
              static_cast<int64_t>(read_rate));
        }
        if (write_rate > 0) {
          thread->shared->write_rate_limiter->SetBytesPerSecond(
              static_cast<int64_t>(write_rate));
        }
      }
      // Start the query
      if (query_type == 0) {
        // the Get query
        gets++;
        read++;
        //if (FLAGS_num_column_families > 1) {
        //  s = db_with_cfh->db->Get(options, db_with_cfh->GetCfh(key_rand), key,
        //                           &pinnable_val);
        //} else {
        //  pinnable_val.Reset();
        //  s = db_with_cfh->db->Get(options,
        //                           db_with_cfh->db->DefaultColumnFamily(), key,
        //                           &pinnable_val);
        //}
        //pinnable_val.Reset();
        s = listdb_Get(thread->client, key, &value);

        if (s == 0/* s.ok()*/) {
          found++;
          bytes += key.size() + value.size();
        }/* else if (!s.IsNotFound()) {
          fprintf(stderr, "Get returned an error: %s\n", s.ToString().c_str());
          abort();
        }*/

        if (thread->shared->read_rate_limiter && read % 100 == 0) {
          thread->shared->read_rate_limiter->Request(100, Env::IO_HIGH);
        }
        thread->stats.FinishedOps(nullptr, 1, kRead);
      } else if (query_type == 1) {
        // the Put query
        puts++;
        int64_t val_size = ParetoCdfInversion(
            u, FLAGS_value_theta, FLAGS_value_k, FLAGS_value_sigma);
        if (val_size < 0) {
          val_size = 10;
        } else if (val_size > value_max) {
          val_size = val_size % value_max;
        }
        //s = db_with_cfh->db->Put(
        //    write_options_, key,
        //    gen.Generate(static_cast<unsigned int>(val_size)));
        s = listdb_Put(thread->client, key, gen.Generate(static_cast<unsigned int>(val_size)));
        //if (s == 0) {
        //  bytes += key.size() + val_size;
        //}
        //if (!s.ok()) {
        //  fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        //  ErrorExit();
        //}

        if (thread->shared->write_rate_limiter && puts % 100 == 0) {
          thread->shared->write_rate_limiter->Request(100, Env::IO_HIGH);
        }
        thread->stats.FinishedOps(nullptr, 1, kWrite);
      } else if (query_type == 2) {
        fprintf(stdout, "Seek is not supported yet.\n");
        abort();
#if 0
        // Seek query
        if (db_with_cfh->db != nullptr) {
          Iterator* single_iter = nullptr;
          single_iter = db_with_cfh->db->NewIterator(options);
          if (single_iter != nullptr) {
            single_iter->Seek(key);
            seek++;
            read++;
            if (single_iter->Valid() && single_iter->key().compare(key) == 0) {
              seek_found++;
            }
            int64_t scan_length =
                ParetoCdfInversion(u, FLAGS_iter_theta, FLAGS_iter_k,
                                   FLAGS_iter_sigma) %
                scan_len_max;
            for (int64_t j = 0; j < scan_length && single_iter->Valid(); j++) {
              Slice value = single_iter->value();
              memcpy(value_buffer, value.data(),
                     std::min(value.size(), sizeof(value_buffer)));
              bytes += single_iter->key().size() + single_iter->value().size();
              single_iter->Next();
              assert(single_iter->status().ok());
            }
          }
          delete single_iter;
        }
        thread->stats.FinishedOps(nullptr, 1, kSeek);
#endif
      }
      if (thread->op_time_arr) {
        thread->op_time_arr->emplace_back(query_type, Clock::NowNanos());
      }
    }
    char msg[256];
    snprintf(msg, sizeof(msg),
             "( Gets:%lu Puts:%lu Seek:%lu of %lu in %lu found)\n",
             gets, puts, seek, found, read);

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);

    //if (FLAGS_perf_level > ROCKSDB_NAMESPACE::PerfLevel::kDisable) {
    //  thread->stats.AddMessage(std::string("PERF_CONTEXT:\n") +
    //                           get_perf_context()->ToString());
    //}
  }

  void RecoveryAfterMixGraph() {
    fprintf(stdout, "> db_->PrintDebugLsmState(0);\n");
    db_->PrintDebugLsmState(0);
    fprintf(stdout, "> delete db_;\n");
    delete db_;
    fprintf(stdout, "> db_ = new ListDB();\n");
    db_ = new ListDB();
    fprintf(stdout, "> db_->Open();\n");
    auto open_begin_tp = std::chrono::steady_clock::now();
    db_->Open();
    auto open_end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> open_dur = open_end_tp - open_begin_tp;
    fprintf(stdout, "Open() time: %.3lf sec\n", open_dur.count());
    fprintf(stdout, "> db_->PrintDebugLsmState(0);\n");
    db_->PrintDebugLsmState(0);
    fprintf(stdout, "\n");
  }

#ifdef IMNOTDEFINED
  bool binary_search(std::vector<int>& data, int start, int end, int key) {
    if (data.empty()) return false;
    if (start > end) return false;
    int mid = start + (end - start) / 2;
    if (mid > static_cast<int>(data.size()) - 1) return false;
    if (data[mid] == key) {
      return true;
    } else if (data[mid] > key) {
      return binary_search(data, start, mid - 1, key);
    } else {
      return binary_search(data, mid + 1, end, key);
    }
  }

  void ResetStats() {
    abort();
    //if (db_.db != nullptr) {
    //  db_.db->ResetStats();
    //}
    //for (const auto& db_with_cfh : multi_dbs_) {
    //  db_with_cfh.db->ResetStats();
    //}
  }

  void OpenDb(Options options, const std::string& db_name,
      DBWithColumnFamilies* db) {
    uint64_t open_start = FLAGS_report_open_timing ? FLAGS_env->NowNanos() : 0;
    Status s;
    // Open with column families if necessary.
    if (FLAGS_num_column_families > 1) {
      size_t num_hot = FLAGS_num_column_families;
      if (FLAGS_num_hot_column_families > 0 &&
          FLAGS_num_hot_column_families < FLAGS_num_column_families) {
        num_hot = FLAGS_num_hot_column_families;
      } else {
        FLAGS_num_hot_column_families = FLAGS_num_column_families;
      }
      std::vector<ColumnFamilyDescriptor> column_families;
      for (size_t i = 0; i < num_hot; i++) {
        column_families.push_back(ColumnFamilyDescriptor(
              ColumnFamilyName(i), ColumnFamilyOptions(options)));
      }
      std::vector<int> cfh_idx_to_prob;
      if (!FLAGS_column_family_distribution.empty()) {
        std::stringstream cf_prob_stream(FLAGS_column_family_distribution);
        std::string cf_prob;
        int sum = 0;
        while (std::getline(cf_prob_stream, cf_prob, ',')) {
          cfh_idx_to_prob.push_back(std::stoi(cf_prob));
          sum += cfh_idx_to_prob.back();
        }
        if (sum != 100) {
          fprintf(stderr, "column_family_distribution items must sum to 100\n");
          exit(1);
        }
        if (cfh_idx_to_prob.size() != num_hot) {
          fprintf(stderr,
                  "got %" ROCKSDB_PRIszt
                  " column_family_distribution items; expected "
                  "%" ROCKSDB_PRIszt "\n",
                  cfh_idx_to_prob.size(), num_hot);
          exit(1);
        }
      }
#ifndef ROCKSDB_LITE
      if (FLAGS_readonly) {
        s = DB::OpenForReadOnly(options, db_name, column_families,
            &db->cfh, &db->db);
      } else if (FLAGS_optimistic_transaction_db) {
        s = OptimisticTransactionDB::Open(options, db_name, column_families,
                                          &db->cfh, &db->opt_txn_db);
        if (s.ok()) {
          db->db = db->opt_txn_db->GetBaseDB();
        }
      } else if (FLAGS_transaction_db) {
        TransactionDB* ptr;
        TransactionDBOptions txn_db_options;
        if (options.unordered_write) {
          options.two_write_queues = true;
          txn_db_options.skip_concurrency_control = true;
          txn_db_options.write_policy = WRITE_PREPARED;
        }
        s = TransactionDB::Open(options, txn_db_options, db_name,
                                column_families, &db->cfh, &ptr);
        if (s.ok()) {
          db->db = ptr;
        }
      } else {
        s = DB::Open(options, db_name, column_families, &db->cfh, &db->db);
      }
#else
      s = DB::Open(options, db_name, column_families, &db->cfh, &db->db);
#endif  // ROCKSDB_LITE
      db->cfh.resize(FLAGS_num_column_families);
      db->num_created = num_hot;
      db->num_hot = num_hot;
      db->cfh_idx_to_prob = std::move(cfh_idx_to_prob);
#ifndef ROCKSDB_LITE
    } else if (FLAGS_readonly) {
      s = DB::OpenForReadOnly(options, db_name, &db->db);
    } else if (FLAGS_optimistic_transaction_db) {
      s = OptimisticTransactionDB::Open(options, db_name, &db->opt_txn_db);
      if (s.ok()) {
        db->db = db->opt_txn_db->GetBaseDB();
      }
    } else if (FLAGS_transaction_db) {
      TransactionDB* ptr = nullptr;
      TransactionDBOptions txn_db_options;
      if (options.unordered_write) {
        options.two_write_queues = true;
        txn_db_options.skip_concurrency_control = true;
        txn_db_options.write_policy = WRITE_PREPARED;
      }
      s = CreateLoggerFromOptions(db_name, options, &options.info_log);
      if (s.ok()) {
        s = TransactionDB::Open(options, txn_db_options, db_name, &ptr);
      }
      if (s.ok()) {
        db->db = ptr;
      }
    } else if (FLAGS_use_blob_db) {
      // Stacked BlobDB
      blob_db::BlobDBOptions blob_db_options;
      blob_db_options.enable_garbage_collection = FLAGS_blob_db_enable_gc;
      blob_db_options.garbage_collection_cutoff = FLAGS_blob_db_gc_cutoff;
      blob_db_options.is_fifo = FLAGS_blob_db_is_fifo;
      blob_db_options.max_db_size = FLAGS_blob_db_max_db_size;
      blob_db_options.ttl_range_secs = FLAGS_blob_db_ttl_range_secs;
      blob_db_options.min_blob_size = FLAGS_blob_db_min_blob_size;
      blob_db_options.bytes_per_sync = FLAGS_blob_db_bytes_per_sync;
      blob_db_options.blob_file_size = FLAGS_blob_db_file_size;
      blob_db_options.compression = FLAGS_blob_db_compression_type_e;
      blob_db::BlobDB* ptr = nullptr;
      s = blob_db::BlobDB::Open(options, blob_db_options, db_name, &ptr);
      if (s.ok()) {
        db->db = ptr;
      }
    } else if (FLAGS_use_secondary_db) {
      if (FLAGS_secondary_path.empty()) {
        std::string default_secondary_path;
        FLAGS_env->GetTestDirectory(&default_secondary_path);
        default_secondary_path += "/dbbench_secondary";
        FLAGS_secondary_path = default_secondary_path;
      }
      s = DB::OpenAsSecondary(options, db_name, FLAGS_secondary_path, &db->db);
      if (s.ok() && FLAGS_secondary_update_interval > 0) {
        secondary_update_thread_.reset(new port::Thread(
            [this](int interval, DBWithColumnFamilies* _db) {
              while (0 == secondary_update_stopped_.load(
                              std::memory_order_relaxed)) {
                Status secondary_update_status =
                    _db->db->TryCatchUpWithPrimary();
                if (!secondary_update_status.ok()) {
                  fprintf(stderr, "Failed to catch up with primary: %s\n",
                          secondary_update_status.ToString().c_str());
                  break;
                }
                ++secondary_db_updates_;
                FLAGS_env->SleepForMicroseconds(interval * 1000000);
              }
            },
            FLAGS_secondary_update_interval, db));
      }
#endif  // ROCKSDB_LITE
    } else {
      s = DB::Open(options, db_name, &db->db);
    }
    if (FLAGS_report_open_timing) {
      std::cout << "OpenDb:     "
                << (FLAGS_env->NowNanos() - open_start) / 1000000.0
                << " milliseconds\n";
    }
    if (!s.ok()) {
      fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      exit(1);
    }
  }

  void PrintStatsHistory() {
    abort();
    //if (db_.db != nullptr) {
    //  PrintStatsHistoryImpl(db_.db, false);
    //}
    //for (const auto& db_with_cfh : multi_dbs_) {
    //  PrintStatsHistoryImpl(db_with_cfh.db, true);
    //}
  }

  void PrintStatsHistoryImpl(DB* db, bool print_header) {
    if (print_header) {
      fprintf(stdout, "\n==== DB: %s ===\n", db->GetName().c_str());
    }

    std::unique_ptr<StatsHistoryIterator> shi;
    Status s = db->GetStatsHistory(0, port::kMaxUint64, &shi);
    if (!s.ok()) {
      fprintf(stdout, "%s\n", s.ToString().c_str());
      return;
    }
    assert(shi);
    while (shi->Valid()) {
      uint64_t stats_time = shi->GetStatsTime();
      fprintf(stdout, "------ %s ------\n",
              TimeToHumanString(static_cast<int>(stats_time)).c_str());
      for (auto& entry : shi->GetStatsMap()) {
        fprintf(stdout, " %lu   %s  %lu\n", stats_time,
                entry.first.c_str(), entry.second);
      }
      shi->Next();
    }
  }

  void PrintStats(const char* key) {
    abort();
    //if (db_.db != nullptr) {
    //  PrintStats(db_.db, key, false);
    //}
    //for (const auto& db_with_cfh : multi_dbs_) {
    //  PrintStats(db_with_cfh.db, key, true);
    //}
  }

  void PrintStats(DB* db, const char* key, bool print_header = false) {
    abort();
    //if (print_header) {
    //  fprintf(stdout, "\n==== DB: %s ===\n", db->GetName().c_str());
    //}
    //std::string stats;
    //if (!db->GetProperty(key, &stats)) {
    //  stats = "(failed)";
    //}
    //fprintf(stdout, "\n%s\n", stats.c_str());
  }

  void PrintStats(const std::vector<std::string>& keys) {
    abort();
    //if (db_.db != nullptr) {
    //  PrintStats(db_.db, keys);
    //}
    //for (const auto& db_with_cfh : multi_dbs_) {
    //  PrintStats(db_with_cfh.db, keys, true);
    //}
  }

  void PrintStats(DB* db, const std::vector<std::string>& keys,
                  bool print_header = false) {
    if (print_header) {
      fprintf(stdout, "\n==== DB: %s ===\n", db->GetName().c_str());
    }

    for (const auto& key : keys) {
      std::string stats;
      if (!db->GetProperty(key, &stats)) {
        stats = "(failed)";
      }
      fprintf(stdout, "%s: %s\n", key.c_str(), stats.c_str());
    }
  }
#endif // IMNOTDEFINED

  DB* db_;
  int64_t num_;
  int key_size_;
  int64_t entries_per_batch_;
  int64_t reads_;
  double read_random_exp_range_;
  int64_t writes_;
};

//void Benchmark::Open(Options* opts) {
//  if (!InitializeOptionsFromFile(opts)) {
//    InitializeOptionsFromFlags(opts);
//  }
//
//  InitializeOptionsGeneral(opts);
//}

int db_bench_tool(int argc, char** argv) {
  static bool initialized = false;
  if (!initialized) {
    Numa::Init();
    google::SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                    " [OPTIONS]...");
    initialized = true;
  }
  google::ParseCommandLineFlags(&argc, &argv, true);

  FLAGS_value_size_distribution_type_e =
    StringToDistributionType(FLAGS_value_size_distribution_type.c_str());

  if (FLAGS_stats_interval_seconds > 0) {
    // When both are set then FLAGS_stats_interval determines the frequency
    // at which the timer is checked for FLAGS_stats_interval_seconds
    FLAGS_stats_interval = 1000;
  }

  Benchmark benchmark;
  benchmark.Run();

#if 0
  if (FLAGS_print_malloc_stats) {
    std::string stats_string;
    ROCKSDB_NAMESPACE::DumpMallocStats(&stats_string);
    fprintf(stdout, "Malloc stats:\n%s\n", stats_string.c_str());
  }
#endif

  return 0;
}

int main(int argc, char** argv) {
  return db_bench_tool(argc, argv);
}
#endif  // GFLAGS
