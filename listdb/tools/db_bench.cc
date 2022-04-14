//  Copyright (c) 2013-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run db_bench\n");
  return 1;
}
#else
#include <chrono>
#include <condition_variable>
#include <mutex>

#include <gflags/gflags.h>

#include "listdb/db_client.h"
#include "listdb/env.h"
#include "listdb/listdb.h"
#include "listdb/monitoring/histogram.h"
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
    "Comma-separated list of operations to run in the specified order. Available benchmarks:\n"
    "\tfillseq       -- write N values in sequential key order in async mode\n"
    "\tfillrandom    -- write N values in random key order in async mode\n"
    "\treadseq       -- read N times sequentially\n"
    "\treadrandom    -- read N times in random order\n");

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

DEFINE_int32(key_size, 16, "size of each key");

DEFINE_double(compression_ratio, 0.5, "Arrange to generate values that shrink"
              " to this fraction of their original size after compression");

DEFINE_double(read_random_exp_range, 0.0,
              "Read random's key will be generated using distribution of "
              "num * exp(-r) where r is uniform number from 0 to this value. "
              "The larger the number is, the more skewed the reads are. "
              "Only used in readrandom and multireadrandom benchmarks.");

DEFINE_bool(histogram, false, "Print histogram of operation timings");

DEFINE_int64(writes, -1, "Number of write operations to do. If negative, do"
             " --num reads.");

DEFINE_uint64(
    benchmark_write_rate_limit, 0,
    "If non-zero, db_bench will rate-limit the writes going into RocksDB. This "
    "is the global rate in bytes/second.");

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

DEFINE_uint64(
    benchmark_read_rate_limit, 0,
    "If non-zero, db_bench will rate-limit the reads from RocksDB. This "
    "is the global rate in ops/second.");

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
      : num_(FLAGS_num),
        key_size_(FLAGS_key_size),
        reads_(FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads),
        read_random_exp_range_(0.0),
        writes_(FLAGS_writes < 0 ? FLAGS_num : FLAGS_writes) {
  }

  ~Benchmark() {
  }

 private:
  DB* db_;
  int64_t num_;
  int key_size_;
  int64_t reads_;
  double read_random_exp_range_;
  int64_t writes_;
  std::vector<std::string> keys_;

#ifdef IMNOTDEFINED
  Slice AllocateKey(std::unique_ptr<const char[]>* key_guard) {
    char* data = new char[key_size_];
    const char* const_data = data;
    key_guard->reset(const_data);
    return Slice(key_guard->get(), key_size_);
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
  void GenerateKeyFromInt(uint64_t v, int64_t num_keys, Slice* key) {
    if (!keys_.empty()) {
      assert(FLAGS_use_existing_keys);
      assert(keys_.size() == static_cast<size_t>(num_keys));
      assert(v < static_cast<uint64_t>(num_keys));
      *key = keys_[v];
      return;
    }
    char* start = const_cast<char*>(key->data());
    char* pos = start;
    if (keys_per_prefix_ > 0) {
      int64_t num_prefix = num_keys / keys_per_prefix_;
      int64_t prefix = v % num_prefix;
      int bytes_to_fill = std::min(prefix_size_, 8);
      if (port::kLittleEndian) {
        for (int i = 0; i < bytes_to_fill; ++i) {
          pos[i] = (prefix >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
        }
      } else {
        memcpy(pos, static_cast<void*>(&prefix), bytes_to_fill);
      }
      if (prefix_size_ > 8) {
        // fill the rest with 0s
        memset(pos + 8, '0', prefix_size_ - 8);
      }
      pos += prefix_size_;
    }

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

  void GenerateKeyFromIntForSeek(uint64_t v, int64_t num_keys, Slice* key) {
    GenerateKeyFromInt(v, num_keys, key);
    if (FLAGS_seek_missing_prefix) {
      assert(prefix_size_ > 8);
      char* key_ptr = const_cast<char*>(key->data());
      // This rely on GenerateKeyFromInt filling paddings with '0's.
      // Putting a '1' will create a non-existing prefix.
      key_ptr[8] = '1';
    }
  }

  void ErrorExit() {
    exit(1);
  }

  void Run() {
    Open(&open_options_);
    PrintHeader(open_options_);
    std::stringstream benchmark_stream(FLAGS_benchmarks);
    std::string name;
    std::unique_ptr<ExpiredTimeFilter> filter;
    while (std::getline(benchmark_stream, name, ',')) {
      // Sanitize parameters
      num_ = FLAGS_num;
      reads_ = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
      writes_ = (FLAGS_writes < 0 ? FLAGS_num : FLAGS_writes);
      deletes_ = (FLAGS_deletes < 0 ? FLAGS_num : FLAGS_deletes);
      value_size = FLAGS_value_size;
      key_size_ = FLAGS_key_size;
      entries_per_batch_ = FLAGS_batch_size;
      writes_before_delete_range_ = FLAGS_writes_before_delete_range;
      writes_per_range_tombstone_ = FLAGS_writes_per_range_tombstone;
      range_tombstone_width_ = FLAGS_range_tombstone_width;
      max_num_range_tombstones_ = FLAGS_max_num_range_tombstones;
      write_options_ = WriteOptions();
      read_random_exp_range_ = FLAGS_read_random_exp_range;
      if (FLAGS_sync) {
        write_options_.sync = true;
      }
      write_options_.disableWAL = FLAGS_disable_wal;

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

      // Both fillseqdeterministic and filluniquerandomdeterministic
      // fill the levels except the max level with UNIQUE_RANDOM
      // and fill the max level with fillseq and filluniquerandom, respectively
      if (name == "fillseqdeterministic" ||
          name == "filluniquerandomdeterministic") {
        if (!FLAGS_disable_auto_compactions) {
          fprintf(stderr,
                  "Please disable_auto_compactions in FillDeterministic "
                  "benchmark\n");
          ErrorExit();
        }
        if (num_threads > 1) {
          fprintf(stderr,
                  "filldeterministic multithreaded not supported"
                  ", use 1 thread\n");
          num_threads = 1;
        }
        fresh_db = true;
        if (name == "fillseqdeterministic") {
          method = &Benchmark::WriteSeqDeterministic;
        } else {
          method = &Benchmark::WriteUniqueRandomDeterministic;
        }
      } else if (name == "fillseq") {
        fresh_db = true;
        method = &Benchmark::WriteSeq;
      } else if (name == "fillbatch") {
        fresh_db = true;
        entries_per_batch_ = 1000;
        method = &Benchmark::WriteSeq;
      } else if (name == "fillrandom") {
        fresh_db = true;
        method = &Benchmark::WriteRandom;
      } else if (name == "filluniquerandom" ||
                 name == "fillanddeleteuniquerandom") {
        fresh_db = true;
        if (num_threads > 1) {
          fprintf(stderr,
                  "filluniquerandom and fillanddeleteuniquerandom "
                  "multithreaded not supported, use 1 thread");
          num_threads = 1;
        }
        method = &Benchmark::WriteUniqueRandom;
      } else if (name == "overwrite") {
        method = &Benchmark::WriteRandom;
      } else if (name == "fillsync") {
        fresh_db = true;
        num_ /= 1000;
        write_options_.sync = true;
        method = &Benchmark::WriteRandom;
      } else if (name == "fill100K") {
        fresh_db = true;
        num_ /= 1000;
        value_size = 100 * 1000;
        method = &Benchmark::WriteRandom;
      } else if (name == "readseq") {
        method = &Benchmark::ReadSequential;
      } else if (name == "readtorowcache") {
        if (!FLAGS_use_existing_keys || !FLAGS_row_cache_size) {
          fprintf(stderr,
                  "Please set use_existing_keys to true and specify a "
                  "row cache size in readtorowcache benchmark\n");
          ErrorExit();
        }
        method = &Benchmark::ReadToRowCache;
      } else if (name == "readtocache") {
        method = &Benchmark::ReadSequential;
        num_threads = 1;
        reads_ = num_;
      } else if (name == "readreverse") {
        method = &Benchmark::ReadReverse;
      } else if (name == "readrandom") {
        if (FLAGS_multiread_stride) {
          fprintf(stderr, "entries_per_batch = %" PRIi64 "\n",
                  entries_per_batch_);
        }
        method = &Benchmark::ReadRandom;
      } else if (name == "readrandomfast") {
        method = &Benchmark::ReadRandomFast;
      } else if (name == "multireadrandom") {
        fprintf(stderr, "entries_per_batch = %" PRIi64 "\n",
                entries_per_batch_);
        method = &Benchmark::MultiReadRandom;
      } else if (name == "approximatesizerandom") {
        fprintf(stderr, "entries_per_batch = %" PRIi64 "\n",
                entries_per_batch_);
        method = &Benchmark::ApproximateSizeRandom;
      } else if (name == "mixgraph") {
        method = &Benchmark::MixGraph;
      } else if (name == "readmissing") {
        ++key_size_;
        method = &Benchmark::ReadRandom;
      } else if (name == "newiterator") {
        method = &Benchmark::IteratorCreation;
      } else if (name == "newiteratorwhilewriting") {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::IteratorCreationWhileWriting;
      } else if (name == "seekrandom") {
        method = &Benchmark::SeekRandom;
      } else if (name == "seekrandomwhilewriting") {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::SeekRandomWhileWriting;
      } else if (name == "seekrandomwhilemerging") {
        num_threads++;  // Add extra thread for merging
        method = &Benchmark::SeekRandomWhileMerging;
      } else if (name == "readrandomsmall") {
        reads_ /= 1000;
        method = &Benchmark::ReadRandom;
      } else if (name == "deleteseq") {
        method = &Benchmark::DeleteSeq;
      } else if (name == "deleterandom") {
        method = &Benchmark::DeleteRandom;
      } else if (name == "readwhilewriting") {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::ReadWhileWriting;
      } else if (name == "readwhilemerging") {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::ReadWhileMerging;
      } else if (name == "readwhilescanning") {
        num_threads++;  // Add extra thread for scaning
        method = &Benchmark::ReadWhileScanning;
      } else if (name == "readrandomwriterandom") {
        method = &Benchmark::ReadRandomWriteRandom;
      } else if (name == "readrandommergerandom") {
        if (FLAGS_merge_operator.empty()) {
          fprintf(stdout, "%-12s : skipped (--merge_operator is unknown)\n",
                  name.c_str());
          ErrorExit();
        }
        method = &Benchmark::ReadRandomMergeRandom;
      } else if (name == "updaterandom") {
        method = &Benchmark::UpdateRandom;
      } else if (name == "xorupdaterandom") {
        method = &Benchmark::XORUpdateRandom;
      } else if (name == "appendrandom") {
        method = &Benchmark::AppendRandom;
      } else if (name == "mergerandom") {
        if (FLAGS_merge_operator.empty()) {
          fprintf(stdout, "%-12s : skipped (--merge_operator is unknown)\n",
                  name.c_str());
          exit(1);
        }
        method = &Benchmark::MergeRandom;
      } else if (name == "randomwithverify") {
        method = &Benchmark::RandomWithVerify;
      } else if (name == "fillseekseq") {
        method = &Benchmark::WriteSeqSeekSeq;
      } else if (name == "compact") {
        method = &Benchmark::Compact;
      } else if (name == "compactall") {
        CompactAll();
#ifndef ROCKSDB_LITE
      } else if (name == "compact0") {
        CompactLevel(0);
      } else if (name == "compact1") {
        CompactLevel(1);
      } else if (name == "waitforcompaction") {
        WaitForCompaction();
#endif
      } else if (name == "flush") {
        Flush();
      } else if (name == "crc32c") {
        method = &Benchmark::Crc32c;
      } else if (name == "xxhash") {
        method = &Benchmark::xxHash;
      } else if (name == "xxhash64") {
        method = &Benchmark::xxHash64;
      } else if (name == "xxh3") {
        method = &Benchmark::xxh3;
      } else if (name == "acquireload") {
        method = &Benchmark::AcquireLoad;
      } else if (name == "compress") {
        method = &Benchmark::Compress;
      } else if (name == "uncompress") {
        method = &Benchmark::Uncompress;
#ifndef ROCKSDB_LITE
      } else if (name == "randomtransaction") {
        method = &Benchmark::RandomTransaction;
        post_process_method = &Benchmark::RandomTransactionVerify;
#endif  // ROCKSDB_LITE
      } else if (name == "randomreplacekeys") {
        fresh_db = true;
        method = &Benchmark::RandomReplaceKeys;
      } else if (name == "timeseries") {
        timestamp_emulator_.reset(new TimestampEmulator());
        if (FLAGS_expire_style == "compaction_filter") {
          filter.reset(new ExpiredTimeFilter(timestamp_emulator_));
          fprintf(stdout, "Compaction filter is used to remove expired data");
          open_options_.compaction_filter = filter.get();
        }
        fresh_db = true;
        method = &Benchmark::TimeSeries;
      } else if (name == "stats") {
        PrintStats("rocksdb.stats");
      } else if (name == "resetstats") {
        ResetStats();
      } else if (name == "verify") {
        VerifyDBFromDB(FLAGS_truth_db);
      } else if (name == "levelstats") {
        PrintStats("rocksdb.levelstats");
      } else if (name == "memstats") {
        std::vector<std::string> keys{"rocksdb.num-immutable-mem-table",
                                      "rocksdb.cur-size-active-mem-table",
                                      "rocksdb.cur-size-all-mem-tables",
                                      "rocksdb.size-all-mem-tables",
                                      "rocksdb.num-entries-active-mem-table",
                                      "rocksdb.num-entries-imm-mem-tables"};
        PrintStats(keys);
      } else if (name == "sstables") {
        PrintStats("rocksdb.sstables");
      } else if (name == "stats_history") {
        PrintStatsHistory();
#ifndef ROCKSDB_LITE
      } else if (name == "replay") {
        if (num_threads > 1) {
          fprintf(stderr, "Multi-threaded replay is not yet supported\n");
          ErrorExit();
        }
        if (FLAGS_trace_file == "") {
          fprintf(stderr, "Please set --trace_file to be replayed from\n");
          ErrorExit();
        }
        method = &Benchmark::Replay;
#endif  // ROCKSDB_LITE
      } else if (name == "getmergeoperands") {
        method = &Benchmark::GetMergeOperands;
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
          if (db_.db != nullptr) {
            db_.DeleteDBs();
            DestroyDB(FLAGS_db, open_options_);
          }
          Options options = open_options_;
          for (size_t i = 0; i < multi_dbs_.size(); i++) {
            delete multi_dbs_[i].db;
            if (!open_options_.wal_dir.empty()) {
              options.wal_dir = GetPathForMultiple(open_options_.wal_dir, i);
            }
            DestroyDB(GetPathForMultiple(FLAGS_db, i), options);
          }
          multi_dbs_.clear();
        }
        Open(&open_options_);  // use open_options for the last accessed
      }

      if (method != nullptr) {
        fprintf(stdout, "DB path: [%s]\n", FLAGS_db.c_str());

#ifndef ROCKSDB_LITE
        // A trace_file option can be provided both for trace and replay
        // operations. But db_bench does not support tracing and replaying at
        // the same time, for now. So, start tracing only when it is not a
        // replay.
        if (FLAGS_trace_file != "" && name != "replay") {
          std::unique_ptr<TraceWriter> trace_writer;
          Status s = NewFileTraceWriter(FLAGS_env, EnvOptions(),
                                        FLAGS_trace_file, &trace_writer);
          if (!s.ok()) {
            fprintf(stderr, "Encountered an error starting a trace, %s\n",
                    s.ToString().c_str());
            ErrorExit();
          }
          s = db_.db->StartTrace(trace_options_, std::move(trace_writer));
          if (!s.ok()) {
            fprintf(stderr, "Encountered an error starting a trace, %s\n",
                    s.ToString().c_str());
            ErrorExit();
          }
          fprintf(stdout, "Tracing the workload to: [%s]\n",
                  FLAGS_trace_file.c_str());
        }
        // Start block cache tracing.
        if (!FLAGS_block_cache_trace_file.empty()) {
          // Sanity checks.
          if (FLAGS_block_cache_trace_sampling_frequency <= 0) {
            fprintf(stderr,
                    "Block cache trace sampling frequency must be higher than "
                    "0.\n");
            ErrorExit();
          }
          if (FLAGS_block_cache_trace_max_trace_file_size_in_bytes <= 0) {
            fprintf(stderr,
                    "The maximum file size for block cache tracing must be "
                    "higher than 0.\n");
            ErrorExit();
          }
          block_cache_trace_options_.max_trace_file_size =
              FLAGS_block_cache_trace_max_trace_file_size_in_bytes;
          block_cache_trace_options_.sampling_frequency =
              FLAGS_block_cache_trace_sampling_frequency;
          std::unique_ptr<TraceWriter> block_cache_trace_writer;
          Status s = NewFileTraceWriter(FLAGS_env, EnvOptions(),
                                        FLAGS_block_cache_trace_file,
                                        &block_cache_trace_writer);
          if (!s.ok()) {
            fprintf(stderr,
                    "Encountered an error when creating trace writer, %s\n",
                    s.ToString().c_str());
            ErrorExit();
          }
          s = db_.db->StartBlockCacheTrace(block_cache_trace_options_,
                                           std::move(block_cache_trace_writer));
          if (!s.ok()) {
            fprintf(
                stderr,
                "Encountered an error when starting block cache tracing, %s\n",
                s.ToString().c_str());
            ErrorExit();
          }
          fprintf(stdout, "Tracing block cache accesses to: [%s]\n",
                  FLAGS_block_cache_trace_file.c_str());
        }
#endif  // ROCKSDB_LITE

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

    if (secondary_update_thread_) {
      secondary_update_stopped_.store(1, std::memory_order_relaxed);
      secondary_update_thread_->join();
      secondary_update_thread_.reset();
    }

#ifndef ROCKSDB_LITE
    if (name != "replay" && FLAGS_trace_file != "") {
      Status s = db_.db->EndTrace();
      if (!s.ok()) {
        fprintf(stderr, "Encountered an error ending the trace, %s\n",
                s.ToString().c_str());
      }
    }
    if (!FLAGS_block_cache_trace_file.empty()) {
      Status s = db_.db->EndBlockCacheTrace();
      if (!s.ok()) {
        fprintf(stderr,
                "Encountered an error ending the block cache tracing, %s\n",
                s.ToString().c_str());
      }
    }
#endif  // ROCKSDB_LITE

    if (FLAGS_statistics) {
      fprintf(stdout, "STATISTICS:\n%s\n", dbstats->ToString().c_str());
    }
    if (FLAGS_simcache_size >= 0) {
      fprintf(
          stdout, "SIMULATOR CACHE STATISTICS:\n%s\n",
          static_cast_with_check<SimCache>(cache_.get())->ToString().c_str());
    }

#ifndef ROCKSDB_LITE
    if (FLAGS_use_secondary_db) {
      fprintf(stdout, "Secondary instance updated  %lu times.\n",
              secondary_db_updates_);
    }
#endif  // ROCKSDB_LITE
  }
#endif  // IMNOTDEFINED

 private:
  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
    void (Benchmark::*method)(ThreadState*);
  };

  static void ThreadBody(void* v) {
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
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
    SharedState shared;
    shared.total = n;
    shared.num_initialized = 0;
    shared.num_done = 0;
    shared.start = false;
    if (FLAGS_benchmark_write_rate_limit > 0) {
      shared.write_rate_limiter.reset(
          NewGenericRateLimiter(FLAGS_benchmark_write_rate_limit));
    }
    if (FLAGS_benchmark_read_rate_limit > 0) {
      shared.read_rate_limiter.reset(NewGenericRateLimiter(
          FLAGS_benchmark_read_rate_limit, 100000 /* refill_period_us */,
          10 /* fairness */, RateLimiter::Mode::kReadsOnly));
    }

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
      //FLAGS_env->StartThread(ThreadBody, &arg[i]);
      threads.emplace_back(ThreadBody, &arg[i]);
    }

    //shared.mu.Lock();
    //while (shared.num_initialized < n) {
    //  shared.cv.Wait();
    //}
    std::unique_lock<std::mutex> lk(shared.mu);
    shared.cv.wait(lk, [&]{ return shared.num_initialized == n; });

    shared.start = true;
    shared.cv.notify_all();
    //while (shared.num_done < n) {
    //  shared.cv.Wait();
    //}
    shared.cv.wait(lk, [&]{ return shared.num_done == n; });
    //shared.mu.Unlock();
    lk.unlock();

    for (auto& t : threads) { if (t.joinable()) t.join(); }

    // Stats for some threads can be excluded.
    Stats merge_stats;
    for (int i = 0; i < n; i++) {
      merge_stats.Merge(arg[i].thread->stats);
    }
    merge_stats.Report(name);

    for (int i = 0; i < n; i++) {
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

#ifdef IMNOTDEFINED
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
          return rand_->Next() % num_;
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
    if (db_.db == nullptr) {
      num_key_gens = multi_dbs_.size();
    }
    std::vector<std::unique_ptr<KeyGenerator>> key_gens(num_key_gens);
    int64_t max_ops = num_ops * num_key_gens;
    int64_t ops_per_stage = max_ops;
    if (FLAGS_num_column_families > 1 && FLAGS_num_hot_column_families > 0) {
      ops_per_stage = (max_ops - 1) / (FLAGS_num_column_families /
                                       FLAGS_num_hot_column_families) +
                      1;
    }

    Duration duration(test_duration, max_ops, ops_per_stage);
    const uint64_t num_per_key_gen = num_ + max_num_range_tombstones_;
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
    WriteBatch batch(/*reserved_bytes=*/0, /*max_bytes=*/0,
                     user_timestamp_size_);
    Status s;
    int64_t bytes = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    std::unique_ptr<const char[]> begin_key_guard;
    Slice begin_key = AllocateKey(&begin_key_guard);
    std::unique_ptr<const char[]> end_key_guard;
    Slice end_key = AllocateKey(&end_key_guard);
    double p = 0.0;
    uint64_t num_overwrites = 0, num_unique_keys = 0, num_selective_deletes = 0;
    // If user set overwrite_probability flag,
    // check if value is in [0.0,1.0].
    if (FLAGS_overwrite_probability > 0.0) {
      p = FLAGS_overwrite_probability > 1.0 ? 1.0 : FLAGS_overwrite_probability;
      // If overwrite set by user, and UNIQUE_RANDOM mode on,
      // the overwrite_window_size must be > 0.
      if (write_mode == UNIQUE_RANDOM && FLAGS_overwrite_window_size == 0) {
        fprintf(stderr,
                "Overwrite_window_size must be  strictly greater than 0.\n");
        ErrorExit();
      }
    }

    // Default_random_engine provides slightly
    // improved throughput over mt19937.
    std::default_random_engine overwrite_gen{
        static_cast<unsigned int>(FLAGS_seed)};
    std::bernoulli_distribution overwrite_decider(p);

    // Inserted key window is filled with the last N
    // keys previously inserted into the DB (with
    // N=FLAGS_overwrite_window_size).
    // We use a deque struct because:
    // - random access is O(1)
    // - insertion/removal at beginning/end is also O(1).
    std::deque<int64_t> inserted_key_window;
    Random64 reservoir_id_gen(FLAGS_seed);

    // --- Variables used in disposable/persistent keys simulation:
    // The following variables are used when
    // disposable_entries_batch_size is >0. We simualte a workload
    // where the following sequence is repeated multiple times:
    // "A set of keys S1 is inserted ('disposable entries'), then after
    // some delay another set of keys S2 is inserted ('persistent entries')
    // and the first set of keys S1 is deleted. S2 artificially represents
    // the insertion of hypothetical results from some undefined computation
    // done on the first set of keys S1. The next sequence can start as soon
    // as the last disposable entry in the set S1 of this sequence is
    // inserted, if the delay is non negligible"
    bool skip_for_loop = false, is_disposable_entry = true;
    std::vector<uint64_t> disposable_entries_index(num_key_gens, 0);
    std::vector<uint64_t> persistent_ent_and_del_index(num_key_gens, 0);
    const uint64_t kNumDispAndPersEntries =
        FLAGS_disposable_entries_batch_size +
        FLAGS_persistent_entries_batch_size;
    if (kNumDispAndPersEntries > 0) {
      if ((write_mode != UNIQUE_RANDOM) || (writes_per_range_tombstone_ > 0) ||
          (p > 0.0)) {
        fprintf(
            stderr,
            "Disposable/persistent deletes are not compatible with overwrites "
            "and DeleteRanges; and are only supported in filluniquerandom.\n");
        ErrorExit();
      }
      if (FLAGS_disposable_entries_value_size < 0 ||
          FLAGS_persistent_entries_value_size < 0) {
        fprintf(
            stderr,
            "disposable_entries_value_size and persistent_entries_value_size"
            "have to be positive.\n");
        ErrorExit();
      }
    }
    Random rnd_disposable_entry(static_cast<uint32_t>(FLAGS_seed));
    std::string random_value;
    // Queue that stores scheduled timestamp of disposable entries deletes,
    // along with starting index of disposable entry keys to delete.
    std::vector<std::queue<std::pair<uint64_t, uint64_t>>> disposable_entries_q(
        num_key_gens);
    // --- End of variables used in disposable/persistent keys simulation.

    std::vector<std::unique_ptr<const char[]>> expanded_key_guards;
    std::vector<Slice> expanded_keys;
    if (FLAGS_expand_range_tombstones) {
      expanded_key_guards.resize(range_tombstone_width_);
      for (auto& expanded_key_guard : expanded_key_guards) {
        expanded_keys.emplace_back(AllocateKey(&expanded_key_guard));
      }
    }

    std::unique_ptr<char[]> ts_guard;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
    }

    int64_t stage = 0;
    int64_t num_written = 0;
    while ((num_per_key_gen != 0) && !duration.Done(entries_per_batch_)) {
      if (duration.GetStage() != stage) {
        stage = duration.GetStage();
        if (db_.db != nullptr) {
          db_.CreateNewCf(open_options_, stage);
        } else {
          for (auto& db : multi_dbs_) {
            db.CreateNewCf(open_options_, stage);
          }
        }
      }

      size_t id = thread->rand.Next() % num_key_gens;
      DBWithColumnFamilies* db_with_cfh = SelectDBWithCfh(id);
      batch.Clear();
      int64_t batch_bytes = 0;

      for (int64_t j = 0; j < entries_per_batch_; j++) {
        int64_t rand_num = 0;
        if ((write_mode == UNIQUE_RANDOM) && (p > 0.0)) {
          if ((inserted_key_window.size() > 0) &&
              overwrite_decider(overwrite_gen)) {
            num_overwrites++;
            rand_num = inserted_key_window[reservoir_id_gen.Next() %
                                           inserted_key_window.size()];
          } else {
            num_unique_keys++;
            rand_num = key_gens[id]->Next();
            if (inserted_key_window.size() < FLAGS_overwrite_window_size) {
              inserted_key_window.push_back(rand_num);
            } else {
              inserted_key_window.pop_front();
              inserted_key_window.push_back(rand_num);
            }
          }
        } else if (kNumDispAndPersEntries > 0) {
          // Check if queue is non-empty and if we need to insert
          // 'persistent' KV entries (KV entries that are never deleted)
          // and delete disposable entries previously inserted.
          if (!disposable_entries_q[id].empty() &&
              (disposable_entries_q[id].front().first <
               Clock::NowMicros())) {
            // If we need to perform a "merge op" pattern,
            // we first write all the persistent KV entries not targeted
            // by deletes, and then we write the disposable entries deletes.
            if (persistent_ent_and_del_index[id] <
                FLAGS_persistent_entries_batch_size) {
              // Generate key to insert.
              rand_num =
                  key_gens[id]->Fetch(disposable_entries_q[id].front().second +
                                      FLAGS_disposable_entries_batch_size +
                                      persistent_ent_and_del_index[id]);
              persistent_ent_and_del_index[id]++;
              is_disposable_entry = false;
              skip_for_loop = false;
            } else if (persistent_ent_and_del_index[id] <
                       kNumDispAndPersEntries) {
              // Find key of the entry to delete.
              rand_num =
                  key_gens[id]->Fetch(disposable_entries_q[id].front().second +
                                      (persistent_ent_and_del_index[id] -
                                       FLAGS_persistent_entries_batch_size));
              persistent_ent_and_del_index[id]++;
              GenerateKeyFromInt(rand_num, FLAGS_num, &key);
              // For the delete operation, everything happens here and we
              // skip the rest of the for-loop, which is designed for
              // inserts.
              if (FLAGS_num_column_families <= 1) {
                batch.Delete(key);
              } else {
                // We use same rand_num as seed for key and column family so
                // that we can deterministically find the cfh corresponding to a
                // particular key while reading the key.
                batch.Delete(db_with_cfh->GetCfh(rand_num), key);
              }
              // A delete only includes Key+Timestamp (no value).
              batch_bytes += key_size_ + user_timestamp_size_;
              bytes += key_size_ + user_timestamp_size_;
              num_selective_deletes++;
              // Skip rest of the for-loop (j=0, j<entries_per_batch_,j++).
              skip_for_loop = true;
            } else {
              assert(false);  // should never reach this point.
            }
            // If disposable_entries_q needs to be updated (ie: when a selective
            // insert+delete was successfully completed, pop the job out of the
            // queue).
            if (!disposable_entries_q[id].empty() &&
                (disposable_entries_q[id].front().first <
                 Clock::NowMicros()) &&
                persistent_ent_and_del_index[id] == kNumDispAndPersEntries) {
              disposable_entries_q[id].pop();
              persistent_ent_and_del_index[id] = 0;
            }

            // If we are deleting disposable entries, skip the rest of the
            // for-loop since there is no key-value inserts at this moment in
            // time.
            if (skip_for_loop) {
              continue;
            }

          }
          // If no job is in the queue, then we keep inserting disposable KV
          // entries that will be deleted later by a series of deletes.
          else {
            rand_num = key_gens[id]->Fetch(disposable_entries_index[id]);
            disposable_entries_index[id]++;
            is_disposable_entry = true;
            if ((disposable_entries_index[id] %
                 FLAGS_disposable_entries_batch_size) == 0) {
              // Skip the persistent KV entries inserts for now
              disposable_entries_index[id] +=
                  FLAGS_persistent_entries_batch_size;
            }
          }
        } else {
          rand_num = key_gens[id]->Next();
        }
        GenerateKeyFromInt(rand_num, FLAGS_num, &key);
        Slice val;
        if (kNumDispAndPersEntries > 0) {
          random_value = rnd_disposable_entry.RandomString(
              is_disposable_entry ? FLAGS_disposable_entries_value_size
                                  : FLAGS_persistent_entries_value_size);
          val = Slice(random_value);
          num_unique_keys++;
        } else {
          val = gen.Generate();
        }
        if (use_blob_db_) {
#ifndef ROCKSDB_LITE
          // Stacked BlobDB
          blob_db::BlobDB* blobdb =
              static_cast<blob_db::BlobDB*>(db_with_cfh->db);
          if (FLAGS_blob_db_max_ttl_range > 0) {
            int ttl = rand() % FLAGS_blob_db_max_ttl_range;
            s = blobdb->PutWithTTL(write_options_, key, val, ttl);
          } else {
            s = blobdb->Put(write_options_, key, val);
          }
#endif  //  ROCKSDB_LITE
        } else if (FLAGS_num_column_families <= 1) {
          batch.Put(key, val);
        } else {
          // We use same rand_num as seed for key and column family so that we
          // can deterministically find the cfh corresponding to a particular
          // key while reading the key.
          batch.Put(db_with_cfh->GetCfh(rand_num), key,
                    val);
        }
        batch_bytes += val.size() + key_size_ + user_timestamp_size_;
        bytes += val.size() + key_size_ + user_timestamp_size_;
        ++num_written;

        // If all disposable entries have been inserted, then we need to
        // add in the job queue a call for 'persistent entry insertions +
        // disposable entry deletions'.
        if (kNumDispAndPersEntries > 0 && is_disposable_entry &&
            ((disposable_entries_index[id] % kNumDispAndPersEntries) == 0)) {
          // Queue contains [timestamp, starting_idx],
          // timestamp = current_time + delay (minimum aboslute time when to
          // start inserting the selective deletes) starting_idx = index in the
          // keygen of the rand_num to generate the key of the first KV entry to
          // delete (= key of the first selective delete).
          disposable_entries_q[id].push(std::make_pair(
              Clock::NowMicros() +
                  FLAGS_disposable_entries_delete_delay /* timestamp */,
              disposable_entries_index[id] - kNumDispAndPersEntries
              /*starting idx*/));
        }
        if (writes_per_range_tombstone_ > 0 &&
            num_written > writes_before_delete_range_ &&
            (num_written - writes_before_delete_range_) /
                    writes_per_range_tombstone_ <=
                max_num_range_tombstones_ &&
            (num_written - writes_before_delete_range_) %
                    writes_per_range_tombstone_ ==
                0) {
          int64_t begin_num = key_gens[id]->Next();
          if (FLAGS_expand_range_tombstones) {
            for (int64_t offset = 0; offset < range_tombstone_width_;
                 ++offset) {
              GenerateKeyFromInt(begin_num + offset, FLAGS_num,
                                 &expanded_keys[offset]);
              if (use_blob_db_) {
#ifndef ROCKSDB_LITE
                // Stacked BlobDB
                s = db_with_cfh->db->Delete(write_options_,
                                            expanded_keys[offset]);
#endif  //  ROCKSDB_LITE
              } else if (FLAGS_num_column_families <= 1) {
                batch.Delete(expanded_keys[offset]);
              } else {
                batch.Delete(db_with_cfh->GetCfh(rand_num),
                             expanded_keys[offset]);
              }
            }
          } else {
            GenerateKeyFromInt(begin_num, FLAGS_num, &begin_key);
            GenerateKeyFromInt(begin_num + range_tombstone_width_, FLAGS_num,
                               &end_key);
            if (use_blob_db_) {
#ifndef ROCKSDB_LITE
              // Stacked BlobDB
              s = db_with_cfh->db->DeleteRange(
                  write_options_, db_with_cfh->db->DefaultColumnFamily(),
                  begin_key, end_key);
#endif  //  ROCKSDB_LITE
            } else if (FLAGS_num_column_families <= 1) {
              batch.DeleteRange(begin_key, end_key);
            } else {
              batch.DeleteRange(db_with_cfh->GetCfh(rand_num), begin_key,
                                end_key);
            }
          }
        }
      }
      if (thread->shared->write_rate_limiter.get() != nullptr) {
        thread->shared->write_rate_limiter->Request(batch_bytes, Env::IO_HIGH, RateLimiter::OpType::kWrite);
        // Set time at which last op finished to Now() to hide latency and
        // sleep from rate limiter. Also, do the check once per batch, not
        // once per write.
        thread->stats.ResetLastOpTime();
      }
      if (user_timestamp_size_ > 0) {
        Slice user_ts = mock_app_clock_->Allocate(ts_guard.get());
        s = batch.AssignTimestamp(user_ts);
        if (!s.ok()) {
          fprintf(stderr, "assign timestamp to write batch: %s\n",
                  s.ToString().c_str());
          ErrorExit();
        }
      }
      if (!use_blob_db_) {
        // Not stacked BlobDB
        s = db_with_cfh->db->Write(write_options_, &batch);
      }
      thread->stats.FinishedOps(entries_per_batch_, kWrite);
      if (FLAGS_sine_write_rate) {
        uint64_t now = Clock::NowMicros();

        uint64_t usecs_since_last;
        if (now > thread->stats.GetSineInterval()) {
          usecs_since_last = now - thread->stats.GetSineInterval();
        } else {
          usecs_since_last = 0;
        }

        if (usecs_since_last >
            (FLAGS_sine_write_rate_interval_milliseconds * uint64_t{1000})) {
          double usecs_since_start =
                  static_cast<double>(now - thread->stats.GetStart());
          thread->stats.ResetSineInterval();
          uint64_t write_rate =
                  static_cast<uint64_t>(SineRate(usecs_since_start / 1000000.0));
          thread->shared->write_rate_limiter.reset(
                  NewGenericRateLimiter(write_rate));
        }
      }
      if (!s.ok()) {
        s = listener_->WaitForRecovery(600000000) ? Status::OK() : s;
      }

      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        ErrorExit();
      }
    }
    if ((write_mode == UNIQUE_RANDOM) && (p > 0.0)) {
      fprintf(stdout,
              "Number of unique keys inserted: %" PRIu64
              ".\nNumber of overwrites: %lu\n",
              num_unique_keys, num_overwrites);
    } else if (kNumDispAndPersEntries > 0) {
      fprintf(stdout,
              "Number of unique keys inserted (disposable+persistent): %" PRIu64
              ".\nNumber of 'disposable entry delete': %lu\n",
              num_written, num_selective_deletes);
    }
    thread->stats.AddBytes(bytes);
  }

  void ReadSequential(ThreadState* thread) {
    if (db_.db != nullptr) {
      ReadSequential(thread, db_.db);
    } else {
      for (const auto& db_with_cfh : multi_dbs_) {
        ReadSequential(thread, db_with_cfh.db);
      }
    }
  }

  void ReadSequential(ThreadState* thread, DB* db) {
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
  }

  int64_t GetRandomKey(Random64* rand) {
    uint64_t rand_int = rand->Next();
    int64_t key_rand;
    if (read_random_exp_range_ == 0) {
      key_rand = rand_int % FLAGS_num;
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
    int num_keys = 0;
    int64_t key_rand = 0;
    ReadOptions options(FLAGS_verify_checksum, true);
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    PinnableSlice pinnable_val;
    std::unique_ptr<char[]> ts_guard;
    Slice ts;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
    }

    Duration duration(FLAGS_duration, reads_);
    while (!duration.Done(1)) {
      DBWithColumnFamilies* db_with_cfh = SelectDBWithCfh(thread);
      // We use same key_rand as seed for key and column family so that we can
      // deterministically find the cfh corresponding to a particular key, as it
      // is done in DoWrite method.
      if (entries_per_batch_ > 1 && FLAGS_multiread_stride) {
        if (++num_keys == entries_per_batch_) {
          num_keys = 0;
          key_rand = GetRandomKey(&thread->rand);
          if ((key_rand + (entries_per_batch_ - 1) * FLAGS_multiread_stride) >=
              FLAGS_num) {
            key_rand = FLAGS_num - entries_per_batch_ * FLAGS_multiread_stride;
          }
        } else {
          key_rand += FLAGS_multiread_stride;
        }
      } else {
        key_rand = GetRandomKey(&thread->rand);
      }
      GenerateKeyFromInt(key_rand, FLAGS_num, &key);
      read++;
      std::string ts_ret;
      std::string* ts_ptr = nullptr;
      if (user_timestamp_size_ > 0) {
        ts = mock_app_clock_->GetTimestampForRead(thread->rand, ts_guard.get());
        options.timestamp = &ts;
        ts_ptr = &ts_ret;
      }
      Status s;
      pinnable_val.Reset();
      if (FLAGS_num_column_families > 1) {
        s = db_with_cfh->db->Get(options, db_with_cfh->GetCfh(key_rand), key,
                                 &pinnable_val, ts_ptr);
      } else {
        s = db_with_cfh->db->Get(options,
                                 db_with_cfh->db->DefaultColumnFamily(), key,
                                 &pinnable_val, ts_ptr);
      }
      if (s.ok()) {
        found++;
        bytes += key.size() + pinnable_val.size() + user_timestamp_size_;
      } else if (!s.IsNotFound()) {
        fprintf(stderr, "Get returned an error: %s\n", s.ToString().c_str());
        abort();
      }

      if (thread->shared->read_rate_limiter.get() != nullptr &&
          read % 256 == 255) {
        thread->shared->read_rate_limiter->Request(256, Env::IO_HIGH, RateLimiter::OpType::kRead);
      }

      thread->stats.FinishedOps(db_, 1, kRead);
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "(%lu of %lu found)\n",
             found, read);

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);

    if (FLAGS_perf_level > ROCKSDB_NAMESPACE::PerfLevel::kDisable) {
      thread->stats.AddMessage(std::string("PERF_CONTEXT:\n") +
                               get_perf_context()->ToString());
    }
  }

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
    if (db_.db != nullptr) {
      db_.db->ResetStats();
    }
    for (const auto& db_with_cfh : multi_dbs_) {
      db_with_cfh.db->ResetStats();
    }
  }

#ifdef IMNOTDEFINED
  void Open(Options* opts) {
    if (!InitializeOptionsFromFile(opts)) {
      InitializeOptionsFromFlags(opts);
    }

    InitializeOptionsGeneral(opts);
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

    Status Initiate(std::vector<double> ratio_input) {
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
      return Status::OK();
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
    Status InitiateExpDistribution(int64_t total_keys, double prefix_a,
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

      return Status::OK();
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
    int64_t seek_found = 0;
    int64_t bytes = 0;
    const int64_t default_value_max = 1 * 1024 * 1024;
    int64_t value_max = default_value_max;
    int64_t scan_len_max = FLAGS_mix_max_scan_len;
    double write_rate = 1000000.0;
    double read_rate = 1000000.0;
    bool use_prefix_modeling = false;
    bool use_random_modeling = false;
    GenerateTwoTermExpKeys gen_exp;
    std::vector<double> ratio{FLAGS_mix_get_ratio, FLAGS_mix_put_ratio,
                              FLAGS_mix_seek_ratio};
    char value_buffer[default_value_max];
    QueryDecider query;
    RandomGenerator gen;
    Status s;
    if (value_max > FLAGS_mix_max_value_size) {
      value_max = FLAGS_mix_max_value_size;
    }

    ReadOptions options(FLAGS_verify_checksum, true);
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    PinnableSlice pinnable_val;
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
    while (!duration.Done(1)) {
      DBWithColumnFamilies* db_with_cfh = SelectDBWithCfh(thread);
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
        if (FLAGS_num_column_families > 1) {
          s = db_with_cfh->db->Get(options, db_with_cfh->GetCfh(key_rand), key,
                                   &pinnable_val);
        } else {
          pinnable_val.Reset();
          s = db_with_cfh->db->Get(options,
                                   db_with_cfh->db->DefaultColumnFamily(), key,
                                   &pinnable_val);
        }

        if (s.ok()) {
          found++;
          bytes += key.size() + pinnable_val.size();
        } else if (!s.IsNotFound()) {
          fprintf(stderr, "Get returned an error: %s\n", s.ToString().c_str());
          abort();
        }

        if (thread->shared->read_rate_limiter && read % 100 == 0) {
          thread->shared->read_rate_limiter->Request(100, Env::IO_HIGH);
        }
        thread->stats.FinishedOps(db_, 1, kRead);
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
        s = db_with_cfh->db->Put(
            write_options_, key,
            gen.Generate(static_cast<unsigned int>(val_size)));
        if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          ErrorExit();
        }

        if (thread->shared->write_rate_limiter && puts % 100 == 0) {
          thread->shared->write_rate_limiter->Request(100, Env::IO_HIGH);
        }
        thread->stats.FinishedOps(db_, 1, kWrite);
      } else if (query_type == 2) {
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
        thread->stats.FinishedOps(db_, 1, kSeek);
      }
    }
    char msg[256];
    snprintf(msg, sizeof(msg),
             "( Gets:%lu Puts:%lu Seek:%lu of %" PRIu64
             " in %lu found)\n",
             gets, puts, seek, found, read);

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);

    if (FLAGS_perf_level > ROCKSDB_NAMESPACE::PerfLevel::kDisable) {
      thread->stats.AddMessage(std::string("PERF_CONTEXT:\n") +
                               get_perf_context()->ToString());
    }
  }

  void PrintStatsHistory() {
    if (db_.db != nullptr) {
      PrintStatsHistoryImpl(db_.db, false);
    }
    for (const auto& db_with_cfh : multi_dbs_) {
      PrintStatsHistoryImpl(db_with_cfh.db, true);
    }
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
    if (db_.db != nullptr) {
      PrintStats(db_.db, key, false);
    }
    for (const auto& db_with_cfh : multi_dbs_) {
      PrintStats(db_with_cfh.db, key, true);
    }
  }

  void PrintStats(DB* db, const char* key, bool print_header = false) {
    if (print_header) {
      fprintf(stdout, "\n==== DB: %s ===\n", db->GetName().c_str());
    }
    std::string stats;
    if (!db->GetProperty(key, &stats)) {
      stats = "(failed)";
    }
    fprintf(stdout, "\n%s\n", stats.c_str());
  }

  void PrintStats(const std::vector<std::string>& keys) {
    if (db_.db != nullptr) {
      PrintStats(db_.db, keys);
    }
    for (const auto& db_with_cfh : multi_dbs_) {
      PrintStats(db_with_cfh.db, keys, true);
    }
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
#endif // IMNOTDEFINED
};

int db_bench_tool(int argc, char** argv) {
  static bool initialized = false;
  if (!initialized) {
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
