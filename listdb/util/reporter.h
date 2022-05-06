#pragma once

class Reporter {
 public:
  enum class OpType {
    kFlush,
    kCompaction,
    kPut,
    kGet
  };
  Reporter(const std::string& fname, uint64_t report_interval_msecs = 1000, std::string header = "");
  ~Reporter();
  void Start();
  void ReportFinishedOps(OpType op_type, int64_t num_ops);

 private:
  std::string Header() const { return "msecs_elapsed,flush_done,compaction_done,put_done,get_done"; }
  void SleepAndReport();

  static constexpr uint64_t kMicrosInMilliSecond = 1000U;

  const uint64_t report_interval_msecs_;
  std::ofstream report_file_;
  std::array<std::atomic<int64_t>, 4> total_ops_done_;
  std::array<int64_t, 4> last_report_;
  std::thread reporting_thread_;
  std::mutex mu_;
  std::condition_variable stop_cv_;
  bool start_;
  bool stop_;
};

Reporter::Reporter(const std::string& fname, uint64_t report_interval_msecs, std::string header)
    : report_interval_msecs_(report_interval_msecs),
      start_(false),
      stop_(false) {
  for (auto& cnt : total_ops_done_) {
    cnt.store(0);
  }
  last_report_.fill(0);
  report_file_.open(fname);
  if (report_file_.good()) {
    if (header.empty()) {
      header = Header();
    }
    report_file_ << header << std::endl;
    report_file_.flush();
  } else {
    fprintf(stderr, "Can't open %s: %s\n", fname.c_str(), std::strerror(errno));
    abort();
  }

  reporting_thread_ = std::thread([&]() { SleepAndReport(); });
}

Reporter::~Reporter() {
  std::unique_lock<std::mutex> lk(mu_);
  stop_ = true;
  stop_cv_.notify_all();
  lk.unlock();
  if (reporting_thread_.joinable()) {
    reporting_thread_.join();
  }
  report_file_.close();
}

void Reporter::Start() {
  std::lock_guard<std::mutex> lk(mu_);
  start_ = true;
  stop_cv_.notify_all();
}

void Reporter::ReportFinishedOps(OpType op_type, int64_t num_ops) {
  total_ops_done_[static_cast<int>(op_type)].fetch_add(num_ops, std::memory_order_relaxed);
}

void Reporter::SleepAndReport() {
  {
    std::unique_lock<std::mutex> lk(mu_);
    stop_cv_.wait(lk, [&]() { return start_ || stop_; });
  }
  auto time_started = Clock::NowMicros();
  while (true) {
    {
      std::unique_lock<std::mutex> lk(mu_);
      if (stop_ || stop_cv_.wait_for(lk, std::chrono::milliseconds(report_interval_msecs_), [&]() { return stop_; })) {
        break;
      }
    }
    auto msecs_elapsed = (Clock::NowMicros() - time_started + kMicrosInMilliSecond / 2) / kMicrosInMilliSecond;
    std::stringstream ss;
    ss << msecs_elapsed << ",";
    for (unsigned int i = 0; i < total_ops_done_.size(); i++) {
      auto total_ops_done_snapshot = total_ops_done_[i].load();
      ss << total_ops_done_snapshot - last_report_[i];
      if (i < total_ops_done_.size() - 1) {
        ss << ",";
      } else  {
        ss << std::endl;
      }
      last_report_[i] = total_ops_done_snapshot;
    }
    report_file_ << ss.rdbuf();
    report_file_.flush();
    if (!report_file_.good()) {
      fprintf(stderr, "Can't write to report file (%s), stopping the reporting\n", std::strerror(errno));
      break;
    }
  }
}
