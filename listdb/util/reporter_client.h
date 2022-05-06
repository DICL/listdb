#pragma once

#include "listdb/util/reporter.h"

class ReporterClient {
 public:
  using OpType = Reporter::OpType;
  ReporterClient(Reporter* reporter, uint64_t report_interval_msecs = 100)
    : reporter_(reporter),
      report_interval_msecs_(report_interval_msecs),
      last_up_report_time_(Clock::NowMicros()) {
    total_ops_done_.fill(0);
    last_report_.fill(0);
  }

  ~ReporterClient() {
    if (reporter_ != nullptr) {
      for (unsigned int i = 0; i < total_ops_done_.size(); i++) {
        reporter_->ReportFinishedOps(static_cast<OpType>(i), total_ops_done_[i] - last_report_[i]);
      }
    }
  }

  void ReportFinishedOps(OpType op_type, int64_t num_ops) {
    total_ops_done_[static_cast<int>(op_type)] += num_ops;
    if (reporter_ != nullptr) {
      uint64_t now_micro = Clock::NowMicros();
      auto msec_since_last = (now_micro - last_up_report_time_ + kMicrosInMilliSecond / 2) / kMicrosInMilliSecond;
      if (msec_since_last > report_interval_msecs_) {
        for (unsigned int i = 0; i < total_ops_done_.size(); i++) {
          reporter_->ReportFinishedOps(static_cast<OpType>(i), total_ops_done_[i] - last_report_[i]);
          last_report_[i] = total_ops_done_[i];
        }
        last_up_report_time_ = now_micro;
      }
    }
  }

 private:
  static constexpr uint64_t kMicrosInMilliSecond = 1000U;

  Reporter* reporter_;  // not owned.
  const uint64_t report_interval_msecs_;
  uint64_t last_up_report_time_;  // micro
  std::array<int64_t, 4> total_ops_done_;
  std::array<int64_t, 4> last_report_;
};
