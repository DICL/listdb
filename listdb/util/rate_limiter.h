#ifndef LISTDB_UTIL_RATE_LIMITER_H_
#define LISTDB_UTIL_RATE_LIMITER_H_

#include <condition_variable>
#include <mutex>

#include "listdb/util/clock.h"
#include "listdb/env.h"

// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class RateLimiter {
 public:
  enum class OpType {
    // Limitation: we currently only invoke Request() with OpType::kRead for
    // compactions when DBOptions::new_table_reader_for_compaction_inputs is set
    kRead,
    kWrite,
  };
  enum class Mode {
    kReadsOnly,
    kWritesOnly,
    kAllIo,
  };

  static const char* Type() { return "RateLimiter"; }

  // For API compatibility, default to rate-limiting writes only.
  explicit RateLimiter(Mode mode = Mode::kWritesOnly);

  virtual ~RateLimiter() {}

  // Deprecated. Will be removed in a major release. Derived classes
  // should implement this method.
  virtual const char* Name() const { return ""; }

  // This API allows user to dynamically change rate limiter's bytes per second.
  // REQUIRED: bytes_per_second > 0
  virtual void SetBytesPerSecond(int64_t bytes_per_second) = 0;

  // Deprecated. New RateLimiter derived classes should override
  // Request(const int64_t, const Env::IOPriority, Statistics*) or
  // Request(const int64_t, const Env::IOPriority, Statistics*, OpType)
  // instead.
  //
  // Request for token for bytes. If this request can not be satisfied, the call
  // is blocked. Caller is responsible to make sure
  // bytes <= GetSingleBurstBytes()
  // and bytes >= 0.
  virtual void Request(const int64_t /*bytes*/, const Env::IOPriority /*pri*/) {
    assert(false);
  }

  // Requests token to read or write bytes and potentially updates statistics.
  //
  // If this request can not be satisfied, the call is blocked. Caller is
  // responsible to make sure bytes <= GetSingleBurstBytes()
  // and bytes >= 0.
  virtual void Request(const int64_t bytes, const Env::IOPriority pri, OpType op_type) {
    if (IsRateLimited(op_type)) {
      Request(bytes, pri);
    }
  }

#if 0
  // Requests token to read or write bytes and potentially updates statistics.
  // Takes into account GetSingleBurstBytes() and alignment (e.g., in case of
  // direct I/O) to allocate an appropriate number of bytes, which may be less
  // than the number of bytes requested.
  size_t RequestToken(size_t bytes, size_t alignment,
                              Env::IOPriority io_priority, Statistics* stats,
                              RateLimiter::OpType op_type);
#endif

  // Max bytes can be granted in a single burst
  virtual int64_t GetSingleBurstBytes() const = 0;

  // Total bytes that go through rate limiter
  virtual int64_t GetTotalBytesThrough(
      const Env::IOPriority pri = Env::IO_TOTAL) const = 0;

  // Total # of requests that go through rate limiter
  virtual int64_t GetTotalRequests(
      const Env::IOPriority pri = Env::IO_TOTAL) const = 0;

#if 0
  // Total # of requests that are pending for bytes in rate limiter
  // For convenience, this function is supported by the RateLimiter returned
  // by NewGenericRateLimiter but is not required by RocksDB.
  //
  // REQUIRED: total_pending_request != nullptr
  Status GetTotalPendingRequests(
      int64_t* total_pending_requests,
      const Env::IOPriority pri = Env::IO_TOTAL) const {
    assert(total_pending_requests != nullptr);
    (void)total_pending_requests;
    (void)pri;
    return Status::NotSupported();
  }
#endif

  virtual int64_t GetBytesPerSecond() const = 0;

  bool IsRateLimited(OpType op_type) {
    if ((mode_ == RateLimiter::Mode::kWritesOnly &&
         op_type == RateLimiter::OpType::kRead) ||
        (mode_ == RateLimiter::Mode::kReadsOnly &&
         op_type == RateLimiter::OpType::kWrite)) {
      return false;
    }
    return true;
  }

 protected:
  Mode GetMode() { return mode_; }

 private:
  Mode mode_;
};

class GenericRateLimiter : public RateLimiter {
 public:
  struct GenericRateLimiterOptions {
    static const char* kName() { return "GenericRateLimiterOptions"; }
    GenericRateLimiterOptions(int64_t _rate_bytes_per_sec,
                              int64_t _refill_period_us, int32_t _fairness,
                              bool _auto_tuned)
        : max_bytes_per_sec(_rate_bytes_per_sec),
          refill_period_us(_refill_period_us),
          fairness(_fairness > 100 ? 100 : _fairness),
          auto_tuned(_auto_tuned) {}
    int64_t max_bytes_per_sec;
    int64_t refill_period_us;
    int32_t fairness;
    bool auto_tuned;
  };

 public:
  explicit GenericRateLimiter(
      int64_t refill_bytes, int64_t refill_period_us = 100 * 1000,
      int32_t fairness = 10,
      RateLimiter::Mode mode = RateLimiter::Mode::kWritesOnly,
      bool auto_tuned = false);

  virtual ~GenericRateLimiter();

  static const char* kClassName() { return "GenericRateLimiter"; }
  const char* Name() const override { return kClassName(); }
  //Status PrepareOptions(const ConfigOptions& options) override;

  // This API allows user to dynamically change rate limiter's bytes per second.
  virtual void SetBytesPerSecond(int64_t bytes_per_second) override;

  // Request for token to write bytes. If this request can not be satisfied,
  // the call is blocked. Caller is responsible to make sure
  // bytes <= GetSingleBurstBytes() and bytes >= 0. Negative bytes
  // passed in will be rounded up to 0.
  using RateLimiter::Request;
  virtual void Request(const int64_t bytes, const Env::IOPriority pri) override;

  virtual int64_t GetSingleBurstBytes() const override {
    return refill_bytes_per_period_.load(std::memory_order_relaxed);
  }

  virtual int64_t GetTotalBytesThrough(
      const Env::IOPriority pri = Env::IO_TOTAL) const override {
    std::lock_guard<std::mutex> g(request_mutex_);
    if (pri == Env::IO_TOTAL) {
      int64_t total_bytes_through_sum = 0;
      for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {
        total_bytes_through_sum += total_bytes_through_[i];
      }
      return total_bytes_through_sum;
    }
    return total_bytes_through_[pri];
  }

  virtual int64_t GetTotalRequests(
      const Env::IOPriority pri = Env::IO_TOTAL) const override {
    std::lock_guard<std::mutex> g(request_mutex_);
    if (pri == Env::IO_TOTAL) {
      int64_t total_requests_sum = 0;
      for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {
        total_requests_sum += total_requests_[i];
      }
      return total_requests_sum;
    }
    return total_requests_[pri];
  }

#if 0
  virtual Status GetTotalPendingRequests(
      int64_t* total_pending_requests,
      const Env::IOPriority pri = Env::IO_TOTAL) const override {
    assert(total_pending_requests != nullptr);
    MutexLock g(&request_mutex_);
    if (pri == Env::IO_TOTAL) {
      int64_t total_pending_requests_sum = 0;
      for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {
        total_pending_requests_sum += static_cast<int64_t>(queue_[i].size());
      }
      *total_pending_requests = total_pending_requests_sum;
    } else {
      *total_pending_requests = static_cast<int64_t>(queue_[pri].size());
    }
    return Status::OK();
  }
#endif

  virtual int64_t GetBytesPerSecond() const override {
    return rate_bytes_per_sec_;
  }

 private:
  // Pending request
  struct Req {
    explicit Req(int64_t _bytes)
        : request_bytes(_bytes), bytes(_bytes), granted(false) {}
    int64_t request_bytes;
    int64_t bytes;
    std::condition_variable cv;
    bool granted;
  };

  void Initialize();
  void RefillBytesAndGrantRequests();
  std::vector<Env::IOPriority> GeneratePriorityIterationOrder();
  int64_t CalculateRefillBytesPerPeriod(int64_t rate_bytes_per_sec);
  void Tune();

  uint64_t NowMicrosMonotonic() {
    return Clock::NowNanos() / std::milli::den;
  }

  // This mutex guard all internal states
  mutable std::mutex request_mutex_;

  GenericRateLimiterOptions options_;

  int64_t rate_bytes_per_sec_;
  // This variable can be changed dynamically.
  std::atomic<int64_t> refill_bytes_per_period_;

  bool stop_;
  std::condition_variable exit_cv_;
  int32_t requests_to_wait_;

  int64_t total_requests_[Env::IO_TOTAL];
  int64_t total_bytes_through_[Env::IO_TOTAL];
  int64_t available_bytes_;
  int64_t next_refill_us_;

  Random rnd_;

  struct Req;
  std::deque<Req*> queue_[Env::IO_TOTAL];
  bool wait_until_refill_pending_;

  int64_t num_drains_;
  int64_t prev_num_drains_;
  std::chrono::microseconds tuned_time_;
};

RateLimiter::RateLimiter(Mode mode) : mode_(mode) {
  //RegisterOptions("", &mode_, &rate_limiter_type_info);
}

// --- GenericRateLimiter ---

GenericRateLimiter::GenericRateLimiter(
    int64_t rate_bytes_per_sec, int64_t refill_period_us, int32_t fairness,
    RateLimiter::Mode mode,
    bool auto_tuned)
    : RateLimiter(mode),
      options_(rate_bytes_per_sec, refill_period_us, fairness,
               auto_tuned),
      stop_(false),
      requests_to_wait_(0),
      available_bytes_(0),
      rnd_((uint32_t)time(nullptr)),
      wait_until_refill_pending_(false),
      num_drains_(0),
      prev_num_drains_(0) {
  //RegisterOptions(&options_, &generic_rate_limiter_type_info);
  for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {
    total_requests_[i] = 0;
    total_bytes_through_[i] = 0;
  }
  Initialize();
}
void GenericRateLimiter::Initialize() {
  options_.fairness = std::min(options_.fairness, 100);
  next_refill_us_ = NowMicrosMonotonic();
  tuned_time_ = std::chrono::microseconds(NowMicrosMonotonic());
  if (options_.auto_tuned) {
    rate_bytes_per_sec_ = options_.max_bytes_per_sec / 2;
  } else {
    rate_bytes_per_sec_ = options_.max_bytes_per_sec;
  }
  refill_bytes_per_period_ = CalculateRefillBytesPerPeriod(rate_bytes_per_sec_);
}

GenericRateLimiter::~GenericRateLimiter() {
  std::unique_lock<std::mutex> lk(request_mutex_);
  stop_ = true;
  std::deque<Req*>::size_type queues_size_sum = 0;
  for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {
    queues_size_sum += queue_[i].size();
  }
  requests_to_wait_ = static_cast<int32_t>(queues_size_sum);

  for (int i = Env::IO_TOTAL - 1; i >= Env::IO_LOW; --i) {
    std::deque<Req*> queue = queue_[i];
    for (auto& r : queue) {
      r->cv.notify_one();
    }
  }

  exit_cv_.wait(lk, [&]{ return requests_to_wait_ <= 0; });
}

// This API allows user to dynamically change rate limiter's bytes per second.
void GenericRateLimiter::SetBytesPerSecond(int64_t bytes_per_second) {
  assert(bytes_per_second > 0);
  rate_bytes_per_sec_ = bytes_per_second;
  refill_bytes_per_period_.store(
      CalculateRefillBytesPerPeriod(bytes_per_second),
      std::memory_order_relaxed);
}

void GenericRateLimiter::Request(int64_t bytes, const Env::IOPriority pri) {
  assert(bytes <= refill_bytes_per_period_.load(std::memory_order_relaxed));
  bytes = std::max(static_cast<int64_t>(0), bytes);
  //MutexLock g(&request_mutex_);
  std::unique_lock<std::mutex> lk(request_mutex_);

  if (options_.auto_tuned) {
    static const int kRefillsPerTune = 100;
    std::chrono::microseconds now(NowMicrosMonotonic());
    if (now - tuned_time_ >= kRefillsPerTune * std::chrono::microseconds(
                                                   options_.refill_period_us)) {
      Tune();
    }
  }

  if (stop_) {
    // It is now in the clean-up of ~GenericRateLimiter().
    // Therefore any new incoming request will exit from here
    // and not get satiesfied.
    return;
  }

  ++total_requests_[pri];

  if (available_bytes_ >= bytes) {
    // Refill thread assigns quota and notifies requests waiting on
    // the queue under mutex. So if we get here, that means nobody
    // is waiting?
    available_bytes_ -= bytes;
    total_bytes_through_[pri] += bytes;
    return;
  }

  // Request cannot be satisfied at this moment, enqueue
  Req r(bytes);
  queue_[pri].push_back(&r);
  // A thread representing a queued request coordinates with other such threads.
  // There are two main duties.
  //
  // (1) Waiting for the next refill time.
  // (2) Refilling the bytes and granting requests.
  do {
    int64_t time_until_refill_us = next_refill_us_ - NowMicrosMonotonic();
    if (time_until_refill_us > 0) {
      if (wait_until_refill_pending_) {
        // Somebody is performing (1). Trust we'll be woken up when our request
        // is granted or we are needed for future duties.
        //r.cv.Wait();
        r.cv.wait(lk);
      } else {
        // Whichever thread reaches here first performs duty (1) as described
        // above.
        //int64_t wait_until = Clock::NowMicros() + time_until_refill_us;
        //RecordTick(stats, NUMBER_RATE_LIMITER_DRAINS);
        ++num_drains_;
        wait_until_refill_pending_ = true;
        //r.cv.TimedWait(wait_until);
        r.cv.wait_for(lk, std::chrono::duration<int64_t, std::micro>(time_until_refill_us));
        wait_until_refill_pending_ = false;
      }
    } else {
      // Whichever thread reaches here first performs duty (2) as described
      // above.
      RefillBytesAndGrantRequests();
      if (r.granted) {
        // If there is any remaining requests, make sure there exists at least
        // one candidate is awake for future duties by signaling a front request
        // of a queue.
        for (int i = Env::IO_TOTAL - 1; i >= Env::IO_LOW; --i) {
          std::deque<Req*> queue = queue_[i];
          if (!queue.empty()) {
            queue.front()->cv.notify_one();
            break;
          }
        }
      }
    }
    // Invariant: non-granted request is always in one queue, and granted
    // request is always in zero queues.
#ifndef NDEBUG
    int num_found = 0;
    for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {
      if (std::find(queue_[i].begin(), queue_[i].end(), &r) !=
          queue_[i].end()) {
        ++num_found;
      }
    }
    if (r.granted) {
      assert(num_found == 0);
    } else {
      assert(num_found == 1);
    }
#endif  // NDEBUG
  } while (!stop_ && !r.granted);

  if (stop_) {
    // It is now in the clean-up of ~GenericRateLimiter().
    // Therefore any woken-up request will have come out of the loop and then
    // exit here. It might or might not have been satisfied.
    --requests_to_wait_;
    exit_cv_.notify_one();
  }
}

std::vector<Env::IOPriority>
GenericRateLimiter::GeneratePriorityIterationOrder() {
  std::vector<Env::IOPriority> pri_iteration_order(Env::IO_TOTAL /* 4 */);
  // We make Env::IO_USER a superior priority by always iterating its queue
  // first
  pri_iteration_order[0] = Env::IO_USER;

  bool high_pri_iterated_after_mid_low_pri = rnd_.OneIn(options_.fairness);
  //TEST_SYNC_POINT_CALLBACK(
  //    "GenericRateLimiter::GeneratePriorityIterationOrder::"
  //    "PostRandomOneInFairnessForHighPri",
  //    &high_pri_iterated_after_mid_low_pri);
  bool mid_pri_itereated_after_low_pri = rnd_.OneIn(options_.fairness);
  //TEST_SYNC_POINT_CALLBACK(
  //    "GenericRateLimiter::GeneratePriorityIterationOrder::"
  //    "PostRandomOneInFairnessForMidPri",
  //    &mid_pri_itereated_after_low_pri);

  if (high_pri_iterated_after_mid_low_pri) {
    pri_iteration_order[3] = Env::IO_HIGH;
    pri_iteration_order[2] =
        mid_pri_itereated_after_low_pri ? Env::IO_MID : Env::IO_LOW;
    pri_iteration_order[1] =
        (pri_iteration_order[2] == Env::IO_MID) ? Env::IO_LOW : Env::IO_MID;
  } else {
    pri_iteration_order[1] = Env::IO_HIGH;
    pri_iteration_order[3] =
        mid_pri_itereated_after_low_pri ? Env::IO_MID : Env::IO_LOW;
    pri_iteration_order[2] =
        (pri_iteration_order[3] == Env::IO_MID) ? Env::IO_LOW : Env::IO_MID;
  }

  //TEST_SYNC_POINT_CALLBACK(
  //    "GenericRateLimiter::GeneratePriorityIterationOrder::"
  //    "PreReturnPriIterationOrder",
  //    &pri_iteration_order);
  return pri_iteration_order;
}

void GenericRateLimiter::RefillBytesAndGrantRequests() {
  //TEST_SYNC_POINT("GenericRateLimiter::RefillBytesAndGrantRequests");
  next_refill_us_ = NowMicrosMonotonic() + options_.refill_period_us;
  // Carry over the left over quota from the last period
  auto refill_bytes_per_period =
      refill_bytes_per_period_.load(std::memory_order_relaxed);
  if (available_bytes_ < refill_bytes_per_period) {
    available_bytes_ += refill_bytes_per_period;
  }

  std::vector<Env::IOPriority> pri_iteration_order =
      GeneratePriorityIterationOrder();

  for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {
    assert(!pri_iteration_order.empty());
    Env::IOPriority current_pri = pri_iteration_order[i];
    auto* queue = &queue_[current_pri];
    while (!queue->empty()) {
      auto* next_req = queue->front();
      if (available_bytes_ < next_req->request_bytes) {
        // Grant partial request_bytes to avoid starvation of requests
        // that become asking for more bytes than available_bytes_
        // due to dynamically reduced rate limiter's bytes_per_second that
        // leads to reduced refill_bytes_per_period hence available_bytes_
        next_req->request_bytes -= available_bytes_;
        available_bytes_ = 0;
        break;
      }
      available_bytes_ -= next_req->request_bytes;
      next_req->request_bytes = 0;
      total_bytes_through_[current_pri] += next_req->bytes;
      queue->pop_front();

      next_req->granted = true;
      // Quota granted, signal the thread to exit
      //next_req->cv.Signal();
      next_req->cv.notify_one();
    }
  }
}

int64_t GenericRateLimiter::CalculateRefillBytesPerPeriod(
    int64_t rate_bytes_per_sec) {
  if (std::numeric_limits<int64_t>::max() / rate_bytes_per_sec < options_.refill_period_us) {
    // Avoid unexpected result in the overflow case. The result now is still
    // inaccurate but is a number that is large enough.
    return std::numeric_limits<int64_t>::max() / 1000000;
  } else {
    return rate_bytes_per_sec * options_.refill_period_us / 1000000;
  }
}

void GenericRateLimiter::Tune() {
  const int kLowWatermarkPct = 50;
  const int kHighWatermarkPct = 90;
  const int kAdjustFactorPct = 5;
  // computed rate limit will be in
  // `[max_bytes_per_sec_ / kAllowedRangeFactor, max_bytes_per_sec_]`.
  const int kAllowedRangeFactor = 20;

  std::chrono::microseconds prev_tuned_time = tuned_time_;
  tuned_time_ = std::chrono::microseconds(NowMicrosMonotonic());

  int64_t elapsed_intervals =
      (tuned_time_ - prev_tuned_time +
       std::chrono::microseconds(options_.refill_period_us) -
       std::chrono::microseconds(1)) /
      std::chrono::microseconds(options_.refill_period_us);
  // We tune every kRefillsPerTune intervals, so the overflow and division-by-
  // zero conditions should never happen.
  assert(num_drains_ - prev_num_drains_ <= std::numeric_limits<int64_t>::max() / 100);
  assert(elapsed_intervals > 0);
  int64_t drained_pct =
      (num_drains_ - prev_num_drains_) * 100 / elapsed_intervals;

  int64_t prev_bytes_per_sec = GetBytesPerSecond();
  int64_t new_bytes_per_sec;
  if (drained_pct == 0) {
    new_bytes_per_sec = options_.max_bytes_per_sec / kAllowedRangeFactor;
  } else if (drained_pct < kLowWatermarkPct) {
    // sanitize to prevent overflow
    int64_t sanitized_prev_bytes_per_sec =
        std::min(prev_bytes_per_sec, std::numeric_limits<int64_t>::max() / 100);
    new_bytes_per_sec =
        std::max(options_.max_bytes_per_sec / kAllowedRangeFactor,
                 sanitized_prev_bytes_per_sec * 100 / (100 + kAdjustFactorPct));
  } else if (drained_pct > kHighWatermarkPct) {
    // sanitize to prevent overflow
    int64_t sanitized_prev_bytes_per_sec = std::min(
        prev_bytes_per_sec, std::numeric_limits<int64_t>::max() / (100 + kAdjustFactorPct));
    new_bytes_per_sec =
        std::min(options_.max_bytes_per_sec,
                 sanitized_prev_bytes_per_sec * (100 + kAdjustFactorPct) / 100);
  } else {
    new_bytes_per_sec = prev_bytes_per_sec;
  }
  if (new_bytes_per_sec != prev_bytes_per_sec) {
    SetBytesPerSecond(new_bytes_per_sec);
  }
  num_drains_ = prev_num_drains_;
  //return Status::OK();
}

RateLimiter* NewGenericRateLimiter(
    int64_t rate_bytes_per_sec, int64_t refill_period_us = 100 * 1000,
    int32_t fairness = 10,
    RateLimiter::Mode mode = RateLimiter::Mode::kWritesOnly,
    bool auto_tuned = false) {
  assert(rate_bytes_per_sec > 0);
  assert(refill_period_us > 0);
  assert(fairness > 0);
  std::unique_ptr<RateLimiter> limiter(
      new GenericRateLimiter(rate_bytes_per_sec, refill_period_us, fairness,
                             mode, auto_tuned));
  //Status s = limiter->PrepareOptions(ConfigOptions());
  if (true /* s.ok() */) {
    return limiter.release();
  } else {
    assert(false);
    return nullptr;
  }
}



#endif  // LISTDB_UTIL_RATE_LIMITER_H_
