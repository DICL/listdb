#ifndef LISTDB_CORE_BUFFERED_PMEM_LOG_H_
#define LISTDB_CORE_BUFFERED_PMEM_LOG_H_

#include "listdb/core/pmem_log.h"

constexpr size_t kLogBufferSize = 1ull << 10;

class BufferedPmemLog {
 public:
  void Add(Key& key, uint64_t tag, Value& value, int height);

};

void BufferedPmemLog::Add(const Key& key, const uint64_t tag, const Value& value, const int height) {
  size_t log_size = key.size() + /*tag*/8 + /*value*/8 + /*height(aligned)*/8 + /*next_ptrs*/(8 * height);
  size_t before = buffered_size_.fetch_add(log_size, MO_RELAXED);
  if (before + log_size <= kLogBufferSize) {
    char* p = buf_ + before;
    *reinterpret_cast<Key*>(p) = key;
    p += 8;
  }
}

#endif  // LISTDB_CORE_BUFFERED_PMEM_LOG_H_
