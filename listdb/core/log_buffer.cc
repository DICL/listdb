#include "listdb/core/log_buffer.h"

// Not thread-safe
// per-thread use only
void LogBuffer::Add(const Key& key, const uint64_t tag, const Value& value, const int height) {
  const static size_t kValueSize = sizeof(uint64_t);
  size_t log_size = key.size() + kTagSize + sizeof(tag) + kValueSize + 8 + 8 * height;

  if (buffered_size_ + log_size > kLogBufferSize)  {
    Flush();
    buffered_size_ = 0;
  }

  insert_ops_.emplace_back(key, tag, value, height, log_size);
  buffered_size_ += log_size;
}
