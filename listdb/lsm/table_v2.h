#ifndef LISTDB_LSM_TABLE_V2_H_
#define LISTDB_LSM_TABLE_V2_H_

#include <atomic>

#include "listdb/common.h"
#include "listdb/index/lockfree_skiplist.h"

constexpr int kNumClients = 80;
constexpr size_t kBulkRef = 10;
constexpr size_t kBulkSize = 10 * 16;

class TableV2 {
 public:
  TableV2(const size_t capacity);

  void* Put(const Key& key, const Value& value);

  bool Get(const Key& key, void** value_out);

  //virtual Iterator* NewIterator() = 0;

  void SetNext(Table* next);

  Table* Next();

  void w_Ref(const int s);

  void w_UnRef(const int s);

  bool HasRoom(const int s, const size_t size);

  //void RetireSize(const size_t size) {
  //  size_retired_.fetch_add(size, std::memory_order_relaxed);
  //}

 private:
  const size_t capacity_;
  uint64_t sub_ref_[kNumClients];
  //std::atomic<uint64_t> sub_ref_[kNumShards];
  //std::atomic<uint64_t> ref_;
  uint64_t sub_size_reserved_[kNumClients];
  uint64_t sub_size_[kNumClients];
  //std::atomic<uint64_t> sub_size_[kNumShards];
  std::atomic<size_t> size_;
  std::atomic<Table*> next_;
  lockfree_skiplist table_;
};

inline bool w_Ref(const int id) {
  sub_ref_[id]++;
}

inline void w_UnRef(const int id) {
  sub_ref_[id]--;
}

inline bool HasRoom(const int id, const size_t size) {
  if (sub_size_[id] + size > sub_size_reserved_[id]) {
    size_t before = size_.fetch_add(kBulkSize);
    if (before + kBulkSize > capacity_) {
      return false;
    }
    sub_size_reserved_[id] += kBulkSize;
  }
  sub_size_[id] += size;
  return true;
}

#endif  // LISTDB_LSM_TABLE_V2_H_
