#ifndef LISTDB_LSM_TABLE_H_
#define LISTDB_LSM_TABLE_H_

#include <atomic>
#include <map>

#include "listdb/common.h"

class Table {
 public:
  Table(const size_t capacity, TableType type);

  virtual void* Put(const Key& key, const Value& value) = 0;

  virtual bool Get(const Key& key, void** value_out) = 0;

  TableType type() { return type_; }

  //virtual Iterator* NewIterator() = 0;

  void SetNext(Table* next, const std::memory_order mo = std::memory_order_seq_cst);

  Table* Next(const std::memory_order mo = std::memory_order_seq_cst);

  void w_Ref(const std::memory_order mo = std::memory_order_seq_cst);

  void w_UnRef(const std::memory_order mo = std::memory_order_seq_cst);

  bool HasRoom(const size_t size, const std::memory_order mo = std::memory_order_seq_cst);

  int64_t w_RefCount () { return writer_ref_cnt_ - writer_unref_cnt_; }

  void SetSize(const size_t size) { size_.store(size); }

  //void RetireSize(const size_t size) {
  //  size_retired_.fetch_add(size, std::memory_order_relaxed);
  //}

  //virtual Table* NewMutable(Table* const prev, const size_t capacity) = 0;

 protected:
  const size_t capacity_;
  TableType type_;
  std::atomic<int64_t> writer_ref_cnt_ = 0;
  std::atomic<size_t> size_;
  std::atomic<int64_t> writer_unref_cnt_ = 0;
  std::atomic<Table*> next_;
  //std::atomic<size_t> size_retired_;
};

Table::Table(const size_t capacity, TableType type) : capacity_(capacity), type_(type), size_(0), next_(nullptr) { }

inline void Table::SetNext(Table* next, const std::memory_order mo) {
  next_.store(next, mo);
}

inline Table* Table::Next(const std::memory_order mo) {
  return next_.load(mo);
}

// XXX: fetch_add with relaxed memory order is effectively promoted to seq_cst for x86-64.
inline void Table::w_Ref(const std::memory_order mo) {
  writer_ref_cnt_.fetch_add(1, mo);
}

inline void Table::w_UnRef(const std::memory_order mo) {
  writer_unref_cnt_.fetch_add(1, mo);
}

inline bool Table::HasRoom(const size_t size, const std::memory_order mo) {
  const size_t before = size_.fetch_add(size, mo);
  return (before + size <= capacity_);
}

#endif  // LISTDB_LSM_TABLE_H_
