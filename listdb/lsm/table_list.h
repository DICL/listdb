#ifndef LISTDB_LSM_TABLE_LIST_H_
#define LISTDB_LSM_TABLE_LIST_H_

#include <atomic>
#include <mutex>

#include "listdb/common.h"
#include "listdb/lsm/table.h"
//#include "listdb/lsm/table_v2.h"

class TableList {
 public:
  TableList(const size_t table_capacity);

  void* Put(const Key& key, const Value& value);

  bool Get(const Key&);

  bool IsEmpty();

  void SetFront(Table* table);

  void PushFront(Table* table);

  Table* GetFront();

  Table* GetMutable(const size_t size);

 protected:
  virtual Table* NewMutable(size_t table_capacity, Table* next_table) = 0;

  virtual void EnqueueCompaction(Table* table) { return; };

  const size_t table_capacity_;
  std::mutex init_mu_;
  std::atomic<Table*> front_;
};

TableList::TableList(const size_t table_capacity) : table_capacity_(table_capacity), front_(nullptr) { }

void* TableList::Put(const Key& key, const Value& value) {
  const size_t kv_size = key.size() + 8;
  auto table = GetMutable(kv_size);
  auto ret = table->Put(key, value);
  table->w_UnRef();
  return ret;
}

void TableList::SetFront(Table* table) {
  std::lock_guard<std::mutex> lk(init_mu_);
  front_.store(table);
}

void TableList::PushFront(Table* table) {
  std::lock_guard<std::mutex> lk(init_mu_);
  auto front_old = front_.load();
  table->SetNext(front_old);
  front_.store(table);
}

bool TableList::Get(const Key& key) {
  auto table = GetFront();
  while (table) {
    if (table->Get(key, nullptr)) {
      return true;
    }
    table = table->Next();
  }
  return false;
}

inline bool TableList::IsEmpty() {
  auto ret = front_.load(MO_RELAXED);
  return ret == nullptr;
}

Table* TableList::GetFront() {
  Table* ret = front_.load(MO_RELAXED);
  if (ret == nullptr) {
    std::lock_guard<std::mutex> lk(init_mu_);
    ret = front_.load(MO_RELAXED);
    if (ret == nullptr) {
      ret = NewMutable(table_capacity_, nullptr);
      front_.store(ret, MO_RELAXED);
    }
  }
  return ret;
}

Table* TableList::GetMutable(const size_t size) {
  auto table = GetFront();
  table->w_Ref(MO_RELAXED);
  if (!table->HasRoom(size)) {
    table->w_UnRef(MO_RELAXED);
    // >>> IMPLICIT MFENCE
    std::unique_lock<std::mutex> lk(init_mu_);
    table = front_.load(MO_RELAXED);
    table->w_Ref(MO_RELAXED);
    if (!table->HasRoom(size)) {
      table->w_UnRef(MO_RELAXED);
      auto new_table = NewMutable(table_capacity_, table);
      new_table->HasRoom(size);
      new_table->w_Ref(MO_RELAXED);
      front_.store(new_table, MO_RELAXED);
      lk.unlock();
      EnqueueCompaction(table);
      table = new_table;
    }
    // <<< MFENCE
  }
  return table;
}

#if 0
template <class T>
class TableList {
 public:  
  TableList(const size_t table_capacity);
  void* Put(const Key& key, const Value& value);
  bool Get(const Key&);

  void PrintDebug();

  //Iterator* NewIterator();

 protected:
  T* GetFront();
  T* GetMutable(const size_t size);

  const size_t table_capacity_;
  std::mutex init_mu_;
  std::atomic<T*> front_;
};

template <class T>
TableList<T>::TableList(const size_t table_capacity)
    : table_capacity_(table_capacity),
      front_(nullptr) {
}

template <class T>
void* TableList<T>::Put(const Key& key, const Value& value) {
  const size_t kv_size = key.size() + 8;
  T* table = GetMutable(kv_size);
  auto ret = table->Put(key, value);
  table->w_UnRef();
  return ret;
}

template <class T>
bool TableList<T>::Get(const Key& key) {
  T* table = GetFront();
  while (table) {
    if (table->Get(key, nullptr)) {
      return true;
    }
    table = (T*) table->Next();
  }
  return false;
}

template <class T>
void TableList<T>::PrintDebug() {
  T* curr = front_.load();
  while (curr != nullptr) {
    fprintf(stderr, "[%p]->", curr);
    curr = (T*) curr->Next(MO_RELAXED);
  }
  fprintf(stderr, "\n");
}

template <class T>
T* TableList<T>::GetFront() {
  T* ret = front_.load(MO_RELAXED);
  if (ret == nullptr) {
    std::lock_guard<std::mutex> lk(init_mu_);
    ret = front_.load(MO_RELAXED);
    if (ret == nullptr) {
      ret = NewMutable<T>(nullptr, table_capacity_);
      front_.store(ret, MO_RELAXED);
    }
  }
  return ret;
}

template <class T>
T* TableList<T>::GetMutable(const size_t size) {
  T* ret = GetFront();
  ret->w_Ref(MO_RELAXED);
  if (!ret->HasRoom(size)) {
    //ref->RetireSize(size);
    ret->w_UnRef(MO_RELAXED);
    // >>> IMPLICIT MFENCE
    std::lock_guard<std::mutex> lk(init_mu_);
    ret = front_.load(MO_RELAXED);
    ret->w_Ref(MO_RELAXED);
    if (!ret->HasRoom(size)) {
      //ref->RetireSize(size);
      ret->w_UnRef(MO_RELAXED);
      ret = NewMutable<T>(ret, table_capacity_);
      // TODO(wkim): Enqueue Compaction, Stall
      ret->w_Ref(MO_RELAXED);
      front_.store(ret, MO_RELAXED);
    }
    // <<< MFENCE
  }
  return ret;
}
#endif

#if 0
class TableListV2 {
 public:  
  using T = TableV2;

  TableListV2(const size_t table_capacity);
  void* Put(const Key& key, const Value& value);
  bool Get(const Key&);

  void PrintDebug();

  //Iterator* NewIterator();

 protected:
  T* GetFront();
  T* GetMutable(const size_t size);

  const size_t table_capacity_;
  std::mutex init_mu_;
  std::atomic<T*> front_;
};

TableListV2::TableListV2(const size_t table_capacity)
    : table_capacity_(table_capacity),
      front_(nullptr) {
}

void* TableListV2::Put(const Key& key, const Value& value) {
  const size_t kv_size = key.size() + 8;
  T* table = GetMutable(kv_size);
  auto ret = table->Put(key, value);
  table->w_UnRef();
  return ret;
}

bool TableListV2::Get(const Key& key) {
  T* table = GetFront();
  while (table) {
    if (table->Get(key, nullptr)) {
      return true;
    }
    table = (T*) table->Next();
  }
  return false;
}

void TableListV2::PrintDebug() {
  T* curr = front_.load();
  while (curr != nullptr) {
    fprintf(stderr, "[%p]->", curr);
    curr = (T*) curr->Next(MO_RELAXED);
  }
  fprintf(stderr, "\n");
}

T* TableListV2::GetFront() {
  T* ret = front_.load(MO_RELAXED);
  if (ret == nullptr) {
    std::lock_guard<std::mutex> lk(init_mu_);
    ret = front_.load(MO_RELAXED);
    if (ret == nullptr) {
      ret = NewMutable<T>(nullptr, table_capacity_);
      front_.store(ret, MO_RELAXED);
    }
  }
  return ret;
}

T* TableListV2::GetMutable(const size_t size) {
  T* ret = GetFront();
  ret->w_Ref(MO_RELAXED);
  if (!ret->HasRoom(size)) {
    //ref->RetireSize(size);
    ret->w_UnRef(MO_RELAXED);
    // >>> IMPLICIT MFENCE
    std::lock_guard<std::mutex> lk(init_mu_);
    ret = front_.load(MO_RELAXED);
    ret->w_Ref(MO_RELAXED);
    if (!ret->HasRoom(size)) {
      //ref->RetireSize(size);
      ret->w_UnRef(MO_RELAXED);
      ret = NewMutable<T>(ret, table_capacity_);
      // TODO(wkim): Enqueue Compaction, Stall
      ret->w_Ref(MO_RELAXED);
      front_.store(ret, MO_RELAXED);
    }
    // <<< MFENCE
  }
  return ret;
}
#endif

#endif  // LISTDB_LSM_TABLE_LIST_H_
