#ifndef LISTDB_LSM_MEMTABLE_H_
#define LISTDB_LSM_MEMTABLE_H_

#include <cstdio>

#include "listdb/index/lockfree_skiplist.h"
#include "listdb/index/braided_pmem_skiplist.h"
#include "listdb/lsm/table.h"

class MemTable : public Table {
 public:
  using Node = lockfree_skiplist::Node;

  MemTable(const size_t table_capacity);
  ~MemTable();

  virtual void* Put(const Key& key, const Value& value) override;

  virtual bool Get(const Key& key, void** value_out) override;

  bool IsFlushed() { return (l0_ != nullptr); }

  void SetPersistentTable(Table* pmemtable) { l0_ = pmemtable; }

  Table* PersistentTable() { return l0_; }

  lockfree_skiplist* skiplist() { return skiplist_; }

  BraidedPmemSkipList* l0_skiplist() { return l0_skiplist_; }

  void SetL0SkipList(BraidedPmemSkipList* l0_skiplist) { l0_skiplist_ = l0_skiplist; }

  void SetL0Manifest(pmem::obj::persistent_ptr<pmem_l0_info> l0_manifest) { l0_manifest_ = l0_manifest; }

  pmem::obj::persistent_ptr<pmem_l0_info> l0_manifest() { return l0_manifest_; }

  uint64_t l0_id() const { return l0_manifest_->id; }

 private:
  lockfree_skiplist* skiplist_;
  BraidedPmemSkipList* l0_skiplist_ = nullptr;
  // TODO(wkim): use PmemTable*
  Table* l0_ = nullptr;
  pmem::obj::persistent_ptr<pmem_l0_info> l0_manifest_ = nullptr;
};

MemTable::MemTable(const size_t table_capacity) : Table(table_capacity, TableType::kMemTable) {
  skiplist_ = new lockfree_skiplist();
}

MemTable::~MemTable() {
  delete skiplist_;
}

void* MemTable::Put(const Key& key, const Value& value) {
  fprintf(stdout, "Not impl!!!! returning NULL\n");
  return nullptr;
}

bool MemTable::Get(const Key& key, void** value_out) {
  fprintf(stdout, "Not impl!!!! DO NOTHING!\n");
  return false;
}

#endif  // LISTDB_LSM_MEMTABLE_H_
