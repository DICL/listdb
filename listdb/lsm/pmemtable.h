#ifndef LISTDB_LSM_PMEMTABLE_H_
#define LISTDB_LSM_PMEMTABLE_H_

#include "listdb/index/braided_pmem_skiplist.h"
#include "listdb/lsm/table.h"
#include "listdb/index/bloom_filter.h"

#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pool.hpp>

class PmemTable : public Table {
 public:
  using Node = BraidedPmemSkipList::Node;

  PmemTable(const size_t table_capacity, BraidedPmemSkipList* skiplist);
  ~PmemTable();

  virtual void* Put(const Key& key, const Value& value) override;

  virtual bool Get(const Key& key, void** value_out) override;

  BraidedPmemSkipList* skiplist() { return skiplist_; }
  BloomFilter* bloom_filter() { return bloom_filter_; }
  uint64_t l0_compaction_cnt() {return l0_compaction_cnt_;}
  void increase_l0_compaction_cnt(){l0_compaction_cnt_++;}

  void SetManifest(pmem::obj::persistent_ptr<pmem_l0_info> manifest) { manifest_ = manifest; }
  void SetBloomFilter(BloomFilter* bloom_filter) { *bloom_filter_ = *bloom_filter; }

  template <typename T>
  pmem::obj::persistent_ptr<T> manifest() { return manifest_.raw(); }

 private:
  uint64_t l0_compaction_cnt_;
  BraidedPmemSkipList* skiplist_;
  BloomFilter* bloom_filter_;
  pmem::obj::persistent_ptr<pmem_l0_info> manifest_;
};

PmemTable::PmemTable(const size_t table_capacity, BraidedPmemSkipList* skiplist)
    : Table(table_capacity, TableType::kPmemTable), skiplist_(skiplist) {
      l0_compaction_cnt_ = 0;
      bloom_filter_ = new BloomFilter(10,table_capacity/sizeof(Key));
}

PmemTable::~PmemTable() {
  delete bloom_filter_;
}

void* PmemTable::Put(const Key& key, const Value& value) {
  fprintf(stdout, "Not impl!!!! returning NULL\n");
  return nullptr;
}

bool PmemTable::Get(const Key& key, void** value_out) {
  fprintf(stdout, "Not impl!!!! DO NOTHING!\n");
  return false;
}

#endif  // LISTDB_LSM_PMEMTABLE_H_
