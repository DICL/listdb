#ifndef LISTDB_LSM_PMEMTABLE_H_
#define LISTDB_LSM_PMEMTABLE_H_

#include "listdb/index/braided_pmem_skiplist.h"
#include "listdb/lsm/table.h"
#ifdef LISTDB_BLOOM_FILTER
#include "listdb/index/bloom_filter.h"
#endif

#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pool.hpp>

class PmemTable : public Table {
 public:
  using Node = BraidedPmemSkipList::Node;

  PmemTable(const size_t table_capacity, BraidedPmemSkipList* skiplist);
  PmemTable(const size_t table_capacity, BraidedPmemSkipList* skiplist, BloomFilter* bloom_filter);
  virtual ~PmemTable(){
#ifdef LISTDB_BLOOM_FILTER
    delete bloom_filter_;
#endif
  };

  virtual void* Put(const Key& key, const Value& value) override;

  virtual bool Get(const Key& key, void** value_out) override;

  BraidedPmemSkipList* skiplist() { return skiplist_; }
  #ifdef LISTDB_BLOOM_FILTER
  BloomFilter* bloom_filter() { return bloom_filter_; }
  #endif
  uint64_t l0_compaction_cnt() {return l0_compaction_cnt_;}
  void increase_l0_compaction_cnt(){l0_compaction_cnt_++;}

  void SetManifest(pmem::obj::persistent_ptr<pmem_l0_info> manifest) { manifest_ = manifest; }

  template <typename T>
  pmem::obj::persistent_ptr<T> manifest() { return manifest_.raw(); }

 private:
  uint64_t l0_compaction_cnt_;
  BraidedPmemSkipList* skiplist_;
#ifdef LISTDB_BLOOM_FILTER
  BloomFilter* bloom_filter_;
#endif
  pmem::obj::persistent_ptr<pmem_l0_info> manifest_;
};

PmemTable::PmemTable(const size_t table_capacity, BraidedPmemSkipList* skiplist)
    : Table(table_capacity, TableType::kPmemTable), skiplist_(skiplist) {
      l0_compaction_cnt_ = 0;
#ifdef LISTDB_BLOOM_FILTER
      bloom_filter_ = new BloomFilter(10,table_capacity/sizeof(Key));
#endif
}

#ifdef LISTDB_BLOOM_FILTER
PmemTable::PmemTable(const size_t table_capacity, BraidedPmemSkipList* skiplist, BloomFilter* bloom_filter)
    : Table(table_capacity, TableType::kPmemTable), skiplist_(skiplist), bloom_filter_(bloom_filter) {
      l0_compaction_cnt_ = 0;
}
#endif

void* PmemTable::Put(const Key& key, const Value& value) {
  fprintf(stdout, "Not impl!!!! returning NULL\n");
  return nullptr;
}

bool PmemTable::Get(const Key& key, void** value_out) {
  fprintf(stdout, "Not impl!!!! DO NOTHING!\n");
  return false;
}

#endif  // LISTDB_LSM_PMEMTABLE_H_
