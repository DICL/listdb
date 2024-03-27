#ifndef LISTDB_LSM_PMEMTABLE2_H_
#define LISTDB_LSM_PMEMTABLE2_H_

#include "listdb/index/packed_pmem_skiplist.h"
#include "listdb/lsm/table.h"
#include "listdb/core/pmem_db.h"
#ifdef LISTDB_BLOOM_FILTER
#include "listdb/index/bloom_filter.h"
#endif

#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pool.hpp>

class PmemTable2 : public Table {
 public:
  using Node = PackedPmemSkipList::Node;

  PmemTable2(const size_t table_capacity, PackedPmemSkipList* skiplist);

  virtual void* Put(const Key& key, const Value& value) override;

  virtual bool Get(const Key& key, void** value_out) override;

  PackedPmemSkipList* skiplist() { return skiplist_; }
#ifdef LISTDB_BLOOM_FILTER
  BloomFilter* bloom_filter() { return bloom_filter_; }
#endif
  void SetManifest(pmem::obj::persistent_ptr<pmem_l1_info> manifest) { manifest_ = manifest; }

  template <typename T>
  pmem::obj::persistent_ptr<T> manifest() { return manifest_.raw(); }

 private:
  PackedPmemSkipList* skiplist_;
#ifdef LISTDB_BLOOM_FILTER
  BloomFilter* bloom_filter_;
#endif
  pmem::obj::persistent_ptr<pmem_l1_info> manifest_;
};

PmemTable2::PmemTable2(const size_t table_capacity, PackedPmemSkipList* skiplist)
    : Table(table_capacity, TableType::kPmemTable2), skiplist_(skiplist) {
    //there is no bloom filter for l1 (because there is no limit of l1 table capacity)
    //bloom_filter_ = new BloomFilter(10,table_capacity/sizeof(Key));
}



void* PmemTable2::Put(const Key& key, const Value& value) {
  fprintf(stdout, "Not impl!!!! returning NULL\n");
  return nullptr;
}

bool PmemTable2::Get(const Key& key, void** value_out) {
  fprintf(stdout, "Not impl!!!! DO NOTHING!\n");
  return false;
}

#endif  // LISTDB_LSM_PMEMTABLE2_H_
