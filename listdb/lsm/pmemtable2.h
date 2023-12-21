#ifndef LISTDB_LSM_PMEMTABLE2_H_
#define LISTDB_LSM_PMEMTABLE2_H_

#include "listdb/index/packed_pmem_skiplist.h"
#include "listdb/lsm/table.h"
#include "listdb/core/pmem_db.h"
#include "listdb/index/bloom_filter.h"

#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pool.hpp>

class PmemTable2 : public Table {
 public:
  using Node = PackedPmemSkipList::Node;

  PmemTable2(const size_t table_capacity, PackedPmemSkipList* skiplist);
  ~PmemTable2();

  virtual void* Put(const Key& key, const Value& value) override;

  virtual bool Get(const Key& key, void** value_out) override;

  PackedPmemSkipList* skiplist() { return skiplist_; }

  BloomFilter* bloom_filter() { return bloom_filter_; }

  void SetManifest(pmem::obj::persistent_ptr<pmem_l2_info> manifest) { manifest_ = manifest; }

  template <typename T>
  pmem::obj::persistent_ptr<T> manifest() { return manifest_.raw(); }

 private:
  PackedPmemSkipList* skiplist_;
  BloomFilter* bloom_filter_;
  pmem::obj::persistent_ptr<pmem_l2_info> manifest_;
};

PmemTable2::PmemTable2(const size_t table_capacity, PackedPmemSkipList* skiplist)
    : Table(table_capacity, TableType::kPmemTable2), skiplist_(skiplist) {
    //there is no bloom filter for l2 (because there is no limit of l2 table capacity)
    //bloom_filter_ = new BloomFilter(10,table_capacity/sizeof(Key));
}


PmemTable2::~PmemTable2() {
  //delete bloom_filter_;
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
