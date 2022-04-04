#ifndef LISTDB_LSM_DRAM_LAYERED_PMEMTABLE_H_
#define LISTDB_LSM_DRAM_LAYERED_PMEMTABLE_H_

#include "listdb/index/braided_pmem_skiplist.h"
#include "listdb/index/lockfree_skiplist.h"
#include "listdb/lsm/table.h"

class DramLayeredPmemTable : public Table {
 public:
  using Node = BraidedPmemSkipList::Node;
  using DramNode = lockfree_skiplist::Node;

  DramLayeredPmemTable(const size_t table_capacity, BraidedPmemSkipList* skiplist);

 private:
  BraidedPmemSkipList* skiplist_;
  lockfree_skiplist* cache_;
};

#endif  // LISTDB_LSM_DRAM_LAYERED_PMEMTABLE_H_
