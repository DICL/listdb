#ifndef LISTDB_LSM_PMEMTABLE2_LIST_H_
#define LISTDB_LSM_PMEMTABLE2_LIST_H_

#include "listdb/lsm/table_list.h"
#include "listdb/lsm/pmemtable2.h"

class PmemTable2List : public TableList {
 public:
  PmemTable2List(const size_t table_capacity, const int primary_region_pool_id);

  void BindArena(int pool_id, PmemLog* arena);

 protected:
  virtual Table* NewMutable(size_t table_capacity, Table* next_table) override;

  const int primary_region_pool_id_;
  std::map<int, PmemLog*> arena_;
};

PmemTable2List::PmemTable2List(const size_t table_capacity, const int primary_region_pool_id)
    : TableList(table_capacity), primary_region_pool_id_(primary_region_pool_id) { }

void PmemTable2List::BindArena(const int pool_id, PmemLog* arena) {
  arena_.emplace(pool_id, arena);
}

inline Table* PmemTable2List::NewMutable(size_t table_capacity, Table* next_table) {
  // Bind Arena
  auto skiplist = new PackedPmemSkipList(primary_region_pool_id_);
  for (auto& it : arena_) {
    skiplist->BindArena(it.first, it.second);
  }
  skiplist->Init();
  PmemTable2* new_table = new PmemTable2(table_capacity, skiplist);
  new_table->SetNext(next_table);
  return new_table;
}

#endif  // LISTDB_LSM_PMEMTABLE2_LIST_H_
