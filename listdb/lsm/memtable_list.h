#ifndef LISTDB_LSM_MEMTABLE_LIST_H_
#define LISTDB_LSM_MEMTABLE_LIST_H_

#include <condition_variable>
#include <functional>

#include "listdb/lsm/table_list.h"
#include "listdb/lsm/memtable.h"

class MemTableList : public TableList {
 public:
  MemTableList(const size_t table_capacity, const int shard_id);

  void BindEnqueueFunction(std::function<void(MemTable*)> enqueue_fn);

  void BindArena(int region, PmemLog* arena);

  void CleanUpFlushedImmutables();

  void CreateNewFront();

 protected:
  virtual Table* NewMutable(size_t table_capacity, Table* next_table) override;

  virtual void EnqueueCompaction(Table* table) override;

  const int shard_id_;
  const int max_num_memtables_ = kMaxNumMemTables;
  int num_memtables_ = 0;
  std::function<void(MemTable*)> enqueue_fn_;

  PmemLog* arena_[kNumRegions];

  std::mutex mu_;
  std::condition_variable cv_;
};

MemTableList::MemTableList(const size_t table_capacity, const int shard_id)
    : TableList(table_capacity), shard_id_(shard_id) { }

void MemTableList::BindEnqueueFunction(std::function<void(MemTable*)> enqueue_fn) {
  enqueue_fn_ = enqueue_fn;
}

void MemTableList::BindArena(int region, PmemLog* arena) {
  arena_[region] = arena;
}

inline Table* MemTableList::NewMutable(size_t table_capacity, Table* next_table) {
  std::unique_lock<std::mutex> lk(mu_);
  cv_.wait(lk, [&]{
    if (num_memtables_ < max_num_memtables_) {
      return true;
    } else {
      fprintf(stdout, "Stall\n");
      return false;
    }
  });
  num_memtables_++;
  lk.unlock();
  MemTable* new_table = new MemTable(table_capacity);  // TODO(wkim): a table must have an ID

  // Set the next table as full
  if (next_table) {
    auto next_memtable = (MemTable*) next_table;
    auto next_l0_manifest = next_memtable->l0_manifest();
    next_l0_manifest->status = Level0Status::kFull;
    // call clwb
  }

  // Init the new manifest for a new table
  pmem::obj::persistent_ptr<pmem_l0_info> l0_manifest;
  auto db_pool = Pmem::pool<pmem_db>(0);
  pmem::obj::make_persistent_atomic<pmem_l0_info>(db_pool, l0_manifest);
  auto db_root = db_pool.root();
  auto shard_manifest = db_root->shard[shard_id_];
  l0_manifest->id = shard_manifest->l0_cnt++;
  l0_manifest->status = Level0Status::kInitialized;
  BraidedPmemSkipList* l0_skiplist = new BraidedPmemSkipList(arena_[0]->pool_id());
  for (int i = 0; i < kNumRegions; i++) {
    l0_skiplist->BindArena(arena_[i]->pool_id(), arena_[i]);
  }
  l0_skiplist->Init();
  for (int i = 0; i < kNumRegions; i++) {
    int pool_id = arena_[i]->pool_id();
    l0_manifest->head[i] = l0_skiplist->p_head(pool_id);
  }
  l0_manifest->next = shard_manifest->l0_list_head->next;
  shard_manifest->l0_list_head->next = l0_manifest;
  new_table->SetL0SkipList(l0_skiplist);
  new_table->SetL0Manifest(l0_manifest);
  new_table->SetNext(next_table);
  return new_table;
}

inline void MemTableList::EnqueueCompaction(Table* table) {
  //fprintf(stdout, "Enqueue\n");
  enqueue_fn_((MemTable*) table);
}

void MemTableList::CleanUpFlushedImmutables() {
#if LISTDB_FLUSH_MEMTABLE_TO_L1 == 1
  std::unique_lock<std::mutex> lk(mu_);
  num_memtables_--;
  cv_.notify_one();
#else
  auto curr = GetFront();
  std::vector<Table*> tables;
  while (curr) {
    tables.push_back(curr);
    curr = curr->Next();
  }
  int flushed_cnt = 0;

  //std::vector<Table*> pmemtables;
  MemTable* pred = nullptr;
  for (int i = tables.size() - 1; i >= 1; i--) {
    if (tables[i]->type() == TableType::kMemTable) {
      MemTable* imm = (MemTable*) tables[i];
      if (imm->IsFlushed()) {
        //pmemtables.push_back((Table*) imm->PersistentTable());
        auto pmem = (Table*) imm->PersistentTable();
        if (imm->Next()) {
          if (imm->Next()->type() == TableType::kPmemTable) {
            pmem->SetNext(imm->Next());
          }
        }
        pred = (MemTable*) tables[i - 1];
        pred->SetNext(pmem);

        flushed_cnt++;
        // TODO(wkim): free imm
      } else {
        break;
      }
    }
  }

  //for (int i = 0; i < pmemtables.size() - 1; i++) {
  //  pmemtables[i + 1]->SetNext(pmemtables[i]);
  //}
  //if (pred) {
  //  pred->SetNext(pmemtables.back());
  //}

  std::unique_lock<std::mutex> lk(mu_);
  num_memtables_ -= flushed_cnt;
  lk.unlock();
  cv_.notify_one();
#endif
}

void MemTableList::CreateNewFront() {
  std::unique_lock<std::mutex> lk(init_mu_);
  auto table = front_.load(MO_RELAXED);
  auto new_table = NewMutable(table_capacity_, table);
  front_.store(new_table, MO_RELAXED);
  lk.unlock();
}

#endif  // LISTDB_LSM_MEMTABLE_LIST_H_
