#ifndef LISTDB_CORE_PMEM_LOG_H_
#define LISTDB_CORE_PMEM_LOG_H_

#include <functional>
#include <mutex>

#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>

#include "listdb/common.h"
#include "listdb/pmem/pmem.h"
#include "listdb/pmem/pmem_ptr.h"

template <typename T>
using Pool = pmem::obj::pool<T>;

struct pmem_log_block {
  size_t p;
  char data[kPmemLogBlockSize];
  pmem::obj::persistent_ptr<pmem_log_block> next;
  size_t wal_end;

  pmem_log_block(pmem::obj::persistent_ptr<pmem_log_block> next_ = nullptr) : p(0), data(), next(next_), wal_end(0) { }
};

struct pmem_log {
  pmem::obj::persistent_ptr<pmem_log_block> head;
};

struct pmem_log_root {
  pmem::obj::persistent_ptr<pmem_log> shard[kNumShards];
};

// PmemLog

class PmemLog {
 public:
  struct Block {
    std::atomic<size_t> p;  // current end
    char* data;  // pointer to pmem_log_block::data
    size_t wal_end;
    Block* next;

    explicit Block(pmem::obj::persistent_ptr<pmem_log_block> p_block, Block* next_);
    void* Allocate(const size_t size);
  };
  
  PmemLog(const int pool_id, const int shard_id);

  PmemPtr Allocate(const size_t size);

  int pool_id() { return pool_id_; }

 private:
  Block* GetCurrentBlock();

  const int pool_id_;
  pmem::obj::pool<pmem_log_root> pool_;  // for memory allocation
  pmem::obj::persistent_ptr<pmem_log> p_log_;
  std::atomic<Block*> front_;
  std::atomic<size_t> hmm;
  std::mutex block_init_mu_;
};

PmemLog::Block::Block(pmem::obj::persistent_ptr<pmem_log_block> p_block, Block* next_) {
  assert(p_block != nullptr);
  p.store(p_block->p);
  data = p_block->data;
  wal_end = p_block->wal_end;
  next = next_;
  std::atomic_thread_fence(std::memory_order_release);
}

void* PmemLog::Block::Allocate(const size_t size) {
  size_t before = p.fetch_add(size, MO_RELAXED);
  if (before + size <= kPmemLogBlockSize) {
    return (void*) (data + before);
  } else {
    return nullptr;
  }
}

PmemLog::PmemLog(const int pool_id, const int shard_id) : pool_id_(pool_id) {
  auto pool = Pmem::pool<pmem_log_root>(pool_id_);
  pool_ = pool;
  auto root = pool_.root();
  if (root->shard[shard_id] == nullptr) {
    pmem::obj::make_persistent_atomic<pmem_log>(pool_, root->shard[shard_id]);
    p_log_ = root->shard[shard_id];
    front_.store(nullptr);
  } else {
    p_log_ = root->shard[shard_id];
    std::function<Block*(pmem::obj::persistent_ptr<pmem_log_block>)> init_fn =
        [&](pmem::obj::persistent_ptr<pmem_log_block> p_block) {
      return (p_block) ? new Block(p_block, init_fn(p_block->next)) : nullptr;
    };
    auto head_block = init_fn(p_log_->head);
    front_.store(head_block);
  }
}

PmemLog::Block* PmemLog::GetCurrentBlock() {
  Block* ret = front_.load(MO_RELAXED); 
  if (ret == nullptr) {
    std::lock_guard<std::mutex> lk(block_init_mu_);
    ret = front_.load(MO_RELAXED);
    if (ret == nullptr) {
      pmem::obj::persistent_ptr<pmem_log_block> p_new_block;
      pmem::obj::make_persistent_atomic<pmem_log_block>(pool_, p_new_block, p_log_->head);
      p_log_->head = p_new_block;
      auto new_block = new Block(p_new_block, nullptr);
      front_.store(new_block, MO_RELAXED);
      ret = new_block;
    }
  }
  return ret;
}

PmemPtr PmemLog::Allocate(const size_t size) {
  Block* block = GetCurrentBlock();
  void* buf = nullptr;
  if ((buf = block->Allocate(size)) == nullptr) {
    std::lock_guard<std::mutex> lk(block_init_mu_);
    block = front_.load(MO_RELAXED);
    if ((buf = block->Allocate(size)) == nullptr) {
      // TODO: write Block contents to pmem_log_block
      pmem::obj::persistent_ptr<pmem_log_block> p_new_block;
      pmem::obj::make_persistent_atomic<pmem_log_block>(pool_, p_new_block, p_log_->head);
      p_log_->head = p_new_block;
      auto new_block = new Block(p_new_block, block);
      front_.store(new_block, MO_RELAXED);
      buf = new_block->Allocate(size);
    }
  }
  PmemPtr ret(pool_id_, (uint64_t) ((uintptr_t) buf - (uintptr_t) pool_.handle()));
  return ret;
}

#endif  // LISTDB_CORE_PMEM_LOG_H_
