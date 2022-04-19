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
#include "listdb/lib/memory.h"

template <typename T>
using Pool = pmem::obj::pool<T>;

struct pmem_log;
struct pmem_log_block;

struct LogRecordId {
  uint32_t block_id;
  uint32_t offset;
};

struct LogRange {
  LogRecordId begin;
  LogRecordId end;
};

struct pmem_log_root {
  pmem::obj::persistent_ptr<pmem_log> shard[kNumShards];
};

struct pmem_log {
  uint32_t block_cnt;
  pmem::obj::persistent_ptr<pmem_log_block> head;
};

struct pmem_log_block {
  uint32_t id;
  size_t p;
  char data[kPmemLogBlockSize];
  pmem::obj::persistent_ptr<pmem_log_block> next;

  pmem_log_block(pmem::obj::persistent_ptr<pmem_log_block> next_ = nullptr) : p(0), data(), next(next_) { }
};

// PmemLog

class PmemLog {
 public:
  struct Block {
    std::atomic<size_t> p;  // current end
    char* data;  // pointer to pmem_log_block::data
    pmem::obj::persistent_ptr<pmem_log_block> p_block;

    explicit Block(pmem::obj::persistent_ptr<pmem_log_block> p_block_);
    void* Allocate(const size_t size);
  };
  
  PmemLog(const int pool_id, const int shard_id);

  ~PmemLog();

  PmemPtr Allocate(const size_t size);

  int pool_id() { return pool_id_; }

  pmem::obj::pool<pmem_log_root> pool() { return pool_; }

 private:
  Block* GetCurrentBlock();

  const int pool_id_;
  pmem::obj::pool<pmem_log_root> pool_;  // for memory allocation
  pmem::obj::persistent_ptr<pmem_log> p_log_;
  std::atomic<Block*> front_;
  std::atomic<size_t> hmm;
  std::mutex block_init_mu_;
};

PmemLog::Block::Block(pmem::obj::persistent_ptr<pmem_log_block> p_block_) {
  assert(p_block_ != nullptr);
  p.store(p_block_->p);
  data = p_block_->data;
  p_block = p_block_;
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
    // Create
    pmem::obj::persistent_ptr<pmem_log> p_log;
    pmem::obj::make_persistent_atomic<pmem_log>(pool_, p_log);
    p_log->block_cnt = 0;
    root->shard[shard_id] = p_log;
    p_log_ = p_log;
    front_.store(nullptr);
  } else {
    // Open
    p_log_ = root->shard[shard_id];
#if 0
    std::function<Block*(pmem::obj::persistent_ptr<pmem_log_block>)> init_fn =
        [&](pmem::obj::persistent_ptr<pmem_log_block> p_block) {
      return (p_block) ? new Block(p_block, init_fn(p_block->next)) : nullptr;
    };
    auto head_block = init_fn(p_log_->head);
#else
    auto head_block = new Block(p_log_->head);
#endif
    front_.store(head_block);
  }
}

PmemLog::~PmemLog() {
  auto block = GetCurrentBlock();
  block->p_block->p = block->p;
  clwb(&(block->p_block->p), sizeof(size_t));
}

PmemLog::Block* PmemLog::GetCurrentBlock() {
  Block* ret = front_.load(MO_RELAXED); 
  if (ret == nullptr) {
    std::lock_guard<std::mutex> lk(block_init_mu_);
    ret = front_.load(MO_RELAXED);
    if (ret == nullptr) {
      pmem::obj::persistent_ptr<pmem_log_block> p_new_block;
      pmem::obj::make_persistent_atomic<pmem_log_block>(pool_, p_new_block, p_log_->head);

      p_new_block->id = p_log_->block_cnt;
      p_log_->block_cnt++;

      p_log_->head = p_new_block;
      auto new_block = new Block(p_new_block);
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

      p_new_block->id = p_log_->block_cnt;
      p_log_->block_cnt++;

      p_log_->head = p_new_block;
      auto new_block = new Block(p_new_block);
      front_.store(new_block, MO_RELAXED);
      buf = new_block->Allocate(size);
    }
  }
  PmemPtr ret(pool_id_, (uint64_t) ((uintptr_t) buf - (uintptr_t) pool_.handle()));
  return ret;
}

#endif  // LISTDB_CORE_PMEM_LOG_H_
