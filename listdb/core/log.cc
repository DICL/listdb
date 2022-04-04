#include "log.h"

// LogBlock impl.

LogBlock::LogBlock(const int r) : r_(r) { }

void LogBlock::init(const int s) {
  TOID(LogBlockBase) base;
  POBJ_ZALLOC(lpop[s][r_], &base, LogBlockBase, sizeof(LogBlockBase) + (kLogBlockSize - 1));
  base_ = base;
  p_ = &(D_RW(base_)->p);
  data_ = D_RW(base_)->data;
}

void LogBlock::load(TOID(LogBlockBase) base) {
  base_ = base;
  p_ = &(D_RW(base_)->p);
  data_ = D_RW(base_)->data;
}

size_t LogBlock::fetch_add_size(const size_t size) {
  size_t before = std::atomic_fetch_add((std::atomic<uint64_t>*) p_, size);
  //pmemobj_persist(lpop[s][r_], p_, 8);
  return before;
}

TOID(LogBlockBase) LogBlock::base() {
  return base_;
}

char* LogBlock::data() {
  return data_;
}

// Log impl.

void Log::init(const int s) {
  for (int i = 0; i < kNumRegions; i++) {
    TOID(LogBase) base;
    POBJ_ZALLOC(lpop[s][i], &base, LogBase, sizeof(LogBase));
    log_base_[i] = base;

    TOID(LogBlockBase) nbb;  // new block base
    POBJ_ZALLOC(lpop[s][i], &nbb, LogBlockBase, sizeof(LogBlockBase) + (kLogBlockSize - 1));
    D_RW(log_base_[i])->head_block_base = nbb;
    D_RW(log_base_[i])->npp = nbb;
    pmemobj_persist(lpop[s][i], D_RW(log_base_[i]), sizeof(LogBlockBase));

    void* buf = malloc(sizeof(LogBlock));
    LogBlock* nb = new (buf) LogBlock(i);  // new block
    nb->load(nbb);
    blocks_[i] = nb;
  }
}

void Log::load(TOID(LogBase) log_base[]) {
  for (int i = 0; i < kNumRegions; i++) {
    log_base_[i] = log_base[i];
    TOID(LogBlockBase) bb = D_RW(log_base_[i])->npp; 
    while (true) {
      if (!TOID_IS_NULL(D_RW(bb)->next)) {
        bb = D_RW(bb)->next;
      } else {
        break;
      }
    }
    void* buf = malloc(sizeof(LogBlock));
    LogBlock* nb = new (buf) LogBlock(i);
    nb->load(bb);
    blocks_[i] = nb;
  }
}

uint64_t Log::write_WAL(ThreadData* td, const int s, const std::string_view& key, const uint64_t value) {
  const int r = td->region;
  // 8-byte for seq_num, value type and etc., which are not implemented yet.
#ifndef BR_STRING_KV
  const size_t my_size = 3 * sizeof(uint64_t);  // key, tag, value
#else
  const size_t my_size = sizeof(uint32_t) + key.size() + sizeof(uint64_t) + sizeof(uint64_t);  // key_len, key, tag, value
#endif
  LogBlock* b = blocks_[r];
  size_t before;
  while (true) {
    if (b == NULL) {
      b = blocks_[r];
      continue;
    }
    before = b->fetch_add_size(my_size);
    if (my_size + before > kLogBlockSize) {
      std::unique_lock<std::mutex> lk(mu_);
      if (b == blocks_[r]) {
        TOID(LogBlockBase) nbb;
        POBJ_ZALLOC(lpop[s][r], &nbb, LogBlockBase, sizeof(LogBlockBase) + (kLogBlockSize - 1));
        LogBlockBase* nbbp = D_RW(nbb);
        nbbp->p = 0;
        nbbp->npp = 0;
        D_RW(b->base())->next = nbb;
        pmemobj_persist(lpop[s][r], &(D_RW(b->base())->next), sizeof(TOID(LogBlockBase)));
        void* buf = malloc(sizeof(LogBlock));
        LogBlock* nb = new (buf) LogBlock(r);
        nb->load(nbb);
        //std::atomic_store(&blocks_[r], nb);
        blocks_[r] = nb;
        b = blocks_[r];
        //b->fetch_add_size(my_size);
      } else {
        b = blocks_[r];
      }
      lk.unlock();
      continue;
    }
    break;
  }
  // Write log entry
  uint64_t dummy_tag = 0x1;
  void* begin = b->data() + before;
  char* p = (char*) begin;
#ifndef BR_STRING_KV
  *((uint64_t*) p) = *((uint64_t*) key.data());
  p += 8;
#else
  *((uint32_t*) p) = key.length();
  p += 4;
  memcpy(p, key.data(), key.length());
  p += key.length();
#endif
  *((uint64_t*) p) = dummy_tag;
  p += 8;
  *((uint64_t*) p) = value;
  pmemobj_persist(lpop[s][r], begin, my_size);
  uint64_t log_moff = (((uintptr_t) begin - (uintptr_t) lpop[s][r]) << 16) | r;
  return log_moff;
}

uint64_t Log::write_IUL(ThreadData* td, const int s, const std::string_view& key, const uint64_t value) {
  const int r = td->region;
  // Random Height
  static const unsigned int kBranching = 4;
  const int bb = (kBranching > (unsigned int) kNumRegions) ? kBranching / kNumRegions : 1;
  int height = 1;
  if (height < kMaxHeight && ((td->rnd.Next() % bb) == 0)) {
    height++;
    while (height < kMaxHeight && ((td->rnd.Next() % kBranching) == 0)) {
      height++;
    }
  }
  // Compute Allocation Size
#ifndef BR_STRING_KV
  const size_t my_size = sizeof(UINT64_pnode) + (height - 1) * 8;
#else
  const size_t my_size = VARSTR_pnode::compute_alloc_size(key, height);
#endif
  LogBlock* b = blocks_[r];
  size_t before;
  while (true) {
    if (b == NULL) {
      b = blocks_[r];
      continue;
    }
    before = b->fetch_add_size(my_size);
    if (my_size + before > kLogBlockSize) {
      std::unique_lock<std::mutex> lk(mu_);
      if (b == blocks_[r]) {
        TOID(LogBlockBase) nbb;
        POBJ_ZALLOC(lpop[s][r], &nbb, LogBlockBase, sizeof(LogBlockBase) + (kLogBlockSize - 1));
        LogBlockBase* nbbp = D_RW(nbb);
        nbbp->p = 0;
        nbbp->npp = 0;
        D_RW(b->base())->next = nbb;
        pmemobj_persist(lpop[s][r], &(D_RW(b->base())->next), 16);
        void* buf = malloc(sizeof(LogBlock));
        LogBlock* nb = new (buf) LogBlock(r);
        nb->load(nbb);
        //std::atomic_store(&(blocks_[r]), nb);
        blocks_[r] = nb;
        b = blocks_[r];
      } else {
        b = blocks_[r];
      }
      lk.unlock();
      continue;
    }
    break;
  }
  // Write log entry
  uint64_t dummy_tag = kTypeValue;
  void* begin = b->data() + before;
  char* p = (char*) begin;
#ifndef BR_STRING_KV
  *((uint64_t*) p) = *((uint64_t*) key.data());
  p += 8;
  *((uint64_t*) p) = dummy_tag;
  p += 8;
  *((uint64_t*) p) = value;
  p += 8;
  *((int*) p) = height;
#else
  VARSTR_pnode::init_node(p, key, dummy_tag, value, height, false);
#endif 
  pmemobj_persist(lpop[s][r], begin, my_size - (height * 8));
  uint64_t log_moff = (((uintptr_t) begin - (uintptr_t) lpop[s][r]) << 16) | r;
  return log_moff;
}

