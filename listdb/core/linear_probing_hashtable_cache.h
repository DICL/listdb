#pragma once

#include "listdb/common.h"
#include "listdb/index/braided_pmem_skiplist.h"
#include "listdb/lib/murmur3.h"

#define LP_HASH_T_A 1
#ifndef LISTDB_LINEAR_PROBING_HASHTABLE_CACHE
#define LISTDB_LINEAR_PROBING_HASHTABLE_CACHE LP_HASH_T_A
#endif

class LinearProbingHashTableCache {
 public:
  using PmemNode = BraidedPmemSkipList::Node;

  struct Bucket {
    std::atomic<PmemNode*> value;

    Bucket() : value(nullptr) { }
  };

  LinearProbingHashTableCache(size_t size, int shard);

  Bucket* at(const int i) { return &(buckets_[i]); }

  void Insert(const Key& key, PmemNode* const p);

  PmemNode* Lookup(const Key& key);

  uint32_t Hash1(const Key& key);

 private:
  const static int probing_distance_ = LISTDB_L0_CACHE_PROBING_DISTANCE;

  const size_t size_;
  const int shard_;
  const uint32_t seed_;
  Bucket* buckets_;
};

LinearProbingHashTableCache::LinearProbingHashTableCache(size_t size, int shard)
  : size_(size), shard_(shard), seed_(std::hash<int>()(shard_)) {
  buckets_ = new Bucket[size];
  std::atomic_thread_fence(std::memory_order_seq_cst);
}

void LinearProbingHashTableCache::Insert(const Key& key, PmemNode* const p) {
#if LISTDB_LINEAR_PROBING_HASHTABLE_CACHE == LP_HASH_T_A
  uint32_t h = Hash1(key);
  uint32_t pos = h % size_;
  unsigned int cnt = 0;
  while (cnt < probing_distance_) {
    PmemNode* expected = nullptr;
    if (buckets_[pos].value.compare_exchange_strong(expected, p)) {
      return;
    } else {
      cnt++;
      pos = (h + cnt) % size_;
      continue;
    }
  }
  buckets_[pos].value.store(p, std::memory_order_seq_cst);
#else
  fprintf(stderr, "DEFINE LISTDB_LINEAR_PROBING_HASHTABLE_CACHE <type>\n");
  abort();
#endif
}

LinearProbingHashTableCache::PmemNode* LinearProbingHashTableCache::Lookup(const Key& key) {
#if LISTDB_LINEAR_PROBING_HASHTABLE_CACHE == LP_HASH_T_A
  uint32_t h = Hash1(key);
  uint32_t pos = h % size_;
  unsigned int cnt = 0;
  while (cnt <= probing_distance_) {
    PmemNode* value = buckets_[pos].value.load(std::memory_order_seq_cst);
    if (value && value->key.Compare(key) == 0) {
      return value;
    } else {
      cnt++;
      pos = (h + cnt) % size_;
      continue;
    }
  }
  return nullptr;
#else
  fprintf(stderr, "DEFINE LISTDB_LINEAR_PROBING_HASHTABLE_CACHE <type>\n");
  abort();
#endif
}

inline uint32_t LinearProbingHashTableCache::Hash1(const Key& key) {
	uint32_t h;
	//static const uint32_t seed = 0xcafeb0ba;
#ifndef LISTDB_STRING_KEY
	MurmurHash3_x86_32(&key, sizeof(uint64_t), seed_, (void*) &h);
#else
	MurmurHash3_x86_32(key.data(), kStringKeyLength, seed_, (void*) &h);
#endif
	return h;
}
