#ifndef LISTDB_CORE_STATIC_HASHTABLE_CACHE_H_
#define LISTDB_CORE_STATIC_HASHTABLE_CACHE_H_

#include "listdb/common.h"
#include "listdb/index/braided_pmem_skiplist.h"
#include "listdb/lib/murmur3.h"

class StaticHashTableCache {
 public:
  using PmemNode = BraidedPmemSkipList::Node;

  struct Bucket {
    std::atomic<PmemNode*> value;

    Bucket() : value(nullptr) { }
  };

  StaticHashTableCache(size_t size, int shard);

  Bucket* at(const int i) { return &(buckets_[i]); }

  void Insert(const Key& key, PmemNode* const p);

  PmemNode* Lookup(const Key& key);

  uint32_t Hash(const Key& key);

 private:
  const size_t size_;
  const int shard_;
  const uint32_t seed_;
  Bucket* buckets_;
};

StaticHashTableCache::StaticHashTableCache(size_t size, int shard)
  : size_(size), shard_(shard), seed_(std::hash<int>()(shard_)) {
  buckets_ = new Bucket[size];
  std::atomic_thread_fence(std::memory_order_seq_cst);
}

void StaticHashTableCache::Insert(const Key& key, PmemNode* const p) {
  uint32_t pos = Hash(key);
  buckets_[pos].value.store(p, std::memory_order_seq_cst);
}

StaticHashTableCache::PmemNode* StaticHashTableCache::Lookup(const Key& key) {
  uint32_t pos = Hash(key);
  PmemNode* value = buckets_[pos].value.load(std::memory_order_seq_cst);
  if (value && value->key.Compare(key) == 0) {
    return value;
  }
  return nullptr;
}

inline uint32_t StaticHashTableCache::Hash(const Key& key) {
	uint32_t h;
	//static const uint32_t seed = 0xcafeb0ba;
#ifndef LISTDB_STRING_KEY
	MurmurHash3_x86_32(&key, sizeof(uint64_t), seed_, (void*) &h);
#else
	MurmurHash3_x86_32(key.data(), kStringKeyLength, seed_, (void*) &h);
#endif
	return h % size_;
}

#endif  // LISTDB_CORE_STATIC_HASHTABLE_CACHE_H_
