#pragma once

#include "listdb/common.h"
#include "listdb/index/braided_pmem_skiplist.h"
#include "listdb/lib/murmur3.h"
#include "listdb/lib/sha1.h"

#define DOUBLE_HASHING_T_A 1
#define DOUBLE_HASHING_T_B 2
#ifndef LISTDB_DOUBLE_HASHING
#define LISTDB_DOUBLE_HASHING DOUBLE_HASHING_T_B
#endif

class DoubleHashingCache {
 public:
  using PmemNode = BraidedPmemSkipList::Node;

  struct Bucket {
    std::atomic<PmemNode*> value;

    Bucket() : value(nullptr) { }
  };

  DoubleHashingCache(size_t size, int shard);

  Bucket* at(const int i) { return &(buckets_[i]); }

  void Insert(const Key& key, PmemNode* const p);

  PmemNode* Lookup(const Key& key);

  uint32_t Hash1(const Key& key);

  uint32_t Hash2(const Key& key);

 private:
  const static int probing_distance_ = LISTDB_L0_CACHE_PROBING_DISTANCE;

  const size_t size_;
  const int shard_;
  const uint32_t seed_;
  Bucket* buckets_;
};

DoubleHashingCache::DoubleHashingCache(size_t size, int shard)
  : size_(size), shard_(shard), seed_(std::hash<int>()(shard_)) {
  buckets_ = new Bucket[size];
  std::atomic_thread_fence(std::memory_order_seq_cst);
}

void DoubleHashingCache::Insert(const Key& key, PmemNode* const p) {
#if LISTDB_DOUBLE_HASHING == DOUBLE_HASHING_T_A
  uint32_t h = Hash1(key);
  uint32_t pos = h % size_;
  PmemNode* old_value = buckets_[pos].value.load(std::memory_order_seq_cst);
  if (old_value != nullptr) {
    uint32_t pos2 = (h + Hash2(key)) % size_;
    buckets_[pos2].value.store(old_value, std::memory_order_seq_cst);
  }
  buckets_[pos].value.store(p, std::memory_order_seq_cst);
#elif LISTDB_DOUBLE_HASHING == DOUBLE_HASHING_T_B
  uint32_t h = Hash1(key);
  uint32_t pos = h % size_;
  PmemNode* expected = nullptr;
  if (buckets_[pos].value.compare_exchange_strong(expected, p)) {
    return;
  }
  uint32_t h2 = Hash2(key);
  unsigned int cnt = 1;
  pos = (h + cnt * h2) % size_;
  while (cnt < probing_distance_) {
    expected = nullptr;
    if (buckets_[pos].value.compare_exchange_strong(expected, p)) {
      return;
    } else {
      cnt++;
      pos = (h + cnt * h2) % size_;
    }
  }
  buckets_[pos].value.store(p, std::memory_order_seq_cst);
#else
  fprintf(stderr, "DEFINE LISTDB_DOUBLE_HASHING <type>\n");
  abort();
#endif
}

DoubleHashingCache::PmemNode* DoubleHashingCache::Lookup(const Key& key) {
#if LISTDB_DOUBLE_HASHING == DOUBLE_HASHING_T_A
  uint32_t h = Hash1(key);
  uint32_t pos = h % size_;
  PmemNode* value = buckets_[pos].value.load(std::memory_order_seq_cst);
  if (value && value->key.Compare(key) == 0) {
    return value;
  } else {
    uint32_t pos2 = (h + Hash2(key)) % size_;
    PmemNode* value = buckets_[pos2].value.load(std::memory_order_seq_cst);
    if (value && value->key.Compare(key) == 0) {
      return value;
    }
  }
  return nullptr;
#elif LISTDB_DOUBLE_HASHING == DOUBLE_HASHING_T_B
  uint32_t h = Hash1(key);
  uint32_t pos = h % size_;
  PmemNode* value = buckets_[pos].value.load(std::memory_order_seq_cst);
  if (value && value->key.Compare(key) == 0) {
    return value;
  } else {
    uint32_t h2 = Hash2(key);
    unsigned int cnt = 1;
    while (cnt <= probing_distance_) {
      pos = (h + cnt * h2) % size_;
      value = buckets_[pos].value.load(std::memory_order_seq_cst);
      if (value && value->key.Compare(key) == 0) {
        return value;
      } else {
        cnt++;
        continue;
      }
    }
  }
  return nullptr;
#else
  fprintf(stderr, "DEFINE LISTDB_DOUBLE_HASHING <type>\n");
  abort();
#endif
}

inline uint32_t DoubleHashingCache::Hash1(const Key& key) {
	uint32_t h;
	//static const uint32_t seed = 0xcafeb0ba;
#ifndef LISTDB_STRING_KEY
	MurmurHash3_x86_32(&key, sizeof(uint64_t), seed_, (void*) &h);
#else
	MurmurHash3_x86_32(key.data(), kStringKeyLength, seed_, (void*) &h);
#endif
	return h;
}

inline uint32_t DoubleHashingCache::Hash2(const Key& key) {
	char result[21];  // 5 * 32bit
#ifndef LISTDB_STRING_KEY
	SHA1(result, (char*) &key, 8);
#else
	SHA1(result, key.data(), kStringKeyLength);
#endif
	return *reinterpret_cast<uint32_t*>(result);
}
