#ifndef LISTDB_INDEX_SIMPLE_HASH_TABLE_H_
#define LISTDB_INDEX_SIMPLE_HASH_TABLE_H_

#include "listdb/common.h"
#include "listdb/lib/hash.h"

class SimpleHashTable {
 public:
  struct Bucket {
    uint64_t version;
    uint64_t key;
    uint64_t value;
  };

  SimpleHashTable(size_t size);

  Bucket* at(const int i) { return &(buckets_[i]); }

  void Add(const Key& key, const Value& value);

  bool Get(const Key& key, Value* value_out);

 private:
  const size_t size_;
  Bucket* buckets_;
};

SimpleHashTable::SimpleHashTable(size_t size) : size_(size) {
  buckets_ = new Bucket[size];
}

void SimpleHashTable::Add(const Key& key, const Value& value) {
  // key: integer key
  uint32_t idx = ht_murmur3(key);
  uint32_t idx2 = ht_sha1(key);
  auto& buckt = buckets_[idx];
  auto& buckt2 = buckets_[idx2];
  uint64_t prev_ver = std::atomic_load((std::atomic<uint64_t>*) &buckt.version);
  uint64_t prev_ver2 = std::atomic_load((std::atomic<uint64_t>*) &buckt2.version);
  if (prev_ver > prev_ver2) {
    while (!std::atomic_compare_exchange_weak((std::atomic<uint64_t>*) &buckt2.version, &prev_ver2, 0UL)) continue;
#ifndef LISTDB_STRING_KEY
    buckt2.key = key;
#else
    buckt2.key = (uint64_t) key.data();
#endif
    buckt2.value = value;
    std::atomic_store((std::atomic<uint64_t>*) &buckt2.version, prev_ver2 + 1);
  } else {
    while (!std::atomic_compare_exchange_weak((std::atomic<uint64_t>*) &buckt.version, &prev_ver, 0UL)) continue;
#ifndef LISTDB_STRING_KEY
    buckt.key = key;
#else
    buckt.key = (uint64_t) key.data();
#endif
    buckt.value = value;
    std::atomic_store((std::atomic<uint64_t>*) &buckt.version, prev_ver + 1);
  }
}

bool SimpleHashTable::Get(const Key& key, Value* value_out) {
  uint64_t k;
  uint64_t v;
  uint64_t k2;
  uint64_t v2;
  uint32_t idx = ht_murmur3(key);
  uint32_t idx2 = ht_sha1(key);
  auto& buckt = buckets_[idx];
  auto& buckt2 = buckets_[idx2];
  uint64_t prev_ver;
  uint64_t prev_ver2;
  while (true) {
    //prev_ver = std::atomic_load_explicit((std::atomic<uint64_t>*) &buckt.version, MO_RELAXED);
    prev_ver = buckt.version;
    if (prev_ver == 0) continue;
    k = buckt.key;
    v = buckt.value;
    //auto curr_ver = std::atomic_load_explicit((std::atomic<uint64_t>*) &buckt.version, MO_RELAXED);
    //if (prev_ver != curr_ver) continue;
    if (prev_ver != buckt.version) continue;
    break;
  }
  if (buckt.version > 1) {
#ifndef LISTDB_STRING_KEY
    bool key_cmp = (k == key);
#else
    bool key_cmp = (strncmp((char*) k, key.data(), kStringKeyLength) == 0);
#endif
    if (key_cmp) {
      if (value_out) {
        *value_out = v;
      }
      return true;
    }
  }
  while (true) {
    //prev_ver2 = std::atomic_load_explicit((std::atomic<uint64_t>*) &buckt2.version, MO_RELAXED);
    prev_ver2 = buckt2.version;
    if (prev_ver2 == 0) continue;
    k2 = buckt2.key;
    v2 = buckt2.value;
    //auto curr_ver = std::atomic_load_explicit((std::atomic<uint64_t>*) &buckt2.version, MO_RELAXED);
    //if (prev_ver2 != curr_ver) continue;
    if (prev_ver2 != buckt2.version) continue;
    break;
  }
  if (buckt2.version > 1) {
#ifndef LISTDB_STRING_KEY
    bool key_cmp = (k2 == key);
#else
    bool key_cmp = (strncmp((char*) k2, key.data(), kStringKeyLength) == 0);
#endif
    if (key_cmp) {
      if (value_out) {
        *value_out = v2;
      }
      return true;
    }
  }
  return false;
}

#endif  // LISTDB_INDEX_SIMPLE_HASH_TABLE_H_
