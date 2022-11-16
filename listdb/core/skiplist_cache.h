#ifndef LISTDB_CORE_SKIPLIST_CACHE_H_
#define LISTDB_CORE_SKIPLIST_CACHE_H_

#include <algorithm>
#include <sstream>

#include "listdb/common.h"
#include "listdb/index/braided_pmem_skiplist.h"
#include "listdb/util.h"
#include "listdb/util/random.h"

// TODO(wkim): Undefine this after doing the relevant works. Refer `SkipListCache::size_`
#define CACHE_SIZE_IS_FIELD_COUNT

template <std::size_t N>
class SkipListCache {
 public:
  using PmemNode = BraidedPmemSkipList::Node2;

  struct Field {
    Key key;
    std::atomic<uint64_t> value;  // {L0 node height, pmem address offset}

    Field() : key(0), value(0) { }
    Field(const Key& k, uint64_t v) : key(k), value(v) { }
    //Field(const Key& k, int h, uint64_t o) : key(k), value((static_cast<uint64_t>(h) << 48) | o) { }

    void Set(const Key& k, int height, uint64_t offset) {
      key = k;
      //value = (static_cast<uint64_t>(h) << 48) | offset;
      value.store((static_cast<uint64_t>(height) << 48) | offset, std::memory_order_release);
    }

    void Reset() {
      value.store(0, std::memory_order_release);
    }

    bool IsEmpty() const {
      return value.load(std::memory_order_acquire) == 0;
    }

    void operator=(const Field& other) {
      key = other.key;
      value.store(other.value.load(std::memory_order_relaxed), std::memory_order_release);
    }

    uint16_t height() const { return (value >> 48); }
    uint64_t offset() const {
      static const uint64_t kMask = 0x0000ffffffffffff;
      return (value & kMask);
    }
  };


  // Constructor
  SkipListCache(const int pool_id, size_t capacity = kSkipListCacheCapacity);

  // Not thread-safe.
  // Only one background worker thread calls this.
  int Insert(PmemNode* const p);

  // Returns 0 if equal, -1 lessthan, 1 not found
  int LookupLessThanOrEqualsTo(const Key& key, PmemNode** out);

  size_t AcquireLoadSize() { return size_.load(std::memory_order_acquire); }

 private:
  PmemNode* DecodeFieldValue(const Field& f) {
    return PmemPtr::Compose<PmemNode>(pool_id_, f.offset());
  }

  int EvictSome();

  const int pool_id_;
  const size_t capacity_;
  
  // NOTE: size_ and capacity_ must represent used and free memory space respectively.
  // Since this requires a Merge operation, which is not implemented yet, for now, size_ represents the number of fields
  // occupied regardless of the actual memory consumption.
  // TODO(wkim): Impl. Merge and change the semantic of size and capacity.

  
  std::atomic<size_t> size_;
  uint64_t MaxFieldNum;
  Field* fields;
  Key victim_key_;
  //std::mutex mu_;
};



template <std::size_t N>
SkipListCache<N>::SkipListCache(const int pool_id, size_t capacity)
  : pool_id_(pool_id),
    capacity_(capacity),
    size_(0),
    victim_key_(0) {
  MaxFieldNum = (uint64_t)(capacity/sizeof(Field));
    fields = new Field[MaxFieldNum];
  std::atomic_thread_fence(std::memory_order_release);
}


template <std::size_t N>
int SkipListCache<N>::Insert(PmemNode* const p) {
#ifdef CACHE_SIZE_IS_FIELD_COUNT
  size_t curr_size = size_.load(std::memory_order_acquire);
  if (curr_size + sizeof(Field) > capacity_) {
    int num_evicted = EvictSome();
    //fprintf(stdout, "Full. evict_cnt=%d\n", num_evicted);
    if (num_evicted == 0) {
      // Full. Nothing to evict
      return 1;
    }
  }
#endif
  Key& key = p->key[0];
  if(size_.load()==0) victim_key_ = key;

  //find insert pos
  uint64_t insert_pos=0;
  uint64_t curr_field_num = size_.load()/sizeof(Field);
  while(insert_pos < curr_field_num){
    if(fields[insert_pos].key.Compare(key) > 0) break;
    insert_pos++;
  }

  //do shift insert
  for(uint64_t i=curr_field_num;i>insert_pos;i--){
    fields[i] = fields[i-1];
  }
  fields[insert_pos].Set(key, p->height(), PmemPtr::OffsetOfVaddr(pool_id_, p));

  size_.fetch_add(sizeof(Field));
  return 0;
}

template <std::size_t N>
int SkipListCache<N>::LookupLessThanOrEqualsTo(const Key& key, PmemNode** out) {
  uint64_t curr_field_num = size_.load()/sizeof(Field);
  unsigned int pa = 0;
  unsigned int pb = curr_field_num-1;
  unsigned int i;
  unsigned int last_lt_pos = 0;
  i = (pb + pa) / 2;

  while (pa < pb) {
      i = (pb + pa) / 2;
      int cmp = fields[i].key.Compare(key);
      if (cmp == 0) {
        *out = DecodeFieldValue(fields[i]);
        return 0;
      } else if (cmp > 0) {
        pb = i;
        // [0, N) -> [0, N/2) -> ... -> [0, 1) -> [0, 0)
        continue;
      } else {
        last_lt_pos = i;
        pa = i + 1;
        // [0, N) -> [N/2 + 1, N) -> ... -> [N-1, N) -> [N, N)
        continue;
      }
    }

    *out = DecodeFieldValue(fields[last_lt_pos]);
    return -1;
}
#undef SKIPLIST_CACHE_BINARY_SEARCH



template <std::size_t N>
int SkipListCache<N>::EvictSome() {
  // Evict

  static const uint64_t kEvictionNumber = 4;
  unsigned int eviction_cnt = 0;

  uint64_t curr_field_num = size_.load()/sizeof(Field);
  uint64_t victim_pos = 0;
  for(uint64_t i=0; i<curr_field_num; i++){
    if(fields[i].key.Compare(victim_key_)==0){
      victim_pos = i;
      break;
    }
  }
  // evict lowest heights first
  while (eviction_cnt>=kEvictionNumber) {
    uint64_t back_victims = kEvictionNumber;
    uint64_t front_victims = 0;
    if(victim_pos+kEvictionNumber >=curr_field_num){
      front_victims = victim_pos+kEvictionNumber - (curr_field_num-1);
      back_victims = back_victims - front_victims;
    }

    for(uint64_t i=0; i<kEvictionNumber; i++){
      victim_pos++;
      if(victim_pos>=curr_field_num) victim_pos = 0;
    }
    victim_key_ = fields[victim_pos].key;

    // Drop victim
    for(uint64_t i=victim_pos; i<curr_field_num-back_victims; i++){
      fields[i] = fields[i+back_victims];
    }
    for(uint64_t i=front_victims-1; i<curr_field_num-front_victims; i++){
      fields[i] = fields[i+front_victims];
    }

    eviction_cnt += kEvictionNumber;
  }

  size_.fetch_sub(eviction_cnt * sizeof(Field));
  printf("end evict\n");

  return eviction_cnt;

}


#endif  // LISTDB_CORE_SKIPLIST_CACHE_H_
