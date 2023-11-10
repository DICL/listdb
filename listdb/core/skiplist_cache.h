#ifndef LISTDB_CORE_SKIPLIST_CACHE_H_
#define LISTDB_CORE_SKIPLIST_CACHE_H_

#include <algorithm>
#include <sstream>

#include "listdb/common.h"
#include "listdb/index/packed_pmem_skiplist.h"
#include "listdb/lsm/pmemtable2_list.h"
#include "listdb/util.h"
#include "listdb/util/random.h"

// TODO(wkim): Undefine this after doing the relevant works. Refer `SkipListCache::size_`
#define CACHE_SIZE_IS_FIELD_COUNT

template <std::size_t N>
class SkipListCache {
 public:
  using PmemNode = PackedPmemSkipList::Node;

  // Constructor
  SkipListCache(const int pool_id, const int region, size_t capacity = kSkipListCacheCapacity);

  void UpdateCache(PmemTable2List* l2_tl);

  // Returns 0 if equal, -1 lessthan, 1 not found
  int LookupLessThanOrEqualsTo(const Key& key, uint64_t* out);

  size_t AcquireLoadSize() { return size_.load(std::memory_order_acquire); }

  int GetCacheHeight() { return target_height; }

 private:

  const int pool_id_;
  const int region_;
  const size_t capacity_;
   std::atomic<size_t> size_;
  
  //maximum numbers of kv entries
  uint64_t MaxFieldNum;
  uint64_t CurrFieldNum;
  int target_height;

  Key* keys_;
  //values of cache is pmemptr of each keys
  uint64_t* values_;

};



template <std::size_t N>
SkipListCache<N>::SkipListCache(const int pool_id, const int region, size_t capacity)
  : pool_id_(pool_id),
    region_(region),
    capacity_(capacity),
    size_(capacity),
    CurrFieldNum(0),
    target_height(kMaxHeight){
  //calculate MaxFieldNum
  MaxFieldNum = (uint64_t)(capacity/(sizeof(Key)+sizeof(uint64_t)));
  keys_ = (Key*)malloc(sizeof(Key)*MaxFieldNum);
  values_ = (uint64_t*)malloc(sizeof(uint64_t)*MaxFieldNum);
  std::atomic_thread_fence(std::memory_order_release);
}

template <std::size_t N>
void SkipListCache<N>::UpdateCache(PmemTable2List* l2_tl) {
  uint64_t cnt[kMaxHeight] = {0,};

  //sum-up all cnt of l2 manifests
  auto l2_table = (PmemTable2*) l2_tl->GetFront();

  while (true) {
    if (l2_table){
      auto l2_manifest = l2_table->manifest<pmem_l2_info>();
      for(int i=0; i<kMaxHeight; i++){
        cnt[i] += l2_manifest->cnt[region_][i];
      }
    }
    else break;
    
    l2_table = (PmemTable2*)l2_table->Next();
  }


  //calculate the proper height of cache
  //target_height+1 is real target height(target height of 0 means height 1 indeed)
  target_height = kMaxHeight;

  for(int i=kMaxHeight-1; i>=1; i--){
    target_height = i+1;
    if(cnt[i] >= MaxFieldNum) break;
  }

  //traverse all skiplist and update array cache
  uint64_t checking_int = 0;
  uint64_t iter_cnt = 0;

  l2_table = (PmemTable2*) l2_tl->GetFront();
  //traverse table list
  while (true) {
    if (l2_table){
      auto l2_skiplist = l2_table->skiplist();

      PmemNode* pred = l2_skiplist->head(pool_id_);
      uint64_t curr_paddr_dump = pred->next[0];
      //pass through dummy node (head node)
      PmemNode* curr = (PmemNode*) ((PmemPtr*) &curr_paddr_dump)->get();
      //now curr is first node of skiplist
      pred = curr;

      while (true) {
        iter_cnt++;
        //check if this node go into cache
        if((uint64_t)((MaxFieldNum * iter_cnt)/cnt[target_height-1]) > checking_int){
          //insert into cache
          keys_[checking_int] = pred->min_key;
          values_[checking_int] = curr_paddr_dump;
          checking_int++;
        }

        //move to next node with target_height
        curr_paddr_dump = pred->next[target_height-1];
        curr = (PmemNode*) ((PmemPtr*) &curr_paddr_dump)->get();
        if (curr) {
          pred = curr;
          continue;
        }
        break;
      }

    }
    else break;
    
    l2_table = (PmemTable2*)l2_table->Next();
  }
  CurrFieldNum = checking_int;
  if(region_==0) printf("target height is %d and cache %lu out of %lu\n",target_height,checking_int,iter_cnt);//test juwon
}

template <std::size_t N>
int SkipListCache<N>::LookupLessThanOrEqualsTo(const Key& key, uint64_t* out) {
  
    //p,l,h for position, low, high for binary search
    uint64_t h = CurrFieldNum-1;
    uint64_t l = 0;
    uint64_t p = h/2;

    while(h>l){
      p = (l+h)/2;
      //do binary search in kvpair

      if(keys_[p].Compare(key) > 0){
        h = p-1;
        continue;
      }

      if(keys_[p].Compare(key) < 0){
        l = p+1;
        continue;
      }

      if (keys_[p].Compare(key) == 0) {
        *out = values_[p];
        return 0;
      }
    }

    if(keys_[p].Compare(key) < 0){
      *out = values_[p];
      return 1;
    }
    //goto left node sequentially while it meets smaller node
    else{
      p--;
      while(p>0){
        if(keys_[p].Compare(key) < 0){
          *out = values_[p];
          return 1;
        }
        else if(keys_[p].Compare(key) == 0){
          *out = values_[p];
          return 0;
        }
        p--;
      }
    }

    //case of p is 0
    if(keys_[0].Compare(key) < 0){
      *out = values_[0];
      return 1;
    }
    else if(keys_[0].Compare(key) == 0){
      *out = values_[0];
      return 0;
    }


    return -1;
}




#undef SKIPLIST_CACHE_BINARY_SEARCH




#endif  // LISTDB_CORE_SKIPLIST_CACHE_H_
