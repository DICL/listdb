#ifndef LISTDB_CORE_SKIPLIST_CACHE_H_
#define LISTDB_CORE_SKIPLIST_CACHE_H_

#include <algorithm>
#include <sstream>
#include <functional>
#include <cmath>

#include "listdb/common.h"
#include "listdb/index/packed_pmem_skiplist.h"
#include "listdb/lsm/pmemtable2_list.h"
#include "listdb/util.h"
#include "listdb/util/random.h"
#include "listdb/index/greedyplr_entities.h"
#include "listdb/index/greedyplr.h"

//to use GreedyPLR
using namespace PLR;
#define ERROR_BOUND 16

// TODO(wkim): Undefine this after doing the relevant works. Refer `SkipListCache::size_`
//#define CACHE_SIZE_IS_FIELD_COUNT

template <std::size_t N>
class SkipListCache {
 public:
  using PmemNode = PackedPmemSkipList::Node;

  // Constructor
  SkipListCache(const int pool_id, const int region, size_t capacity = kSkipListCacheCapacity);

  void UpdateCache(PmemTable2List* l2_tl);

  // Returns 0 if equal, -1 lessthan, 1 not found
  int LookupLessThanOrEqualsTo(const Key& key, uint64_t* out);

  int GetCacheHeight() { return target_height; }

 private:

  const int pool_id_;
  const int region_;
  const size_t capacity_;
  
  uint64_t CurrFieldNum;//maximum numbers of kv entries
  int target_height;//height which can skip by lookup cache

#ifdef LISTDB_GREEDY_PLR
  GreedyPLR* greedy_;
  //Whether traing of learned index is fit in memory or not
  bool train_fit_;
#endif

  Key* keys_;
  uint64_t* values_; //values of cache is pmemptr of each keys

};



template <std::size_t N>
SkipListCache<N>::SkipListCache(const int pool_id, const int region, size_t capacity)
  : pool_id_(pool_id),
    region_(region),
    capacity_(capacity),
    CurrFieldNum(0),
    target_height(kMaxHeight)
    {
  //calculate MaxFieldNum
  size_t capacity_for_kv_array = capacity;
#ifdef LISTDB_GREEDY_PLR
  capacity_for_kv_array *= (1-kLearnedIndexCapacityRatio);
#endif
  uint64_t MaxFieldNum = (uint64_t)((capacity_for_kv_array-sizeof(SkipListCache))/(sizeof(Key)+sizeof(uint64_t)));
  keys_ = (Key*)malloc(sizeof(Key)*MaxFieldNum);
  values_ = (uint64_t*)malloc(sizeof(uint64_t)*MaxFieldNum);
  std::atomic_thread_fence(std::memory_order_release);
}

template <std::size_t N>
void SkipListCache<N>::UpdateCache(PmemTable2List* l2_tl) {
  uint64_t cnt[kMaxHeight] = {0,};
  uint64_t MaxFieldNum = (uint64_t)(capacity_/(sizeof(Key)+sizeof(uint64_t)));

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
  bool first_table_flag = true;
  //user prev key to guarantee sorted order
  Key prev_key(0);

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
      if (curr){
        if(first_table_flag) first_table_flag=false;
        //case : table is not in sorted order
        else if(curr->min_key.Compare(prev_key)<0) break;
      }

      while (true) {
        if (curr) {
          pred = curr;
        }
        else break;

        iter_cnt++;
        //check if this node go into cache
        if((uint64_t)((MaxFieldNum * iter_cnt)/cnt[target_height-1]) > checking_int){
          //insert into cache
          keys_[checking_int] = pred->min_key;
          values_[checking_int] = curr_paddr_dump;

          prev_key = pred->min_key;
          checking_int++;
        }

        //move to next node with target_height
        curr_paddr_dump = pred->next[target_height-1];
        curr = (PmemNode*) ((PmemPtr*) &curr_paddr_dump)->get();
        
      }

    }
    else break;
    
    l2_table = (PmemTable2*)l2_table->Next();


  }
  CurrFieldNum = checking_int;
  if(region_==0) printf("target height is %d and cache %lu out of %lu\n",target_height,checking_int,iter_cnt);//test juwon

#ifdef LISTDB_GREEDY_PLR
  if(greedy_ != nullptr) delete greedy_;
  greedy_ = new GreedyPLR(ERROR_BOUND, capacity_*kLearnedIndexCapacityRatio);
  train_fit_ = greedy_->train(keys_, CurrFieldNum);
  if(region_==0) greedy_->report();//test juwon
#endif
}

template <std::size_t N>
int SkipListCache<N>::LookupLessThanOrEqualsTo(const Key& key, uint64_t* out) {
  uint64_t h = CurrFieldNum-1;
  uint64_t l = 0;
  uint64_t p = h/2;
  int rv;
  
#ifdef LISTDB_GREEDY_PLR //using leaned index method (juwon)
  //if learned index successfully trained
  if(train_fit_){
    p = greedy_->predict(key);
    if(p>CurrFieldNum-1) p = CurrFieldNum-1;
    h = p+ERROR_BOUND;
    if(h>CurrFieldNum-1) h = CurrFieldNum-1;
    l = p-ERROR_BOUND;
    if(p<ERROR_BOUND) l = 0;
  }
  /*
  //case of use linear search from p
  rv = keys_[p].Compare(key);
  if(rv == 0){
    *out = values_[p];
    return 0;
  }
  else if(rv < 0){
    while(p<CurrFieldNum-1){
        if(rv = keys_[p+1].Compare(key); rv >= 0){
          if(rv==0){
            *out = values_[p+1];
            return 0;
          }
          *out = values_[p];
          return 1;
        }
        p++;
    }
  }
  else{
    while(p>0){
        p--;
        if(rv = keys_[p].Compare(key); rv <= 0){
          *out = values_[p];
          if(rv==0) return 0;
          return 1;
        }
    }
  }

  if(p==0 && keys_[p].Compare(key)>0)return -1;
  else if(p==CurrFieldNum-1 || p==0){
    *out = values_[p];
    return 1;
  }

  return -1;
  */

#endif
    //do binary search from p
    while(h>=l){
      p = (l+h)/2;
      //do binary search in kvpair

      rv = keys_[p].Compare(key);

      //case 1
      if(rv > 0){
        //evict corner case (because of unsigned integer)
        if(p==0) return -1;
        h = p-1;
        continue;
      }
      //case 2
      else if(rv < 0){
        l = p+1;
        continue;
      }
      //case 3 : matched
      else if(rv == 0) {
        *out = values_[p];
        return 0;
      }
    }
    if(keys_[p].Compare(key) < 0){
      *out = values_[p];
      return 1;
    }
    //goto left node sequentially while it found smaller node
    else{
      while(p>0){
        p--;
        if(keys_[p].Compare(key) < 0){
          *out = values_[p];
          return 1;
        }
        else if(keys_[p].Compare(key) == 0){
          *out = values_[p];
          return 0;
        }
      }
    }

    return -1;
}




#undef SKIPLIST_CACHE_BINARY_SEARCH




#endif  // LISTDB_CORE_SKIPLIST_CACHE_H_
