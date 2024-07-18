#ifndef LISTDB_INDEX_PACKED_PMEM_SKIPLIST_H_
#define LISTDB_INDEX_PACKED_PMEM_SKIPLIST_H_

#include <x86intrin.h>

#include <map>

#include <libpmemobj++/make_persistent_array_atomic.hpp>

#include "listdb/pmem/pmem.h"
#include "listdb/pmem/pmem_ptr.h"
#include "listdb/core/pmem_log.h"

class PackedPmemSkipList {
 public:
  struct HintedPtr{
      Key next_key;
      uint64_t next_ptr;
  };

  struct Node {
    uint8_t height; // pointer of KVpairs structure below
    Key min_key;
    HintedPtr next[1];
  };

  static const size_t MAX_BYTES = (sizeof(Key) + sizeof(Value)) * NPAIRS; // Example calculation
  //new structures for node 2
  struct KVpairs{
    uint64_t cnt;
    uint16_t offsets[NPAIRS];
    char bytes[MAX_BYTES];

    void InsertKV(const Key& key, const Value& value) {
      if(cnt==0) offsets[cnt] = 0;
      else offsets[cnt] = offsets[cnt-1]+sizeof(Key)+sizeof(Value);
      *reinterpret_cast<Key*>(bytes+offsets[cnt]) = key;
      *reinterpret_cast<Value*>(bytes+offsets[cnt]+sizeof(Key)) = value;
      cnt++;
    }

    inline Key KeyByIndex(size_t idx) const{
      return *reinterpret_cast<const Key*>(bytes+offsets[idx]);
    }

    inline Value ValueByIndex(size_t idx) const{
      return *reinterpret_cast<const Value*>(bytes+offsets[idx]+sizeof(Key));
    }


  };


  PackedPmemSkipList(int primary_region_pool_id);

  void BindArena(int pool_id, PmemLog* arena);

  void BindHead(const int pool_id, void* head_addr);

  void Init();

  Node* head() { return head_[primary_region_pool_id_]; }

  Node* head(int pool_id) { return head_[pool_id]; }

  int primary_pool_id() { return primary_region_pool_id_; }

  PmemPtr head_paddr() { return PmemPtr(primary_region_pool_id_, (char*) head_[primary_region_pool_id_]); }

  uint64_t head_paddr_dump(int pool_id) { return head_paddr_dump_[pool_id]; }

  pmem::obj::persistent_ptr<char[]> p_head(const int pool_id) { return p_head_[pool_id]; }

 private:
  const int primary_region_pool_id_;
  std::map<int, PmemLog*> arena_;
  std::map<int, Node*> head_;
  std::map<int, uint64_t> head_paddr_dump_;
  std::map<int, pmem::obj::persistent_ptr<char[]>> p_head_;
};

PackedPmemSkipList::PackedPmemSkipList(const int primary_region_pool_id)
    : primary_region_pool_id_(primary_region_pool_id) { }

void PackedPmemSkipList::BindArena(const int pool_id, PmemLog* arena) {
  arena_.emplace(pool_id, arena);
}

void PackedPmemSkipList::BindHead(const int pool_id, void* head_addr) {
  head_.emplace(pool_id, (Node*) head_addr);
}

void PackedPmemSkipList::Init() {
  for (auto& it : arena_) {
    int pool_id = it.first;
    size_t head_size = sizeof(Node) + (kMaxHeight - 1) * sizeof(HintedPtr);

    auto head_paddr = arena_[pool_id]->Allocate(head_size+sizeof(KVpairs));
    Node* head = (Node*) head_paddr.get();
    head->height = kMaxHeight;
    head->min_key = 0; 
    head->next[0].next_key = 0;
    memset(&head->next[0], 0, kMaxHeight * sizeof(HintedPtr));
    head_.emplace(pool_id, head);
    head_paddr_dump_.emplace(pool_id, head_paddr.dump());
  }
}

#endif  // LISTDB_INDEX_PACKED_PMEM_SKIPLIST_H_
