#ifndef LISTDB_INDEX_BRAIDED_PMEM_SKIPLIST_H_
#define LISTDB_INDEX_BRAIDED_PMEM_SKIPLIST_H_

#include <x86intrin.h>

#include <map>

#include <libpmemobj++/make_persistent_array_atomic.hpp>

#include "listdb/pmem/pmem.h"
#include "listdb/pmem/pmem_ptr.h"
#include "listdb/core/pmem_log.h"

class BraidedPmemSkipList {
 public:
  struct Node {
    Key key;
    uint64_t tag;  // seqorder (56-bit), op (4-bit), height (4-bit)
    uint64_t value;
    uint64_t next[1];

    int height() const { return tag & 0xf; }

    uint32_t l0_id() const { return (tag >> 32); }
  };

  BraidedPmemSkipList(int primary_region_pool_id);

  void BindArena(int pool_id, PmemLog* arena);

  void BindHead(const int pool_id, void* head_addr);

  void Init();

  void Insert(PmemPtr node_paddr);

  void FindPosition(int pool_id, Node* node, Node* preds[], uint64_t succs[]);

  PmemPtr Lookup(const Key& key, int pool_id);

  void PrintDebugScan();

  Node* head() { return head_[primary_region_pool_id_]; }

  Node* head(int pool_id) { return head_[pool_id]; }

  int primary_pool_id() { return primary_region_pool_id_; }

  PmemPtr head_paddr() { return PmemPtr(primary_region_pool_id_, (char*) head_[primary_region_pool_id_]); }

  pmem::obj::persistent_ptr<char[]> p_head(const int pool_id) { return p_head_[pool_id]; }

 private:
  const int primary_region_pool_id_;
  std::map<int, PmemLog*> arena_;
  std::map<int, Node*> head_;
  std::map<int, pmem::obj::persistent_ptr<char[]>> p_head_;
};

BraidedPmemSkipList::BraidedPmemSkipList(const int primary_region_pool_id)
    : primary_region_pool_id_(primary_region_pool_id) { }

void BraidedPmemSkipList::BindArena(const int pool_id, PmemLog* arena) {
  arena_.emplace(pool_id, arena);
}

void BraidedPmemSkipList::BindHead(const int pool_id, void* head_addr) {
  head_.emplace(pool_id, (Node*) head_addr);
}

void BraidedPmemSkipList::Init() {
  for (auto& it : arena_) {
    int pool_id = it.first;
    size_t head_size = sizeof(Node) + (kMaxHeight - 1) * sizeof(uint64_t);
#if 0
    auto head_paddr = arena_[pool_id]->Allocate(head_size);
    Node* head = (Node*) head_paddr.get();
#else
    auto pool = it.second->pool();
    pmem::obj::persistent_ptr<char[]> pmem_head_buf;
    pmem::obj::make_persistent_atomic<char[]>(pool, pmem_head_buf, head_size);
    p_head_[pool_id] = pmem_head_buf;
    Node* head = (Node*) pmem_head_buf.get();
#endif
    head->key = 0; 
    head->tag = kMaxHeight;
    memset(&head->next[0], 0, kMaxHeight * sizeof(uint64_t));
    head_.emplace(pool_id, head);
  }
}

void BraidedPmemSkipList::Insert(PmemPtr node_paddr) {
  int pool_id = node_paddr.pool_id();
  Node* node = (Node*) node_paddr.get();
  int height = node->height();

  Node* preds[kMaxHeight];
  uint64_t succs[kMaxHeight];

  while (true) {
    preds[kMaxHeight - 1] = head_[pool_id];
    FindPosition(pool_id, node, preds, succs);

#if 0
    // Set next pointers
    for (int i = 0; i < height; i++) {
      _mm_stream_pi((__m64*) &node->next[i], (__m64) succs[i]);
    }
    //_mm_sfence();
#else
    for (int i = 0; i < height; i++) {
      node->next[i] = succs[i];
    }
    //memcpy(node->next, succs, height * sizeof(uint64_t));
#endif

    // Stores are not reordered with other stores.
    // what about crash-consistency?
    if (!std::atomic_compare_exchange_strong((std::atomic<uint64_t>*) &preds[0]->next[0], &succs[0],
          node_paddr.dump())) {
      continue;
    }

    for (int i = 1; i < height; i++) {
      while (true) {
        if (!std::atomic_compare_exchange_strong((std::atomic<uint64_t>*) &preds[i]->next[i], &succs[i],
              node_paddr.dump())) {
          preds[kMaxHeight - 1] = head_[pool_id];
          FindPosition(pool_id, node, preds, succs);
          continue;
        }
        break;
      }
    }

    break;
  }
  //Node* succ = (Node*) ((PmemPtr*) &succs[0])->get();
  //uint64_t sk = (succ) ? (uint64_t) succ->key : 0ul;
  //fprintf(stdout, "Key inserted %zu -> [ %zu ] -> %zu\n", preds[0]->key, node->key, sk);
  //PrintDebugScan();
}

void BraidedPmemSkipList::FindPosition(const int pool_id, Node* node, Node* preds[], uint64_t succs[]) {
  //Node* pred = head_[pool_id];
  Node* pred = preds[kMaxHeight - 1];
  uint64_t curr_paddr_dump;
  Node* curr;
  int height = pred->height();

  // NUMA-local upper layers
  for (int i = height - 1; i >= 1; i--) {
    while (true) {
      curr_paddr_dump = pred->next[i];
      curr = (Node*) ((PmemPtr*) &curr_paddr_dump)->get();
      if (curr && curr->key.Compare(node->key) < 0) {
        pred = curr;
        continue;
      }
      break;
    }
    preds[i] = pred;
    succs[i] = curr_paddr_dump;
  }

  // Braided bottom layer
  if (pred == head_[pool_id]) {
    pred = head_[primary_region_pool_id_];
  }
  while (true) {
    curr_paddr_dump = pred->next[0];
    curr = (Node*) ((PmemPtr*) &curr_paddr_dump)->get();
    if (curr && curr->key.Compare(node->key) < 0) {
      pred = curr;
      continue;
    }
    break;
  }
  preds[0] = pred;
  succs[0] = curr_paddr_dump;
}

PmemPtr BraidedPmemSkipList::Lookup(const Key& key, const int pool_id) {
  Node* pred = head_[pool_id];
  uint64_t curr_paddr_dump;
  Node* curr;
  int height = pred->height();

  // NUMA-local upper layers
  for (int i = height - 1; i >= 1; i--) {
    while (true) {
      curr_paddr_dump = pred->next[i];
      curr = (Node*) ((PmemPtr*) &curr_paddr_dump)->get();
      if (curr && curr->key.Compare(key) < 0) {
        pred = curr;
        continue;
      }
      break;
    }
  }

  // Braided bottom layer
  if (pred == head_[pool_id]) {
    pred = head_[primary_region_pool_id_];
  }
  while (true) {
    curr_paddr_dump = pred->next[0];
    curr = (Node*) ((PmemPtr*) &curr_paddr_dump)->get();
    if (curr && curr->key.Compare(key) < 0) {
      pred = curr;
      continue;
    }
    //fprintf(stdout, "lookupkey=%zu, curr->key=%zu\n", key, curr->key);
    break;
  }
  return curr_paddr_dump;
}

void BraidedPmemSkipList::PrintDebugScan() {
  //std::string s;
  //Node* pred = head_[primary_region_pool_id_];
  //Node* curr = (Node*) ((PmemPtr*) &pred->next[0])->get();
  //while (curr) {
  //  s.append(std::to_string((uint64_t) curr->key) + "->");
  //  curr = (Node*) ((PmemPtr*) &curr->next[0])->get();
  //}
  //fprintf(stdout, "SCAN %s\n", s.c_str());
}

#endif  // LISTDB_INDEX_BRAIDED_PMEM_SKIPLIST_H_
