#pragma once

#include <bitset>
#include <memory>

#include <libpmemobj.h>

#include "pmem.h"
#include "common.h"
#include "ds/lockfree_skiplist.h"
#include "ds/nodes.h"

class PRegionIterator;

class lockfree_pskiplist {
  friend class PRegionIterator;
  friend class casc_pskiplist_iterator;
 public:
#ifndef BR_STRING_KV
  typedef uint64_t Key;
  typedef struct UINT64_pnode Node;
  typedef struct UINT64_pcmp NodeCmp;
  typedef struct UINT64_node mem_Node;
#else
  typedef std::string_view Key;
  typedef struct VARSTR_pnode Node;
  typedef struct VARSTR_pcmp NodeCmp;
  typedef struct VARSTR_node mem_Node;
#endif
  // Zipper Compaction Item: (lowNode, preds[], succs[])
  struct zc_item {
    int region;
    Node* node;
    Node* preds[kMaxHeight];
    uint64_t succs[kMaxHeight];
    zc_item(const int r, Node* node_in, Node* preds_in[], uint64_t succs_in[]) {
      region = r;
      node = node_in;
      memcpy((void*) preds, (void*) preds_in, kMaxHeight * sizeof(Node*));
      memcpy((void*) succs, (void*) succs_in, kMaxHeight * sizeof(uint64_t));
    }
  };

 public:
  lockfree_pskiplist(PMEMobjpool* pops[]);
  void init();
  void load(TOID(BraidedSkipListBase)& base);
  TOID(BraidedSkipListBase) base();
  Node* new_node(ThreadData* td, const int r, const Key& key, const uint64_t value, int height = 0);
  // Returns pred
  Node* insert(ThreadData* const td, const int r, Node* const node, Node* pred = NULL, bool persist = true);
  // Returns (node->key == key) ? node : NULL
  Node* find(ThreadData* const td, const int r, const Key& key, const Node* pred = NULL);
  void merge_WAL(ThreadData* td, const unsigned long rmask, lockfree_skiplist* other);
  void merge_IUL(ThreadData* td, const unsigned long rmask, lockfree_skiplist* other);
  Node* head(const int r);
  void zipper_compaction(ThreadData* const td, const unsigned long rmask, const int lower_level, lockfree_pskiplist* lower);
  void log_structured_compaction(ThreadData* const td, const unsigned long rmask, const int lower_level, lockfree_pskiplist* lower);

  std::shared_ptr<PRegionIterator> new_region_iterator(const unsigned long rmask);

  Node* get_node_by_offset(const int region, const uint64_t offset);
  Node* get_node_by_moff(const uint64_t moff);

  void debug_scan();

 public:
  void find_position(ThreadData* const td, const int r, Node* node, Node* preds[],
                     uint64_t succs[], Node* pred = NULL, const int min_h = 0);
  void find_position_braided(ThreadData* const td, Node* node, Node* preds[], uint64_t succs[], Node* pred = NULL);
  Node* find_braided(ThreadData* const td, const Key& key, const Node* pred);
  void* offset_to_ptr(const int r, const uint64_t offset);
  void* moff_to_ptr(const uint64_t moff);
  void* moff_to_ptr(const uint64_t moff, int16_t& r);
  int random_height(Random& rnd);
#if 1
  Node* get_shortcut(const Node* node);
#endif

 public:
  NodeCmp cmp_;
  Node* head_[kMaxNumRegions];
  PMEMobjpool* pop_[kMaxNumRegions];
  TOID(BraidedSkipListBase) base_;
};

class PRegionIterator {
  using Node = lockfree_pskiplist::Node;
 public:
  PRegionIterator(lockfree_pskiplist* const skiplist, const unsigned long rmask);
  bool valid();
  void next();
  Node* node();
  int region();
 private:
  lockfree_pskiplist* const skiplist_;
  //const unsigned long rmask_;
  std::bitset<kMaxNumRegions> rbs_;
  int r_;
  Node* node_;
};

#if 1
class casc_pskiplist_iterator {
 public:
  using Key = lockfree_pskiplist::Key;
  using Node = lockfree_pskiplist::Node;
  using NodeCmp = lockfree_skiplist::NodeCmp;

  casc_pskiplist_iterator(const int region, lockfree_pskiplist* const skiplist);
  void seek(const Key& key, const Node* pred = NULL);
  //void seek_two(const Key& key, Node* pred, const Key& key_a);
  void seek_braided(const Key& key, const Node* pred);
  //bool is_ready_new_shortcut();
  //uint64_t pred_a_moff();
  void next();
  bool valid() const;
  int cmp();
  //Node* pred();
  Node* node();
  const Node* shortcut();

 public:
  const int r_;
  lockfree_pskiplist* const skiplist_;
  //Node* pred_;
  Node* node_;
  int cmp_;
  const Node* shortcut_;
  //uint64_t pred_a_offset_;
};
// shortcut version 2
// every node has shortcut information
#if 0
class casc_pskiplist_iterator {
 public:
  using Key = lockfree_pskiplist::Key;
  using Node = lockfree_pskiplist::Node;
  using NodeCmp = lockfree_skiplist::NodeCmp;

  casc_pskiplist_iterator(lockfree_pskiplist* const skiplist, const int region);
  void seek(const Key& key, Node* pred = NULL);
  void seek_two(const Key& key, Node* pred, const Key& key_a);
  void seek_braided(const Key& key, Node* pred);
  bool is_ready_new_shortcut();
  uint64_t pred_a_moff();
  bool valid();
  int cmp();
  Node* pred();
  Node* node();
  Node* shortcut();

 private:
  lockfree_pskiplist* const skiplist_;
  const int r_;
  Node* pred_;
  Node* node_;
  int cmp_;
  Node* shortcut_;
  uint64_t pred_a_offset_;
};
#endif
#endif
