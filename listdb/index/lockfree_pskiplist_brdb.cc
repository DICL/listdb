#include "ds/lockfree_pskiplist.h"

#include <bitset>
#include <new>
#include <stack>
#include <stdlib.h>
#include <thread>
#include <numa.h>

#include "common.h"
#include "ds/lockfree_skiplist.h"

lockfree_pskiplist::lockfree_pskiplist(PMEMobjpool* pops[]) {
  for (int i = 0; i < kNumRegions; i++) {
    pop_[i] = pops[i];
  }
}

void lockfree_pskiplist::init() {
  TOID(BraidedSkipListBase) base;
  POBJ_ZALLOC(pop_[kPrimaryRegion], &base, BraidedSkipListBase, sizeof(BraidedSkipListBase));
  for (int i = 0; i < kNumRegions; i++) {
    auto head_key = Node::head_key();
    size_t alloc_size = Node::compute_alloc_size(head_key, kMaxHeight);
    TOID(char) head_buf;
    POBJ_ALLOC(pop_[i], &head_buf, char, alloc_size, NULL, NULL);
    head_[i] = Node::init_node(D_RW(head_buf), head_key, 0xffffffffffffffff, 0, kMaxHeight);
    pmemobj_persist(pop_[i], D_RW(head_buf), alloc_size);
    D_RW(base)->head[i] = head_buf;
  }
  pmemobj_persist(pop_[kPrimaryRegion], D_RW(base), sizeof(BraidedSkipListBase));
}

void lockfree_pskiplist::load(TOID(BraidedSkipListBase)& base) {
  for (int i = 0; i < kNumRegions; i++) {
    head_[i] = (Node*) D_RW(D_RW(base)->head[i]);
  }
  base_ = base;
  std::atomic_thread_fence(std::memory_order_release);
}

TOID(BraidedSkipListBase) lockfree_pskiplist::base() {
  return base_;
}

lockfree_pskiplist::Node* lockfree_pskiplist::new_node(ThreadData* td, const int r, const Key& key, const uint64_t value, int height) {
  if (height == 0) {
    height = 1;
    static const unsigned int kBranching = 4;
    const int bb = (kBranching > (unsigned int) kNumRegions) ? kBranching / kNumRegions : 1;
    if (height < kMaxHeight && ((td->rnd.Next() % bb) == 0)) {
      height++;
      while (height < kMaxHeight && ((td->rnd.Next() % kBranching) == 0)) {
        height++;
      }
    }
  }
  size_t palloc_size = Node::compute_alloc_size(key, height);
  TOID(char) pbuf;
  POBJ_ALLOC(pop_[r], &pbuf, char, palloc_size, NULL, NULL);
  Node* new_node = Node::init_node(D_RW(pbuf), key, 0x1, value, height);
  new_node->next[0].store(0);
  pmemobj_persist(pop_[r], new_node->data(), new_node->alloc_size());
  return new_node;
}

lockfree_pskiplist::Node* lockfree_pskiplist::insert(ThreadData* const td, const int r, Node* const node, Node* pred, bool persist)  {
  if (pred == NULL) {
    pred = head_[r];
  }
  uint64_t node_offset = (uintptr_t) node - (uintptr_t) pop_[r];
  uint64_t node_moff = (node_offset << 16) | (int16_t) r;
  assert(node == moff_to_ptr(node_moff));
  Node* preds[kMaxHeight];
  uint64_t succs[kMaxHeight];
  while (true) {
    find_position(td, r, node, preds, succs, pred);
    for (int l = 1; l < node->height; l++) {
      node->next[l].store(succs[l]);
    }
    node->next[0].store(succs[0]);
    if (persist) {
      pmemobj_persist(pop_[r], node->next, node->height * 8);
    }
    if (!preds[0]->next[0].compare_exchange_strong(succs[0], node_moff)) {
      pred = preds[kMaxHeight - 1];
      continue;
    }
    if (persist) {
      int bottom_pred_region = (preds[0] == head_[kPrimaryRegion]) ? kPrimaryRegion : r;
      pmemobj_persist(pop_[bottom_pred_region], preds[0]->next, 8);
    }

    for (int l = 1; l < node->height; l++) {
      while (true) {
        if (!preds[l]->next[l].compare_exchange_strong(succs[l], node_offset)) {
          find_position(td, r, node, preds, succs, preds[kMaxHeight - 1], l);
          continue;
        }
        break;
      }
    }
    break;
  }
  return preds[kMaxHeight - 1];
}

lockfree_pskiplist::Node* lockfree_pskiplist::find(ThreadData* const td, const int r, const Key& key, const Node* pred) {
  if (pred == NULL) {
    pred = head_[r];
  }
  uint64_t curr_offset;
  Node* curr;
  int h = pred->height;
  int cr;
  for (int l = h - 1; l >= 1; l--) {
    while (true) {
      curr_offset = pred->next[l].load();
      curr = (Node*) offset_to_ptr(r, curr_offset);
      if ((cr = cmp_(curr, key)) < 0) {
//td->visit_cnt++;
        pred = curr;
        continue;
      }
      break;
    }
  }
  if (pred == head_[r] && r != kPrimaryRegion) {
    pred = head_[kPrimaryRegion];
  }
  return find_braided(td, key, pred);
  //return (cr == 0) ? curr : NULL;
}

void lockfree_pskiplist::merge_WAL(ThreadData* td, const unsigned long rmask, lockfree_skiplist* other) {
  std::bitset<kMaxNumRegions> bs(rmask);
  Node* pred[kMaxNumRegions];
  for (int i = 0; i < kNumRegions; i++) {
    pred[i] = head_[i];
  }
  mem_Node* o = other->head()->next[0].load();
  while (o) {
    // *** Create a new persistent node
    // Random Height
    int height = random_height(td->rnd);
    int16_t r = 0xffff & o->log_moff;
    if (o->type() == kTypeValue && bs.test(r)) {
      size_t palloc_size = Node::compute_alloc_size(o->key(), height);
      TOID(char) pbuf;
      POBJ_ALLOC(pop_[r], &pbuf, char, palloc_size, NULL, NULL);
      Node* new_node = Node::init_node(D_RW(pbuf), o->key(), o->tag(), o->value(), height);
      new_node->next[0].store(0);
      pmemobj_persist(pop_[r], new_node->data(), new_node->alloc_size());

      pred[r] = insert(td, r, new_node, pred[r], /*persist=*/true);
#ifndef BR_STRING_KV
      ht_add(o->key(), o->value());
#else
      ht_add((uint64_t) D_RW(pbuf), o->value());
#endif
      td->mem_compaction_cnt++;
    }
    o = o->next[0].load();
  }
}

void lockfree_pskiplist::merge_IUL(ThreadData* td, const unsigned long rmask, lockfree_skiplist* other) {
  std::bitset<kMaxNumRegions> bs(rmask);
  Node* pred[kNumRegions];
  for (int i = 0; i < kNumRegions; i++) {
    pred[i] = head_[i];
  }
  mem_Node* o = other->head()->next[0].load();
  while (o) {
    int16_t r = 0xffff & o->log_moff;
    if (o->type() == kTypeValue && bs.test(r)) {
      uint64_t log_offset = o->log_moff >> 16;
      char* node_buf = (char*) offset_to_ptr(r, log_offset);
      Node* new_node = Node::load_node(node_buf);
//{
//  // FOR TEST
//  if (o->key() != new_node->key()) {
//    fprintf(stderr, "mem key: %zu iul key: %zu\n", o->key(), new_node->key());
//    abort();
//  }
//  if (o->type() != new_node->type()) {
//    fprintf(stderr, "mem type: %d iul key: %d\n", o->type(), new_node->type());
//    abort();
//  }
//  //if (o->height != new_node->height) {
//  //  fprintf(stderr, "mem height: %d iul height: %d\n", o->height, new_node->height);
//  //  abort();
//  //}
//}
      pred[r] = insert(td, r, new_node, pred[r], /*persist=*/false);
#ifndef BR_STRING_KV
      //ht_add_stash(o->key(), o->value());
//#ifndef LPV2
//      ht_add_lp(o->key(), o->value());
//#else
//      ht_add_lp_v2(o->key(), o->value());
//#endif
      ht_add(o->key(), o->value());
#else
      ht_add((uint64_t) node_buf, o->value());
#endif
      td->mem_compaction_cnt++;
    } else if (/* FOR TEST */true && o->type() == kTypeShortcut && bs.test(r)) {
      uint64_t log_offset = o->log_moff >> 16;
      char* node_buf = (char*) offset_to_ptr(r, log_offset);
      Node* new_node = Node::load_node(node_buf);
      pred[r] = insert(td, r, new_node, pred[r], /*persist=*/false);
      //fprintf(stderr,"TSET dram key=%zu pmem key=%zu\n", ((mem_Node*) o->value())->key(), ((Node*) moff_to_ptr(o->log_moff))->key());
      // Creation: MemTable::add()
      // Deliver: casc_pskiplist_iterator::shortcut() <= seek()
      // Use: casc_pmem_level_getter::seek()
      //size_t palloc_size = Node::compute_alloc_size(o->key(), o->height);
      //TOID(char) pbuf;
      //POBJ_ALLOC(pop_[r], &pbuf, char, palloc_size, NULL, NULL);
      //Node* new_node = Node::init_node(D_RW(pbuf), o->key(), o->tag(), o->log_moff, o->height);
      //new_node->next[0].store(0);
      //pmemobj_persist(pop_[r], new_node->data(), new_node->alloc_size());

      //pred[r] = insert(td, r, new_node, pred[r], /*persist=*/false);
    }
    o = o->next[0].load();
  }
}

lockfree_pskiplist::Node* lockfree_pskiplist::head(const int r) {
  return head_[r];
}

// `r`: Shortcut region
void lockfree_pskiplist::zipper_compaction(ThreadData* const td, const unsigned long rmask, const int lower_level, lockfree_pskiplist* lower) {
  Node* lnode;
  Node* unode[kMaxNumRegions];
  Node* preds[kMaxHeight];
  uint64_t succs[kMaxHeight];
  int16_t rr;  // region read from node
  std::deque<zc_item> item_stack;

  std::bitset<kMaxNumRegions> bs(rmask);
  lnode = lower->head(kPrimaryRegion);
  assert(lnode != NULL);
  do {
    lnode = (Node*) lower->moff_to_ptr(lnode->next[0].load(), rr);
  } while (lnode && !bs.test(rr));

  for (int i = 0; i < kNumRegions; i++) {
    if (bs.test(i)) {
      unode[i] = head_[i];
    } else {
      unode[i] = NULL;
    }
  }
  // 1. Scan Phase
  while (lnode) {
    if (/* FOR TEST */true || lnode->type() != kTypeShortcut) {
      assert(lnode->key() != 0);
      find_position(td, rr, lnode, preds, succs, unode[rr]);
      item_stack.emplace_back(rr, lnode, preds, succs);
      // XXX: we can optimize this further, like by unode = preds[lnext->height]
      unode[rr] = preds[kMaxHeight - 1];
    }
    do {
      lnode = (Node*) lower->moff_to_ptr(lnode->next[0].load(), rr);
    } while (lnode && !bs.test(rr));
  }
  // 2. Merge Phase
  while (!item_stack.empty()) {
    auto& z = item_stack.back();  // item being merged
    while (true) {
      uint64_t ori_moff = z.node->next[0].load();  // original, lower
      Node* ori_succ = (Node*) moff_to_ptr(ori_moff);
      uint64_t cand_moff = z.succs[0];  // candidate, upper
      Node* cand_succ = (Node*) moff_to_ptr(cand_moff);
      uint64_t expected_succ_moff;
      if (ori_succ == NULL || cmp_(cand_succ, ori_succ) < 0) {
        z.node->next[0].store(cand_moff);
        // lazy persist?
        expected_succ_moff = cand_moff;
      } else {
        expected_succ_moff = ori_moff;
      }
      uint64_t z_offset = (uintptr_t) z.node - (uintptr_t) pop_[z.region];
      uint64_t z_moff = (z_offset << 16) | (int16_t) z.region;
      if (!z.preds[0]->next[0].compare_exchange_strong(expected_succ_moff, z_moff)) {
        // With high probability, consecutive zipper items will fail on CAS
        find_position(td, z.region, z.node, z.preds, z.succs, z.preds[kMaxHeight-1]);
        continue;
      } else {
      // lazy persist?
        break;
      }
    }

    // height 1 to top
    for (int h = 1; h < z.node->height; h++) {
      while (true) {
        uint64_t ori_offset = z.node->next[h].load();
        Node* ori_succ = (Node*) offset_to_ptr(z.region, ori_offset);
        uint64_t cand_offset = z.succs[h];
        Node* cand_succ = (Node*) offset_to_ptr(z.region, cand_offset);
        uint64_t expected_succ_offset;
        if (ori_succ == NULL || cmp_(cand_succ, ori_succ) < 0) {
          z.node->next[h].store(cand_offset);
          // lazy persist?
          expected_succ_offset = cand_offset;
        } else {
          expected_succ_offset = ori_offset;
        }
        uint64_t z_offset = (uintptr_t) z.node - (uintptr_t) pop_[z.region];
        if (!z.preds[h]->next[h].compare_exchange_strong(expected_succ_offset, z_offset)) {
          find_position(td, z.region, z.node, z.preds, z.succs, z.preds[kMaxHeight - 1], h);
          continue;
        } else {
        // lazy persist?
          break;
        }
      }
    }
    td->pmem_compaction_cnt[lower_level]++;
    item_stack.pop_back();
  }
}

void lockfree_pskiplist::log_structured_compaction(ThreadData* td, const unsigned long rmask, const int lower_level, lockfree_pskiplist* lower) {
  Node* pred[kMaxNumRegions];
  for (int i = 0; i < kNumRegions; i++) {
    pred[i] = head_[i];
  }
  auto it = lower->new_region_iterator(rmask);
  while (it->valid()) {
    Node* lnode = it->node();
    if (lnode->type() != kTypeShortcut) {
      int r = it->region();
      int height = random_height(td->rnd);
      TOID(char) new_node_buf;
      size_t alloc_size = Node::compute_alloc_size(lnode->key(), height);
      POBJ_ALLOC(pop_[r], &new_node_buf, char, alloc_size, NULL, NULL);
      Node* new_node = Node::init_node(D_RW(new_node_buf), lnode->key(), lnode->tag(), lnode->value(), height);
      pmemobj_persist(pop_[r], D_RW(new_node_buf), alloc_size - (height-1)*8);
      pred[r] = insert(td, r, new_node, pred[r], /*persist=*/true);
      td->pmem_compaction_cnt[lower_level]++;
    }
    it->next();
  }
}

// Refer this code for later researches
//  see how num_consec_locals works
//void lockfree_pskiplist::log_structured_compaction_all_regions(ThreadData* const td, const int lower_level, lockfree_pskiplist* lower) {
//  Node* pred[kMaxNumRegions];
//  for (int i = 0; i < kNumRegions; i++) {
//    pred[i] = head_[i];
//  }
//
//  const int num_consec_locals = 1;
//  int16_t lr = -1;
//  Node* lnode = (Node*) lower->moff_to_ptr(lower->head(kPrimaryRegion)->next[0].load(), lr);
//  while (lnode) {
//    for (int i = 0; lnode && i < kNumRegions; i++) {
//      for (int j = 0; lnode && j < num_consec_locals; j++) {
//        int height = random_height(td->rnd);
//        TOID(char) new_node_buf;
//        size_t alloc_size = Node::compute_alloc_size(lnode->key(), height);
//        POBJ_ALLOC(pop_[i], &new_node_buf, char, alloc_size, NULL, NULL);
//        Node* new_node = Node::init_node(D_RW(new_node_buf), lnode->key(), lnode->tag(), lnode->value(), height);
//        pmemobj_persist(pop_[i], D_RW(new_node_buf), alloc_size - (height-1)*8);
//
//        pred[i] = insert(td, i, new_node, pred[i], /*persist=*/true);
//        td->pmem_compaction_cnt[lower_level]++;
//
//        // Go to the next
//        lnode = (Node*) lower->moff_to_ptr(lnode->next[0].load(), lr);
//      }
//    }
//  }
//}

void lockfree_pskiplist::find_position(ThreadData* const td, const int r, Node* node, Node* preds[], uint64_t succs[], Node* pred, const int min_h) {
  if (pred == NULL) {
    pred = head_[r];
  }
  uint64_t curr_offset;
  Node* curr;
  int h = pred->height;
  int l;
  for (l = h - 1; l >= std::max(1, min_h); l--) {
    while (true) {
//if (pred) fprintf(stdout, "pred->key=%zu @%p\n", pred->key, pred);
      curr_offset = pred->next[l].load();
      curr = (Node*) offset_to_ptr(r, curr_offset);
      if (cmp_(curr, node) < 0) {
        pred = curr;
        continue;
      }
      break;
    }
//fprintf(stdout, "l=%d pred = %p\n", l, pred);
    preds[l] = pred;
    succs[l] = curr_offset;
  }
  if (l == 0) {
    if (pred == head_[r] && r != kPrimaryRegion) {
      pred = head_[kPrimaryRegion];
    }
    find_position_braided(td, node, preds, succs, pred);
  }
}

void lockfree_pskiplist::find_position_braided(ThreadData* const td, Node* node, Node* preds[], uint64_t succs[], Node* pred) {
  assert(pred != NULL);
  uint64_t curr_moff;
  Node* curr;
  while (true) {
    curr_moff = pred->next[0].load();
    curr = (Node*) moff_to_ptr(curr_moff);
    if (cmp_(curr, node) < 0) {
      pred = curr;
      continue;
    }
    break;
  }
//fprintf(stdout, "l=0 pred = %p\n", pred);
  preds[0] = pred;
  succs[0] = curr_moff;
}

lockfree_pskiplist::Node* lockfree_pskiplist::find_braided(ThreadData* const td, const Key& key, const Node* pred) {
  assert(pred != NULL);
  uint64_t curr_moff;
  Node* curr;
  int cr = -1;
  while (true) {
    curr_moff = pred->next[0].load();
    curr = (Node*) moff_to_ptr(curr_moff);
    if ((cr = cmp_(curr, key)) < 0) {
//td->visit_cnt++;
      pred = curr;
      continue;
    }
    break;
  }
  return (cr == 0) ? curr : NULL;
}

void* lockfree_pskiplist::offset_to_ptr(const int r, const uint64_t offset) {
  if (offset) {
    return (void*) ((uintptr_t) pop_[r] + offset);
  } else {
    return NULL;
  }
}

void* lockfree_pskiplist::moff_to_ptr(const uint64_t moff) {
  int16_t r = 0xffff & moff;
  uint64_t offset = moff >> 16;
  if (offset) {
    return (void*) ((uintptr_t) pop_[r] + offset);
  } else {
    return NULL;
  }
}

void* lockfree_pskiplist::moff_to_ptr(const uint64_t moff, int16_t& r) {
  r = 0xffff & moff;
  uint64_t offset = moff >> 16;
  if (offset) {
    return (void*) ((uintptr_t) pop_[r] + offset);
  } else {
    return NULL;
  }
}

int lockfree_pskiplist::random_height(Random& rnd) {
  // Random Height
  static const unsigned int kBranching = 4;
  const int bb = (kBranching > (unsigned int) kNumRegions) ? kBranching / kNumRegions : 1;
  int height = 1;
  if (height < kMaxHeight && ((rnd.Next() % bb) == 0)) {
    height++;
    while (height < kMaxHeight && ((rnd.Next() % kBranching) == 0)) {
      height++;
    }
  }
  return height;
}

std::shared_ptr<PRegionIterator> lockfree_pskiplist::new_region_iterator(const unsigned long rmask) {
  auto it = std::make_shared<PRegionIterator>(this, rmask);
  return it;
}

lockfree_pskiplist::Node* lockfree_pskiplist::get_node_by_offset(const int region, const uint64_t offset) {
  return (Node*) offset_to_ptr(region, offset);
}

lockfree_pskiplist::Node* lockfree_pskiplist::get_node_by_moff(const uint64_t moff) {
  return (Node*) moff_to_ptr(moff);
}

void lockfree_pskiplist::debug_scan() {
  int cnt = 0;
  //Node* pred = head_[kPrimaryRegion];
  Node* pred = head(kPrimaryRegion);
  fprintf(stdout, "{HEAD[0] @%p}", pred);
  while (true) {
    int16_t r;
    //uint64_t curr_moff = pred->next[0].load();
    //Node* curr = (Node*) moff_to_ptr(curr_moff, r);
    Node* curr = (Node*) moff_to_ptr(pred->next[0].load(), r);
    //int16_t r = 0xffff & curr_moff;
    //uint64_t curr_offset = curr_moff >> 16;
    if (curr) {
      cnt++;
      fprintf(stdout, "->{r:%d| KEY INFO NOT IMPLEMENTED |h:%d @%p}", r, curr->height, curr);
      //fprintf(stdout, "->{r:%d|k:%zu|h:%d @%p}", r, curr->key, curr->height, curr);
      pred = curr;
    } else {
      break;
    }
  }
  fprintf(stdout, "\nnode_cnt: %d\n", cnt);
}

PRegionIterator::PRegionIterator(lockfree_pskiplist* const skiplist, const unsigned long rmask) : skiplist_(skiplist), rbs_(rmask) {
  Node* node = skiplist_->head(kPrimaryRegion);
  assert(node != NULL);
  node_ = node;
  next();
}

bool PRegionIterator::valid() {
  return node_ != NULL;
}

void PRegionIterator::next() {
  Node* node = node_;
  int16_t node_region;
  do {
    node = (Node*) skiplist_->moff_to_ptr(node->next[0].load(), node_region);
  } while (node && !rbs_.test(node_region));
  r_ = node_region;
  node_ = node;
}

PRegionIterator::Node* PRegionIterator::node() {
  return node_;
}

int PRegionIterator::region() {
  return r_;
}

#if 1

// =========================================
// class casc_pskiplist_iterator
// -----------------------------------------
casc_pskiplist_iterator::casc_pskiplist_iterator(const int region, lockfree_pskiplist* const skiplist)
    : r_(region), skiplist_(skiplist) {
  //pred_ = NULL;
  node_ = NULL;
  shortcut_ = NULL;
  //pred_a_offset_ = 0;
}

void casc_pskiplist_iterator::seek(const Key& key, const Node* pred) {
  if (pred == NULL) {
    pred = skiplist_->head_[r_];
  }
  uint64_t curr_offset;
  Node* curr = NULL;
  int h = pred->height;
  for (int l = h - 1; l >= 1; l--) {
    while (true) {
      curr_offset = pred->next[l].load();
      curr = (Node*) skiplist_->offset_to_ptr(r_, curr_offset);
      if (skiplist_->cmp_(curr, key) < 0) {
        pred = curr;
        if (pred->type() == kTypeShortcut) {
//fprintf(stderr, "TEST found shortut! \n");
          //int16_t r;
          shortcut_ = (Node*) skiplist_->moff_to_ptr(pred->value());
        }
        continue;
      }
      break;
    }
  }
  if (pred == skiplist_->head_[r_] && r_ != kPrimaryRegion) {
    pred = skiplist_->head_[kPrimaryRegion];
  }
  return seek_braided(key, pred);
}

void casc_pskiplist_iterator::seek_braided(const Key& key, const Node* pred) {
  assert(pred != NULL);
  uint64_t curr_moff;
  Node* curr;
  int cr = 1;
  while (true) {
    curr_moff = pred->next[0].load();
    curr = (Node*) skiplist_->moff_to_ptr(curr_moff);
    if ((cr = skiplist_->cmp_(curr, key)) < 0) {
      pred = curr;
      continue;
    }
    break;
  }
  cmp_ = cr;
  //node_ = (cr == 0) ? curr : NULL;
  node_ = curr;
}

void casc_pskiplist_iterator::next() {
  Node* pred = node_;
  Node* curr = (Node*) skiplist_->moff_to_ptr(pred->next[0].load());
  cmp_ = 1;
  node_ = curr;
}

bool casc_pskiplist_iterator::valid() const {
  return (node_ != NULL);
}

int casc_pskiplist_iterator::cmp() {
  return cmp_;
}

//casc_pskiplist_iterator::Node* casc_pskiplist_iterator::pred() {
//  return pred_;
//}
casc_pskiplist_iterator::Node* casc_pskiplist_iterator::node() {
  return node_;
}

const casc_pskiplist_iterator::Node* casc_pskiplist_iterator::shortcut() {
  return shortcut_;
}

// shortcut V2
// shortcut info in every node
#if 0
lockfree_pskiplist::Node* lockfree_pskiplist::get_shortcut(const Node* node) {
  //return NULL;
  return (Node*) moff_to_ptr(node->shortcut_moff);
}

casc_pskiplist_iterator::casc_pskiplist_iterator(lockfree_pskiplist* const skiplist, const int region)
    : skiplist_(skiplist), r_(region) {
  pred_ = NULL;
  node_ = NULL;
  shortcut_ = NULL;
  pred_a_offset_ = 0;
}

void casc_pskiplist_iterator::seek(const Key& key, Node* pred) {
  if (pred == NULL) {
    pred = skiplist_->head_[r_];
  }
  uint64_t curr_offset;
  Node* curr = NULL;
  int h = pred->height;
  for (int l = h - 1; l >= 1; l--) {
    while (true) {
      curr_offset = pred->next[l].load();
      curr = (Node*) skiplist_->offset_to_ptr(r_, curr_offset);
      if (skiplist_->cmp_(curr, key) < 0) {
        if (auto sc = skiplist_->get_shortcut(curr)) {
          shortcut_ = sc;
        }
        pred = curr;
        continue;
      }
      break;
    }
  }

  pred_ = pred;
  return seek_braided(key, pred);
}

void casc_pskiplist_iterator::seek_two(const Key& key, Node* pred, const Key& key_a) {
  if (pred == NULL) {
    pred = skiplist_->head_[r_];
  }
  uint64_t curr_offset;
  Node* curr = NULL;
  int h = pred->height;
  //bool create_shortcut = (std::rand() % 10 == 0);
  bool create_shortcut = true;
  for (int l = h - 1; l >= 1; l--) {
    while (true) {
      curr_offset = pred->next[l].load();
      curr = (Node*) skiplist_->offset_to_ptr(r_, curr_offset);
      if (create_shortcut && l >= kMaxHeight - 6/*or SHORTCUT_CREATION_CRITERIA*/) {
        if (skiplist_->cmp_(curr, key_a) < 0) {
          pred_a_offset_ = curr_offset;
        }
      }
      if (skiplist_->cmp_(curr, key) < 0) {
        if (auto sc = skiplist_->get_shortcut(curr)) {
          shortcut_ = sc;
        }
        pred = curr;
        continue;
      }
      break;
    }
  }
  pred_ = pred;
  return seek_braided(key, pred);
}

void casc_pskiplist_iterator::seek_braided(const Key& key, Node* pred) {
  assert(pred != NULL);
  uint64_t curr_moff;
  Node* curr;
  int cr = 1;
  while (true) {
    curr_moff = pred->next[0].load();
    curr = (Node*) skiplist_->moff_to_ptr(curr_moff);
    if ((cr = skiplist_->cmp_(curr, key)) < 0) {
      pred = curr;
      continue;
    }
    break;
  }
  cmp_ = cr;
  node_ = curr;
}

bool casc_pskiplist_iterator::is_ready_new_shortcut() {
  return (pred_a_offset_ != 0);
}

uint64_t casc_pskiplist_iterator::pred_a_moff() {
  return (pred_a_offset_<<16 | r_);
}

bool casc_pskiplist_iterator::valid() {
  return (node_ != NULL);
}

int casc_pskiplist_iterator::cmp() {
  return cmp_;
}

casc_pskiplist_iterator::Node* casc_pskiplist_iterator::pred() {
  return pred_;
}
casc_pskiplist_iterator::Node* casc_pskiplist_iterator::node() {
  return node_;
}

casc_pskiplist_iterator::Node* casc_pskiplist_iterator::shortcut() {
  return shortcut_;
}
#endif
#endif
