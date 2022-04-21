#ifndef LISTDB_CORE_SKIPLIST_CACHE_H_
#define LISTDB_CORE_SKIPLIST_CACHE_H_

#include "listdb/common.h"
#include "listdb/index/braided_pmem_skiplist.h"
#include "listdb/util.h"
#include "listdb/util/random.h"

template <std::size_t N>
class SkipListCache {
 public:
  using PmemNode = BraidedPmemSkipList::Node;
  static const uint16_t kMaxHeight_ = kSkipListCacheMaxHeight;
  static const uint16_t kBranching_ = kSkipListCacheBranching;
  static const uint32_t kScaledInverseBranching_ = kSkipListCacheScaledInverseBranching;

  struct Field {
    Key key;
    std::atomic<uint64_t> value;  // {L0 node height, pmem address offset}

    Field() : key(0), value(0) { }
    //Field(const Key& k, int h, uint64_t o) : key(k), value((static_cast<uint64_t>(h) << 48) | o) { }

    void Set(const Key& k, int height, uint64_t offset) {
      key = k;
      //value = (static_cast<uint64_t>(h) << 48) | offset;
      value.store((static_cast<uint64_t>(height) << 48) | offset, std::memory_order_release);
    }

    void Reset() {
      value.store(0, std::memory_order_release);
    }

    bool IsEmpty() {
      return value.load(std::memory_order_acquire) == 0;
    }

    uint16_t height() const { return (value >> 48); }
    uint64_t offset() const {
      static const uint64_t kMask = 0x0000ffffffffffff;
      return (value & kMask);
    }
  };

  struct Node {
    //Key key_end;
    uint64_t tag;  // height (8-bit)
    Field fields[N];  // descending order
    std::atomic<Node*> next[1];
    
    explicit Node(const Key& k, int height, int pnode_height, uint64_t offset) {
      tag = static_cast<uint16_t>(height);
      fields[0].Set(k, pnode_height, offset);
    }
    Key* key() { return &fields[0].key; }
    uint16_t height() const { return tag & 0xff; }

    bool IsFull() { return fields[N - 1].value != 0; }

    void InsertField(const Key& key, int pnode_height, uint64_t offset) {
      int pos = 0;
      while (pos < N) {
        if (!fields[pos].IsEmpty() && fields[pos].key.Compare(key) >= 0) {
          pos++;
        } else {
          break;
        }
      }
      assert(pos < N);
      int pos2 = pos;
      while (pos2 < N) {
        if (fields[pos2].IsEmpty()) {
          break;
        }
        pos2++;
      }
      while (pos2 > pos) {
        fields[pos2].key = fields[pos].key;
        fields[pos2].value.store(fields[pos].value.load(), std::memory_order_release);
        pos2--;
      }
      fields[pos].Set(key, pnode_height, offset);
    }
  };

  // Constructor
  SkipListCache(const int pool_id);

  // Not thread-safe.
  // Only one background worker thread calls this.
  int Insert(PmemNode* const p);

  PmemNode* Lookup(const Key& key);

 private:
  Node* NewNode(const Key& key, const int height, PmemNode* p = nullptr);

  int RandomHeight();

  PmemNode* DecodeFieldValue(const Field& f) {
    return PmemPtr::Compose<PmemNode>(pool_id_, f.offset());
  }

  bool KeyIsAfterNode(const Key& key, Node* n) const;

  Node* FindPosition(const Key& key, Node* preds[], Node* succs[]);

  const int pool_id_;
  Node* head_;
  size_t size_ = 0;
  size_t capacity_ = kSkipListCacheCapacity;
  std::mutex mu_;
};

template <std::size_t N>
SkipListCache<N>::SkipListCache(const int pool_id)
  : pool_id_(pool_id),
    head_(NewNode(uint64_t{0}, kMaxHeight_)) {
  for (int i = 0; i < kMaxHeight_; i++) {
    head_->next[i].store(nullptr, std::memory_order_relaxed);
  }
  std::atomic_thread_fence(std::memory_order_release);
}

template <std::size_t N>
int SkipListCache<N>::Insert(PmemNode* const p) {
  Key& key = p->key;

  // Insert new node into SkipList
  Node* preds[kMaxHeight_];
  Node* succs[kMaxHeight_];
  //while (true) {
    Node* n = FindPosition(key, preds, succs);
    if (n == nullptr) {
      // Create a new node
      int height = RandomHeight();
      Node* x = NewNode(key, height, p);
      for (int i = 0; i < height; i++) {
        x->next[i].store(succs[i], std::memory_order_relaxed);
        preds[i]->next[i].store(x, std::memory_order_release);
      }
    } else if (!n->IsFull()) {
      n->InsertField(key, p->height(), PmemPtr::OffsetOfVaddr(pool_id_, p));
    } else if (size_ + N * sizeof(Field) <= capacity_) {
      // Split
      //
      fprintf(stdout, "split\n");

      // Determine a split key
      // preds -> A{ split_key } -> n{ old_key }
      auto& split_field = n->fields[(N - 1) / 2];
      auto& split_key = split_field.key;
      PmemNode* split_pnode = DecodeFieldValue(split_field);

      // Create a new node
      int height = RandomHeight();
      Node* x = NewNode(split_key, height, split_pnode);  // TODO(wkim): Impl. NewNode(Field&, height)
      for (int i = 0; i < height; i++) {
        //x->next[i].store(preds[i]->next[i].load(std::memory_order_relaxed), std::memory_order_relaxed);
        x->next[i].store(succs[i], std::memory_order_relaxed);
        preds[i]->next[i].store(x, std::memory_order_release);
      }

      // Cleanup
      for (int i = N - 1; i >= (N - 1) / 2; i--) {
        n->fields[i].Reset();
      }

      if (key.Compare(split_key) > 0) {
        n->InsertField(key, p->height(), PmemPtr::OffsetOfVaddr(pool_id_, p));
      } else {
        x->InsertField(key, p->height(), PmemPtr::OffsetOfVaddr(pool_id_, p));
      }
    } else {
      // Evict
      return 1;
    }
    //break;
  //}
  return 0;
}

template <std::size_t N>
typename SkipListCache<N>::PmemNode* SkipListCache<N>::Lookup(const Key& key) {
  Node* preds[kMaxHeight_];
  Node* n = FindPosition(key, preds, nullptr);
  for (int i = 0; i < N; i++) {
    if (n->fields[i].IsEmpty()) {
      break;
    }
    if (n->fields[i].key.Compare(key) < 0) {
      return DecodeFieldValue(n->fields[i]);
    }
  }
  // TODO(wkim): make cache head points to the L0 head
  if (preds[0] != head_) {
    n = preds[0];
    return DecodeFieldValue(n->fields[0]);
  } else {
    return nullptr;
  }
}

template <std::size_t N>
typename SkipListCache<N>::Node* SkipListCache<N>::NewNode(const Key& key, const int height, PmemNode* p) {
  size_t node_size = util::AlignedSize(8, sizeof(Node) + (height - 1) * 8);
  void* buf = aligned_alloc(8, node_size);
  Node* node;
  if (p != nullptr) {
    node = new (buf) Node(key, height, p->height(), PmemPtr::OffsetOfVaddr(pool_id_, p));
  } else {
    node = new (buf) Node(key, height, 0, 0);
  }
  return node;
}

template <std::size_t N>
int SkipListCache<N>::RandomHeight() {
  auto rnd = Random::GetTLSInstance();
  // Increase height with probability 1 in kBranching
  int height = 1;
  while (height < kMaxHeight_ && rnd->Next() < kScaledInverseBranching_) {
    height++;
  }
  return height;
}

template <std::size_t N>
bool SkipListCache<N>::KeyIsAfterNode(const Key& key, Node* n) const {
  // nullptr n is considered infinite
  return (n != nullptr) && (n->key()->Compare(key) < 0);
}

template <std::size_t N>
typename SkipListCache<N>::Node* SkipListCache<N>::FindPosition(const Key& key, Node* preds[], Node* succs[]) {
  Node* x = head_;
  int level = kMaxHeight_ - 1;
  Node* last_not_after = nullptr;
  while (true) {
    Node* next = x->next[level].load(std::memory_order_relaxed);
    if (next != last_not_after && KeyIsAfterNode(key, next)) {
      x = next;
    } else {
      if (preds != nullptr) {
        preds[level] = x;
      }
      if (succs != nullptr) {
        succs[level] = next;
      }
      if (level == 0) {
        return next;
      } else {
        // Switch to next list, reuse KeyIsAfterNode() result
        last_not_after = next;
        level--;
      }
    }
  }
}

#endif  // LISTDB_CORE_SKIPLIST_CACHE_H_
