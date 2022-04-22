#ifndef LISTDB_CORE_SKIPLIST_CACHE_H_
#define LISTDB_CORE_SKIPLIST_CACHE_H_

#include <sstream>

#include "listdb/common.h"
#include "listdb/index/braided_pmem_skiplist.h"
#include "listdb/util.h"
#include "listdb/util/random.h"

// TODO(wkim): Undefine this after doing the relevant works. Refer `SkipListCache::size_`
#define CACHE_SIZE_IS_MEMORY_FOOTPRINT

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

  struct Node {
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

    void MaybeShiftInsertField(const Key& key, int pnode_height, uint64_t offset) {
      unsigned int pos = 0;
      while (pos < N) {
        if (!fields[pos].IsEmpty() && fields[pos].key.Compare(key) >= 0) {
          pos++;
        } else {
          break;
        }
      }
      assert(pos < N);
      unsigned int pos2 = pos;
      while (pos2 < N) {
        if (fields[pos2].IsEmpty()) {
          break;
        }
        pos2++;
      }
      while (pos2 > pos) {
        fields[pos2] = fields[pos];
        //fields[pos2].key = fields[pos].key;
        //fields[pos2].value.store(fields[pos].value.load(), std::memory_order_release);
        pos2--;
      }
      fields[pos].Set(key, pnode_height, offset);
    }
  };

  struct Cursor {
    Key key;
    std::atomic<Node*> node;

    Cursor() : key(0), node(nullptr) { }

    void Set(const Key& k, Node* n) {
      key = k;
      node.store(n, std::memory_order_release);
    }

    void Reset() {
      node.store(nullptr, std::memory_order_release);
    }

    bool IsEmpty() {
      return node.load(std::memory_order_acquire) == 0;
    }
  };

  // Constructor
  SkipListCache(const int pool_id);

  // Not thread-safe.
  // Only one background worker thread calls this.
  int Insert(PmemNode* const p);

  PmemNode* LookupLessThan(const Key& key);

  void GetDebugString(const std::string& name, std::string* buf);

 private:
  Node* NewNode(const Key& key, const int height, PmemNode* p = nullptr);

  int RandomHeight();

  PmemNode* DecodeFieldValue(const Field& f) {
    return PmemPtr::Compose<PmemNode>(pool_id_, f.offset());
  }

  bool KeyIsAfterNode(const Key& key, Node* n) const;

  Node* FindPosition(const Key& key, Node* preds[], Node* succs[]);

  void MaybeMoveCursor(Node* node, unsigned int begin, unsigned int end, Node* move_to);

  void MaybeUpdateCursor(int height, const Key& key, Node* node);

  const int pool_id_;
  Node* head_;
  // NOTE: size_ and capacity_ must represent used and free memory space respectively.
  // Since this requires a Merge operation, which is not implemented yet, for now, size_ represents the number of fields
  // occupied regardless of the actual memory consumption.
  // TODO(wkim): Impl. Merge and change the semantic of size and capacity.
  std::atomic<size_t> size_;
  size_t capacity_ = kSkipListCacheCapacity;
  Cursor smallest_cursor_[kMaxHeight_];
  std::mutex mu_;
};

template <std::size_t N>
SkipListCache<N>::SkipListCache(const int pool_id)
  : pool_id_(pool_id),
    head_(NewNode(uint64_t{0}, kMaxHeight_)),
    size_(0) {
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
    size_t curr_size = size_.load(std::memory_order_acquire);
    Node* n = FindPosition(key, preds, succs);
    if (n == nullptr) {
      // Create a new node
      int height = RandomHeight();
      Node* x = NewNode(key, height, p);
      for (int i = 0; i < height; i++) {
        x->next[i].store(succs[i], std::memory_order_relaxed);
        preds[i]->next[i].store(x, std::memory_order_release);
      }
      MaybeUpdateCursor(p->height(), key, x);
    } else if (!n->IsFull()) {
      n->MaybeShiftInsertField(key, p->height(), PmemPtr::OffsetOfVaddr(pool_id_, p));
      MaybeUpdateCursor(p->height(), key, n);
    } else if (curr_size + sizeof(Field) <= capacity_) {
      // Split
      fprintf(stdout, "split\n");

      //for (int i = 0; i < kMaxHeight_; i++) {
      //  if (smallest_cursor_[i].node == n) {
      //  }

      // Determine a split key
      // preds -> A{ split_key } -> n{ old_key }
      auto& split_field = n->fields[N/2];
      auto& split_key = split_field.key;
      PmemNode* split_pnode = DecodeFieldValue(split_field);

      // Create a new node
      int height = RandomHeight();
#ifndef CACHE_SIZE_IS_MEMORY_FOOTPRINT
      size_t node_size = util::AlignedSize(8, sizeof(Node) + (height - 1) * 8);
      if (curr_size + node_size > capacity_) {
        size_t available_space_for_height = capacity_ - (curr_size + (sizeof(Node) - 8));
        height = available_space_for_height / 8;
        node_size = util::AlignedSize(8, sizeof(Node) + (height - 1) * 8);
        while (curr_size + node_size > capacity_ && height > 1) {
          height--;
          node_size = util::AlignedSize(8, sizeof(Node) + (height - 1) * 8);
        }
      }
      size_.fetch_add(node_size, std::memory_order_release);
#else
      size_.fetch_add(sizeof(Field));
#endif
      Node* x = NewNode(split_key, height, split_pnode);  // TODO(wkim): Impl. NewNode(Field&, height)

      bool insert_to_new_node = key.Compare(split_key) <= 0;
      // Copy fields having the key < split_key
      // smallest_cursor_ may be updated if a cursor points to the node containing the field to move to the new node.
      int pos = 1;
      if (insert_to_new_node) {
        for (unsigned int i = N/2 + 1; i < N; i++) {
          // Insertion in descending order prevents field shifts.
          if (UNLIKELY(n->fields[i].key.Compare(key) < 0)) {
            x->fields[pos++].Set(key, p->height(), PmemPtr::OffsetOfVaddr(pool_id_, p));
          }
          x->fields[pos++] = n->fields[i];
        }
      } else {
        for (unsigned int i = N/2 + 1; i < N; i++) {
          // Insertion in descending order prevents field shifts.
          x->fields[pos++] = n->fields[i];
        }
      }
      MaybeMoveCursor(n, N/2 + 1, N, x);

      // Link the newely created node
      for (int i = 0; i < height; i++) {
        x->next[i].store(succs[i], std::memory_order_relaxed);
        preds[i]->next[i].store(x, std::memory_order_release);
      }
      // Cleanup
      for (unsigned int i = N-1; i >= N/2; i--) {
        n->fields[i].Reset();
      }
      if (insert_to_new_node) {
        MaybeUpdateCursor(p->height(), key, x);
      } else {
        n->MaybeShiftInsertField(key, p->height(), PmemPtr::OffsetOfVaddr(pool_id_, p));
        MaybeUpdateCursor(p->height(), key, n);
      }
    } else {
      // Evict
      fprintf(stdout, "evict\n");

      // Select victims
      int pnode_height = p->height();
      
      // evict smallest heights first

      return 1;
    }
    //break;
  //}
  return 0;
}

template <std::size_t N>
typename SkipListCache<N>::PmemNode* SkipListCache<N>::LookupLessThan(const Key& key) {
  Node* preds[kMaxHeight_];
  Node* n = FindPosition(key, preds, nullptr);
  for (unsigned int i = 0; i < N; i++) {
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

template <std::size_t N>
void SkipListCache<N>::MaybeMoveCursor(Node* node, unsigned int begin, unsigned int end, Node* move_to) {
  for (int i = 0; i < kMaxHeight_; i++) {
    if (smallest_cursor_[i].node.load(std::memory_order_acquire) == node) {
      unsigned int pos = begin; 
      while (pos < end) {
        if (node->fields[pos].IsEmpty()) {
          break;
        }
        int cmp = node->fields[pos].key.Compare(smallest_cursor_[i].key);
        if (cmp < 0) {
          break;
        } else if (cmp == 0) {
          smallest_cursor_[i].node.store(move_to, std::memory_order_relaxed);
          break;
        }
        pos++;
      }
    }
  }
}

template <std::size_t N>
void SkipListCache<N>::MaybeUpdateCursor(int height, const Key& key, Node* node) {
  if (smallest_cursor_[height].IsEmpty() || smallest_cursor_[height].key.Compare(key) > 0) {
    smallest_cursor_[height].Set(key, node);
  }
}

template <std::size_t N>
void SkipListCache<N>::GetDebugString(const std::string& name, std::string* buf) {
  std::stringstream ss;
  if (name == "cursor") {
    for (int i = 0; i < kMaxHeight_; i++) {
      ss << i << ": " << smallest_cursor_[i].key.key_num() << std::endl;
    }
    buf->assign(std::move(ss.str()));
  }
}

#endif  // LISTDB_CORE_SKIPLIST_CACHE_H_
