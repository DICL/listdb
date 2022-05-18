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
  using PmemNode = BraidedPmemSkipList::Node;
  static const uint16_t kMaxHeight_ = kSkipListCacheMaxHeight;
  static const uint16_t kBranching_ = kSkipListCacheBranching;

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
      assert(IsFull() == false);
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

    void MaybeShiftDeleteFieldByPosition(unsigned int pos) {
      assert(pos != 0);
      while (pos < N - 1 && !fields[pos + 1].IsEmpty()) {
        fields[pos] = fields[pos + 1];
        pos++;
      }
      fields[pos].Reset();
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
  SkipListCache(const int pool_id, size_t capacity = kSkipListCacheCapacity);

  // Not thread-safe.
  // Only one background worker thread calls this.
  int Insert(PmemNode* const p);

  PmemNode* LookupLessThan(const Key& key);

  // Returns 0 if equal, -1 lessthan, 1 not found
  int LookupLessThanOrEqualsTo(const Key& key, PmemNode** out);

  void GetDebugString(const std::string& name, std::string* buf);

  size_t AcquireLoadSize() { return size_.load(std::memory_order_acquire); }

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

  int EvictSome(int height_upper);

  const int pool_id_;
  const size_t capacity_;
  Node* head_;
  // NOTE: size_ and capacity_ must represent used and free memory space respectively.
  // Since this requires a Merge operation, which is not implemented yet, for now, size_ represents the number of fields
  // occupied regardless of the actual memory consumption.
  // TODO(wkim): Impl. Merge and change the semantic of size and capacity.
  std::atomic<size_t> size_;
  std::vector<Cursor> smallest_cursor_;
  //std::mutex mu_;
};

template <std::size_t N>
SkipListCache<N>::SkipListCache(const int pool_id, size_t capacity)
  : pool_id_(pool_id),
    capacity_(capacity),
    head_(NewNode(uint64_t{0}, kMaxHeight_)),
    size_(0),
    smallest_cursor_(kMaxHeight) {
  for (int i = 0; i < kMaxHeight_; i++) {
    head_->next[i].store(nullptr, std::memory_order_relaxed);
  }
  std::atomic_thread_fence(std::memory_order_release);
}

template <std::size_t N>
int SkipListCache<N>::Insert(PmemNode* const p) {
#ifdef CACHE_SIZE_IS_FIELD_COUNT
  size_t curr_size = size_.load(std::memory_order_acquire);
  if (curr_size + sizeof(Field) > capacity_) {
    int num_evicted = EvictSome(p->height());
    //fprintf(stdout, "Full. evict_cnt=%d\n", num_evicted);
    if (num_evicted == 0) {
      // Full. Nothing to evict
      return 1;
    }
  }
#endif

  Key& key = p->key;

  // Insert new node into SkipList
  Node* preds[kMaxHeight_];
  Node* succs[kMaxHeight_];

  Node* n = FindPosition(key, preds, succs);
  if (n == nullptr) {
    // Create a new node
    int height = RandomHeight();
    Node* x = NewNode(key, height, p);
    for (int i = 0; i < height; i++) {
      x->next[i].store(succs[i], std::memory_order_relaxed);
      preds[i]->next[i].store(x, std::memory_order_release);
    }
#ifdef CACHE_SIZE_IS_FIELD_COUNT
    MaybeUpdateCursor(p->height(), key, x);
    size_.fetch_add(sizeof(Field));
#endif
  } else if (!n->IsFull()) {
    n->MaybeShiftInsertField(key, p->height(), PmemPtr::OffsetOfVaddr(pool_id_, p));
    MaybeUpdateCursor(p->height(), key, n);
#ifdef CACHE_SIZE_IS_FIELD_COUNT
    size_.fetch_add(sizeof(Field));
#endif
  } else {
    // Split
    //fprintf(stdout, "split\n");

    // Determine a split key
    // preds -> A{ split_key } -> n{ old_key }
    auto& split_field = n->fields[N/2];
    auto& split_key = split_field.key;
    PmemNode* split_pnode = DecodeFieldValue(split_field);

    // Create a new node
    int height = RandomHeight();
#ifdef CACHE_SIZE_IS_FIELD_COUNT
    size_.fetch_add(sizeof(Field));
#else
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
  }
  return 0;
}

template <std::size_t N>
typename SkipListCache<N>::PmemNode* SkipListCache<N>::LookupLessThan(const Key& key) {
  Node* preds[kMaxHeight_];
  Node* n = FindPosition(key, preds, nullptr);
  if (n != nullptr) {
    for (unsigned int i = 0; i < N; i++) {
      if (n->fields[i].IsEmpty()) {
        break;
      }
      if (n->fields[i].key.Compare(key) < 0) {
        return DecodeFieldValue(n->fields[i]);
      }
    }
  }
  if (preds[0] != head_) {
    n = preds[0];
    return DecodeFieldValue(n->fields[0]);
  }
  return nullptr;
}

//#define SKIPLIST_CACHE_BINARY_SEARCH
template <std::size_t N>
int SkipListCache<N>::LookupLessThanOrEqualsTo(const Key& key, PmemNode** out) {
  Node* preds[kMaxHeight_];
  Node* n = FindPosition(key, preds, nullptr);
  if (n != nullptr) {
#ifndef SKIPLIST_CACHE_BINARY_SEARCH
    for (unsigned int i = 0; i < N; i++) {
      if (n->fields[i].IsEmpty()) {
        break;
      }
      int cmp = n->fields[i].key.Compare(key);
      if (cmp == 0) {
        *out = DecodeFieldValue(n->fields[i]);
        return 0;
      } else if (cmp < 0) {
        *out = DecodeFieldValue(n->fields[i]);
        return -1;
      }
    }
#else
    unsigned int pa = 0;
    unsigned int pb = N;
    unsigned int i;
    unsigned int last_lt_pos = N;
    while (pa < pb) {
      i = (pb + pa) / 2;
      if (n->fields[i].IsEmpty()) {
        pb = i;
        continue;
      }
      int cmp = n->fields[i].key.Compare(key);
      if (cmp == 0) {
        *out = DecodeFieldValue(n->fields[i]);
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
    if (last_lt_pos < N) {
      *out = DecodeFieldValue(n->fields[last_lt_pos]);
      return -1;
    }
#endif
  }
  if (preds[0] != head_) {
    n = preds[0];
    *out = DecodeFieldValue(n->fields[0]);
    return -1;
  }
  *out = nullptr;
  return 1;
}
#undef SKIPLIST_CACHE_BINARY_SEARCH

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
  while (height < kMaxHeight_ && rnd->Next() % kBranching_ == 0) {
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
int SkipListCache<N>::EvictSome(int height_upper) {
  // Evict

  static const unsigned int kEvictionBatchSize = 4;
  unsigned int eviction_cnt = 0;
  int curr_height = 0;
  bool collection_done = false;
  // evict lowest heights first
  while (curr_height < height_upper && !collection_done) {
    if (smallest_cursor_[curr_height].IsEmpty()) {
      curr_height++;
      continue;
    }
    std::vector<std::pair<Node*, int>> victims;
    Node* x = smallest_cursor_[curr_height].node.load(std::memory_order_acquire);
    while (x != nullptr && !collection_done) {
      // TODO(wkim): Split node when field[pos = 0] should be evicted
      // Since fields[pos = 0] contains node key, we need to split the node to evict fields[pos = 0]
      // For now, we don't evict the node key. Collect the victim infos upto pos = 1.
      unsigned int pos = N - 1;
      while (pos > 0) {
        if (!x->fields[pos].IsEmpty() && x->fields[pos].height() - 1 == curr_height) {
          //fprintf(stdout, "cursor points to node key (curr_height=%d)\n", curr_height);
          victims.emplace_back(x, pos);
          if (eviction_cnt + victims.size() > kEvictionBatchSize) {
            collection_done = true;
            break;
          }
        }
        pos--;
      }
      x = x->next[0].load(std::memory_order_acquire);
    }
    // Update cursor
    if (victims.size() > kEvictionBatchSize - eviction_cnt) {
      auto& new_smallest = victims.back();
      Node* new_smallest_node = new_smallest.first;
      int new_smallest_field_pos = new_smallest.second;
      smallest_cursor_[curr_height].Set(new_smallest_node->fields[new_smallest_field_pos].key, new_smallest_node);
      victims.pop_back();
    } else {
      smallest_cursor_[curr_height].Reset();
    }
    // Drop victims
    auto it = victims.begin();
    while (it != victims.end()) {
      std::vector<int> positions;
      Node* x = it->first;
      while (it != victims.end()) {
        if (it->first == x) {
          positions.push_back(it->second);
        }
        ++it;
      }
      for (auto& pos : positions) {
        x->MaybeShiftDeleteFieldByPosition(pos);
      }
    }
    eviction_cnt += victims.size();
    curr_height++;
  }

  size_.fetch_sub(eviction_cnt * sizeof(Field));

  return eviction_cnt;
}

template <std::size_t N>
void SkipListCache<N>::GetDebugString(const std::string& name, std::string* buf) {
  std::stringstream ss;
  if (name == "cursor") {
    for (int i = 0; i < kMaxHeight_; i++) {
      ss << i + 1 << ": " << smallest_cursor_[i].key.key_num() << std::endl;
    }
    buf->assign(std::move(ss.str()));
  }
}

#endif  // LISTDB_CORE_SKIPLIST_CACHE_H_
