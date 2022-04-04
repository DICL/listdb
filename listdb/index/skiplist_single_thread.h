#ifndef LISTDB_INDEX_SKIPLIST_H_
#define LISTDB_INDEX_SKIPLIST_H_

class SkipList {
 public:
  struct Node {
    Key k;       // integer value or offset
    uint64_t t;       // tag
    uint64_t v;     // integer value or offset, (SHORTCUT: pointer to next memtable node)
    uint64_t log_moff;  // log marked offset, (SHORTCUT: moff to upper pmem node)
    int height;
    Node* next[1];

    static uint64_t head_key() { return 0ULL; }

    static size_t compute_alloc_size(const Key& key, const int height) {
      return aligned_size(8, sizeof(Node) + (height-1)*8);
    }

    static Node* init_node(char* buf, const Key& key,
                           const uint64_t tag, const uint64_t value,
                           const int height, bool init_next_arr = true) {
      Node* node = (Node*) buf;
      node->k = key;
      node->t = tag;
      node->v = value;
      node->height = height;
      if (init_next_arr) {
        memset(node->next, 0, height * 8);
      }
      return node;
    }

    size_t alloc_size() const { return sizeof(Node) + (height-1)*8; }
    Key key() const { return k; }
    uint64_t tag() const { return t; }
    uint8_t type() const { return ValueType(t & 0xff); }
    uint64_t value() const { return v; }
    char* data() const { return (char*) this; }
  };

  SkipList();
  // Returns pred
  Node* insert(Node* const node, Node* pred = NULL);
  // Returns (node->key == key) ? node : NULL
  Node* find(const Key& key, const Node* pred = NULL);
  Node* head();

 private:
  void find_position(Node* node, Node* preds[], Node* succs[], Node* pred = NULL, const int min_h = 0);

 public:
  Node* head_;
};

SkipList::SkipList() {
  auto head_key = Node::head_key();
  const size_t alloc_size = Node::compute_alloc_size(head_key, kMaxHeight);
  void* buf = aligned_alloc(8, alloc_size);
  //void* buf = malloc(alloc_size);
  head_ = Node::init_node((char*) buf, head_key, 0xffffffffffffffff, 0, kMaxHeight);
  head_->log_moff = 0x000000000000ffff;  // offset: 0, region: -1
}

SkipList::Node* SkipList::insert(Node* const node, Node* pred)  {
  if (pred == NULL) {
    pred = head_;
  }
  Node* preds[kMaxHeight];
  Node* succs[kMaxHeight];
  find_position(node, preds, succs, pred);
  for (int l = 0; l < node->height; l++) {
    node->next[l] = succs[l];
  }
  for (int l = 0; l < node->height; l++) {
    preds[l]->next[l] = node;
  }
  return preds[kMaxHeight - 1];
}

SkipList::Node* SkipList::find(const Key& key, const Node* pred) {
  if (pred == NULL) {
    pred = head_;
  }
  Node* curr;
  int h = pred->height;
  int cr;
  for (int l = h - 1; l >= 0; l--) {
    while (true) {
      curr = pred->next[l];
      if ((cr = curr->k.Compare(key)) < 0) {
        pred = curr;
        continue;
      }
      break;
    }
  }
  return (cr == 0) ? curr : NULL;
}

SkipList::Node* SkipList::head() {
  return head_;
}

void SkipList::find_position(Node* node, Node* preds[], Node* succs[], Node* pred, const int min_h) {
  if (pred == NULL) {
    pred = head_;
  }
  Node* curr;
  int h = pred->height;
  for (int l = h - 1; l >= min_h; l--) {
    while (true) {
      curr = pred->next[l];
      if (curr && curr->k.Compare(node->k) < 0) {
        pred = curr;
        continue;
      }
      break;
    }
    preds[l] = pred;
    succs[l] = curr;
  }
}

#endif  // LISTDB_INDEX_SKIPLIST_H_
