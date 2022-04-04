#ifndef ARENA_H_
#define ARENA_H_

#include <atomic>
#include <mutex>

class Arena {
  struct Block {
    std::atomic<size_t> p;
    std::atomic<Block*> next;
    char data[1];
    Block() : p(0), next(NULL) { }
  };
 public:
  Arena(const size_t block_size) : head_(NULL), curr_(NULL), block_size_(block_size) { }
  ~Arena() {
    Block* b = head_;
    while (b) {
      auto nb = b->next.load();
      delete b;
      b = nb;
    }
  }

  char* allocate(const size_t size) {
    char* ret;
    Block* nb = NULL;
    size_t before;

    int mod = size & 7;
    int slop = (mod == 0) ? 0 : 8 - mod;
    size_t asize = size + slop;

    auto block = curr_.load();
    while (true) {
      if (block) {
        if ((before = block->p.fetch_add(asize)) <= block_size_ - asize) {
          break;
        } else {
          block = block->next.load();
          continue;
        }
      } else {
        nb = new_block();
        nb->p.store(asize);
        nb->next.store(NULL);
        block = nb;
        before = 0;
        break;
      }
    }

    if (nb) {
      std::lock_guard<std::mutex> guard(mu_);
      if (!head_) {
        head_ = nb;
        curr_.store(nb);
      } else {
        Block* pred = head_;
        while (true) {
          Block* pn = pred->next.load();
          if (pn) {
            pred = pn;
          } else {
            break;
          }
        }
        pred->next.store(nb);

        Block* curr = curr_.load();
        while (curr->p >= block_size_) {
          curr_.store(curr->next);
          curr = curr_.load();
        }
      }
    }
    ret = block->data + before;
    return ret;
  }

 private:
  Block* new_block() {
    void* buf = aligned_alloc(8, sizeof(Block) + (block_size_ - 1));
    return (Block*) buf;
  }

 private:
  Block* head_;
  std::atomic<Block*> curr_;
  const size_t block_size_;
  std::mutex mu_;
};

#endif  // ARENA_H_
