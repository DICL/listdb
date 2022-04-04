#include <chrono>
#include <cstdlib>
#include <memory>
#include <thread>
#include <vector>
#include <mutex>

#include "listdb/common.h"
#include "listdb/lsm/table.h"
#include "listdb/lsm/table_list.h"
#include "listdb/index/lockfree_skiplist.h"

class TableSp {
 public:
  TableSp(const size_t c) : capacity_(c), size_(0), next_(nullptr) { }
  void* Put(const Key& key, const Value& value) { table_.emplace(key, value); return nullptr; }
  void SetNext(std::shared_ptr<TableSp> next) { next_ = next; }
  std::shared_ptr<TableSp> Next() {
    return next_;
  }
  bool HasRoom(const size_t size) {
    const size_t before = size_.fetch_add(size);
    return (before + size <= capacity_);
  }
 private:
  const size_t capacity_;
  std::atomic<size_t> size_;
  std::shared_ptr<TableSp> next_;
  std::map<Key, Value> table_;
};

template <class T>
class TableListSharedMutex {
 public:  
  TableListSharedMutex(const size_t table_capacity) : table_capacity_(table_capacity), front_(nullptr) {
  }
  void* Put(const Key& key, const Value& value) {
    const size_t kv_size = key.size() + 8;
    auto table = GetMutable(kv_size);
    table->Put(key, value);
    return nullptr;
  }
  void PrintDebug() {
    auto curr = front_;
    while (curr != nullptr) {
      fprintf(stderr, "[%p]->", curr.get());
      //asm volatile("");
      curr = curr->Next();
    }
    fprintf(stderr, "\n");
  }

 private:
  std::shared_ptr<T> GetFront() {
    auto ret = front_;
    if (ret == nullptr) {
      std::lock_guard<std::mutex> lk(init_mu_);
      ret = front_;
      if (ret == nullptr) {
        ret = std::make_shared<T>(table_capacity_);
        front_ = ret;
      }
    }
    return ret;
  }
  std::shared_ptr<T> GetMutable(const size_t size) {
    auto ret = GetFront();
    if (!ret->HasRoom(size)) {
      std::lock_guard<std::mutex> lk(init_mu_);
      ret = front_;
      if (!ret->HasRoom(size)) {
        auto new_table = std::make_shared<T>(table_capacity_);
        new_table->SetNext(ret);
        ret = new_table;
        front_ = ret;
      }
    }
    return ret;
  }

  const size_t table_capacity_;
  std::mutex init_mu_;
  std::shared_ptr<T> front_;
};

class Random {
 private:
  uint32_t seed_;
 public:
  explicit Random(uint32_t s) : seed_(s & 0x7fffffffu) {
    // Avoid bad seeds.
    if (seed_ == 0 || seed_ == 2147483647L) {
      seed_ = 1;
    }
  }
  uint32_t Next() {
    static const uint32_t M = 2147483647L;   // 2^31-1
    static const uint64_t A = 16807;  // bits 14, 8, 7, 5, 2, 1, 0
    // We are computing
    //       seed_ = (seed_ * A) % M,    where M = 2^31-1
    //
    // seed_ must not be zero or M, or else all subsequent computed values
    // will be zero or M respectively.  For all other values, seed_ will end
    // up cycling through every number in [1,M-1]
    uint64_t product = seed_ * A;

    // Compute (product % M) using the fact that ((x << 31) % M) == x.
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
    // The first reduction may overflow by 1 bit, so we may need to
    // repeat.  mod == M is not possible; using > allows the faster
    // sign-bit-based test.
    if (seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }
  // Returns a uniformly distributed value in the range [0..n-1]
  // REQUIRES: n > 0
  uint32_t Uniform(int n) { return Next() % n; }

  // Randomly returns true ~"1/n" of the time, and false otherwise.
  // REQUIRES: n > 0
  bool OneIn(int n) { return (Next() % n) == 0; }

  // Skewed: pick "base" uniformly from range [0,max_log] and then
  // return "base" random bits.  The effect is to pick a number in the
  // range [0,2^max_log-1] with exponential bias towards smaller numbers.
  uint32_t Skewed(int max_log) {
    return Uniform(1 << Uniform(max_log + 1));
  }
};

thread_local Random kRand(0);

class MemTableLockFreeSkipList : public Table {
 public:
  using Node = lockfree_skiplist::Node;
  MemTableLockFreeSkipList(const size_t c) : Table(c) { }
  virtual void* Put(const Key& key, const Value& value) {
    // Random Height
    static const unsigned int kBranching = 4;
    int height = 1;
    while (height < kMaxHeight && ((kRand.Next() % kBranching) == 0)) {
      height++;
    }

    const size_t alloc_size = lockfree_skiplist::Node::compute_alloc_size(key, height);
    //void* buf = aligned_alloc(8, alloc_size);
    void* buf = malloc(alloc_size);
    Node* node = Node::init_node((char*) buf, key, (1ull<<8|kTypeValue), value, height);
    node->log_moff = 0;

    table_.insert(node);
  }
  virtual bool Get(const Key& key, void** value_out) {
    return true;
  }

 private:
  lockfree_skiplist table_;
};

class TableSpSkip {
 public:
  using Node = lockfree_skiplist::Node;
  TableSpSkip(const size_t c) : capacity_(c), size_(0), next_(nullptr) { }
  void Put(const Key& key, const Value& value) {
    // Random Height
    static const unsigned int kBranching = 4;
    int height = 1;
    while (height < kMaxHeight && ((kRand.Next() % kBranching) == 0)) {
      height++;
    }

    const size_t alloc_size = lockfree_skiplist::Node::compute_alloc_size(key, height);
    //void* buf = aligned_alloc(8, alloc_size);
    void* buf = malloc(alloc_size);
    Node* node = Node::init_node((char*) buf, key, (1ull<<8|kTypeValue), value, height);
    node->log_moff = 0;

    table_.insert(node);
  }
  void SetNext(std::shared_ptr<TableSpSkip> next) { next_ = next; }
  std::shared_ptr<TableSpSkip> Next() {
    return next_;
  }
  bool HasRoom(const size_t size) {
    const size_t before = size_.fetch_add(size);
    return (before + size <= capacity_);
  }
 private:
  const size_t capacity_;
  std::atomic<size_t> size_;
  std::shared_ptr<TableSpSkip> next_;
  lockfree_skiplist table_;
};

template <>
MemTableLockFreeSkipList* NewMutable<MemTableLockFreeSkipList>(MemTableLockFreeSkipList* const prev, const size_t capacity) {
  MemTableLockFreeSkipList* new_table = new MemTableLockFreeSkipList(capacity);
  new_table->SetNext(prev);
  return new_table;
}

void ConcurrentTest() {
  const size_t num = 10*1000*1000;
  const int m = 80;
  fprintf(stdout, "*** custom_refcounter + lockfree_skiplist: %d threads ***\n", m);
  TableList<MemTableLockFreeSkipList> sl_tables(16*num/10);
  auto time_begin = std::chrono::steady_clock::now();
  std::vector<std::thread> workers;
  for (int i = 0; i < m; i++) {
    workers.emplace_back([&, i]{
      for (size_t j = i * (num/m); j < (i+1)*(num/m); j++) {
        sl_tables.Put(j, j);
      }
    });
  }
  for (auto& w : workers) {
    w.join();
  }
  auto duration = std::chrono::duration<double>(std::chrono::steady_clock::now() - time_begin).count();
  fprintf(stdout, "tp: %.3lf ops/s %.3lf sec \n", (double) num / duration, duration);
  sl_tables.PrintDebug();
}

void ConcurrentTest2() {
  const size_t num = 10*1000*1000;
  const int m = 80;
  fprintf(stdout, "*** shared_ptr + lockfree_skiplist: %d threads ***\n", m);
  TableListSharedMutex<TableSpSkip> sl_tables(16*num/10);
  auto time_begin = std::chrono::steady_clock::now();
  std::vector<std::thread> workers;
  for (int i = 0; i < m; i++) {
    workers.emplace_back([&, i]{
      for (size_t j = i * (num/m); j < (i+1)*(num/m); j++) {
        sl_tables.Put(j, j);
      }
    });
  }
  for (auto& w : workers) {
    w.join();
  }
  auto duration = std::chrono::duration<double>(std::chrono::steady_clock::now() - time_begin).count();
  fprintf(stdout, "tp: %.3lf ops/s %.3lf sec \n", (double) num / duration, duration);
  sl_tables.PrintDebug();
}

//class MemTableStlMapMutex;
//template <>
//MemTableStlMapMutex* NewMutable<MemTableStlMapMutex>(MemTableStlMapMutex* const prev, const size_t capacity);
class MemTableStlMapMutex : public MemTableStlMap {
 public:
  MemTableStlMapMutex(const size_t c) : MemTableStlMap(c) { }
  virtual void* Put(const Key& key, const Value& value) override {
    std::lock_guard<std::mutex> lk(write_mu_);
    table_.emplace(key, value);
    return nullptr;
  }
 private:
  std::mutex write_mu_;
};

template <>
MemTableStlMapMutex* NewMutable<MemTableStlMapMutex>(MemTableStlMapMutex* const prev, const size_t capacity) {
  MemTableStlMapMutex* new_table = new MemTableStlMapMutex(capacity);
  new_table->SetNext(prev);
  return new_table;
}

void ConcurrentTest3() {
  const size_t num = 10*1000*1000;
  const int m = 80;
  fprintf(stdout, "*** custom_refcounter + std::map + mutex: %d threads ***\n", m);
  TableList<MemTableStlMapMutex> tbls(16*num/10);
  auto time_begin = std::chrono::steady_clock::now();
  std::vector<std::thread> workers;
  for (int i = 0; i < m; i++) {
    workers.emplace_back([&, i]{
      for (size_t j = i * (num/m); j < (i+1)*(num/m); j++) {
        tbls.Put(j, j);
      }
    });
  }
  for (auto& w : workers) {
    w.join();
  }
  auto duration = std::chrono::duration<double>(std::chrono::steady_clock::now() - time_begin).count();
  fprintf(stdout, "tp: %.3lf ops/s %.3lf sec \n", (double) num / duration, duration);
  tbls.PrintDebug();
}

int main() {
  const size_t num = 10*1000*1000;

  fprintf(stdout, "*** custom_refcounter + std::map: SINGLE THREAD ***\n");
  TableList<MemTableStlMap> my_tables(16*num/10);
  auto time_begin = std::chrono::steady_clock::now();
  for (size_t i = 0; i < num; i++) {
    my_tables.Put(i, i);
  }
  auto duration = std::chrono::duration<double>(std::chrono::steady_clock::now() - time_begin).count();
  fprintf(stdout, "tp: %.3lf ops/s %.3lf sec \n", (double) num / duration, duration);
  my_tables.PrintDebug();

  fprintf(stdout, "*** shared_ptr + std::map: SINGLE THREAD ***\n");
  TableListSharedMutex<TableSp> sp_tables(16*num/10);
  time_begin = std::chrono::steady_clock::now();
  for (size_t i = 0; i < num; i++) {
    sp_tables.Put(i, i);
  }
  duration = std::chrono::duration<double>(std::chrono::steady_clock::now() - time_begin).count();
  fprintf(stdout, "tp: %.3lf ops/s %.3lf sec \n", (double) num / duration, duration);
  sp_tables.PrintDebug();


  fprintf(stdout, "*** custom_refcounter + lockfree_skiplist: SINGLE THREAD ***\n");
  TableList<MemTableLockFreeSkipList> sl_tables(16*num/10);
  time_begin = std::chrono::steady_clock::now();
  for (size_t i = 0; i < num; i++) {
    sl_tables.Put(i, i);
  }
  duration = std::chrono::duration<double>(std::chrono::steady_clock::now() - time_begin).count();
  fprintf(stdout, "tp: %.3lf ops/s %.3lf sec \n", (double) num / duration, duration);
  sl_tables.PrintDebug();

  ConcurrentTest();
  ConcurrentTest2();
  ConcurrentTest3();

  return 0;
}
