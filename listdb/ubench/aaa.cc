#include <chrono>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <shared_mutex>
#include <thread>
#include <vector>

#include "tbb/tbb.h"
#include "tbb/task_scheduler_init.h"

#include "listdb/common.h"
#include "listdb/index/lockfree_skiplist.h"
#include "listdb/index/skiplist.h"
//#include "listdb/lib/arena.h"
#include "listdb/lsm/table.h"
#include "listdb/lsm/table_list.h"
#include "listdb/lsm/memtable_list.h"
#include "listdb/util/random.h"

//#include "cds/init.h"
//#include "cds/gc/hp.h"
//#include "cds/gc/nogc.h"
//#include "cds/container/impl/skip_list_map.h"
//#include "cds/container/skip_list_map_hp.h"
//#include "cds/container/skip_list_map_nogc.h"

constexpr int NUM_THREADS = 80;
constexpr size_t NUM_LOADS = 10 * 1000 * 1000;
constexpr size_t NUM_WORKS = 1 * 1000 * 1000;

enum OpType {
  OP_INSERT,
  OP_UPDATE,
  OP_READ,
  OP_SCAN,
  OP_DELETE,
};

//thread_local Arena arena(1*(1ull<<30));

std::mutex insert_mu;

void drop_cache() {
	// Remove cache
	int size = 256*1024*1024;
	char *garbage = new char[size];
	for(int i=0;i<size;++i)
		garbage[i] = i;
	for(int i=100;i<size;++i)
		garbage[i] += garbage[i-100];
	delete[] garbage;
}

void FillLoadKeys(const size_t num_loads, std::vector<uint64_t>* load_keys) {
  std::string filename = "/home/wkim/RECIPE/index-microbench/workloads_10M_1M_unif/loada_unif_int.dat";
  //std::string filename = "/home/wkim/RECIPE/index-microbench/workloads_100M_10M_unif/loada_unif_int.dat";
  std::ifstream istrm(filename);
  size_t count = 0;
  size_t epoch = 10;
  while ((count < num_loads) && istrm.good()) {
    std::string op;
    uint64_t key;
    istrm >> op >> key;
    if (op == "INSERT") {
      load_keys->push_back(key);
    } else {
      std::cout << "Invalid op: " << op << std::endl;
      exit(1);
    }
    count++;
    if (count % epoch == 0) {
      fprintf(stdout, "\rFilling up load queries: %zu%%", count*100/num_loads);
      fflush(stdout);
      if (count == epoch * 100) {
        epoch *= 10;
      }
    }
  }
  istrm.close();
  if (count != num_loads) {
    std::cout << "Not enough queries in file: " << filename << " (" << count << "/" << num_loads << ")" << std::endl;
    exit(1);
  }
  fprintf(stdout, "\rFilling up load queries: \x1b[32mDONE\x1b[0m\n");
}

void FillWorkKeys(const char workload_type, const size_t num_works, std::vector<OpType>* work_ops,
                  std::vector<uint64_t>* work_keys) {
  std::stringstream ss;
  ss << "/home/wkim/RECIPE/index-microbench/workloads_10M_1M_unif/txns" << workload_type << "_unif_int.dat";
  //ss << "/home/wkim/RECIPE/index-microbench/workloads_100M_10M_unif/txns" << workload_type << "_unif_int.dat";
  std::string filename = ss.str();
  std::ifstream istrm(filename);
  size_t count = 0;
  size_t epoch = 10;
  while ((count < num_works) && istrm.good()) {
    std::string op;
    uint64_t key;
    istrm >> op >> key;
    if (op == "INSERT") {
      work_ops->push_back(OP_INSERT);
      work_keys->push_back(key);
    } else if (op == "UPDATE") {
      work_ops->push_back(OP_UPDATE);
      work_keys->push_back(key);
    } else if (op == "READ") {
      work_ops->push_back(OP_READ);
      work_keys->push_back(key);
    } else {
      std::cout << "Invalid op: " << op << std::endl;
      exit(1);
    }
    count++;
    if (count % epoch == 0) {
      fprintf(stdout, "\rFilling up workload queries: %zu%%", count*100/num_works);
      fflush(stdout);
      if (count == epoch * 100) {
        epoch *= 10;
      }
    }
  }
  istrm.close();
  if (count != num_works) {
    std::cout << "Not enough queries in file: " << filename << " (" << count << "/" << num_works << ")" << std::endl;
    exit(1);
  }
  fprintf(stdout, "\rFilling up workload queries: \x1b[32mDONE\x1b[0m\n");
}

void Run1(const int num_threads, const std::vector<uint64_t>& load_keys, const std::vector<OpType>& work_ops,
          const std::vector<uint64_t>& work_keys) {
  lockfree_skiplist skiplist;
  using Node = lockfree_skiplist::Node;

  fprintf(stdout, "=== lockfree_skiplist ===\n");

  // Load
  {
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> loaders;
    const size_t num_ops_per_thread = NUM_LOADS / num_threads;
    for (int id = 0; id < num_threads; id++) {
      loaders.emplace_back([&, id] {
        SetAffinity(id);
        Random rnd(id);
        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          static const unsigned int kBranching = 4;
          int height = 1;
          while (height < kMaxHeight && ((rnd.Next() % kBranching) == 0)) {
            height++;
          }
          const size_t alloc_size = Node::compute_alloc_size(load_keys[i], height);
          //void* buf = malloc(alloc_size);
          void* buf = aligned_alloc(8, alloc_size);
          //void* buf = arena.allocate(alloc_size);
          Node* node = Node::init_node((char*) buf, load_keys[i], (1ull<<8|kTypeValue), 0, height);
          node->log_paddr = 0;

          skiplist.insert(node);
        }
      });
    }
    for (auto& t : loaders) {
      t.join();
    }
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Load IOPS: %.3lf M\n", NUM_LOADS/dur_sec/1000000);
  }

  // Work
  drop_cache();
  {
    auto begin_tp = std::chrono::steady_clock::now();
    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, NUM_WORKS), [&](const tbb::blocked_range<uint64_t> &scope) {
      int tid = tbb::task_arena::current_thread_index();
      //SetAffinity(tid);
      Random rnd(tid);
      for (uint64_t i = scope.begin(); i != scope.end(); i++) {
        if (work_ops[i] == OP_UPDATE) {
          static const unsigned int kBranching = 4;
          int height = 1;
          while (height < kMaxHeight && ((rnd.Next() % kBranching) == 0)) {
            height++;
          }
          const size_t alloc_size = Node::compute_alloc_size(work_keys[i], height);
          //void* buf = malloc(alloc_size);
          void* buf = aligned_alloc(8, alloc_size);
          //char* buf = arena.allocate(alloc_size);
          Node* node = Node::init_node((char*) buf, work_keys[i], (1ull<<8|kTypeValue), 0, height, false);
          node->log_paddr = 0;

          //skiplist.insert(work_keys[i], 0, height);
          skiplist.insert(node);
        } else if (work_ops[i] == OP_READ) {
          skiplist.find(work_keys[i]);
        }
      }
    });
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Work IOPS: %.3lf M\n", NUM_WORKS/dur_sec/1000000);
  }
  fprintf(stdout, "\n");
}

void Run1_lock(const int num_threads, const std::vector<uint64_t>& load_keys, const std::vector<OpType>& work_ops,
          const std::vector<uint64_t>& work_keys) {
  lockfree_skiplist skiplist;
  using Node = lockfree_skiplist::Node;
  static std::shared_mutex rw_mu;

  fprintf(stdout, "=== lockfree_skiplist + CAS-lock ===\n");

  // Load
  {
    static std::atomic<int> state(0);
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> loaders;
    const size_t num_ops_per_thread = NUM_LOADS / num_threads;
    for (int id = 0; id < num_threads; id++) {
      loaders.emplace_back([&, id] {
        Random rnd(id);
        SetAffinity(id);
        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          static const unsigned int kBranching = 4;
          int height = 1;
          while (height < kMaxHeight && ((rnd.Next() % kBranching) == 0)) {
            height++;
          }
          const size_t alloc_size = Node::compute_alloc_size(load_keys[i], height);
          //void* buf = malloc(alloc_size);
          void* buf = aligned_alloc(8, alloc_size);
          //char* buf = arena.allocate(alloc_size);
          Node* node = Node::init_node((char*) buf, load_keys[i], (1ull<<8|kTypeValue), 0, height);
          node->log_paddr = 0;

          while (true) {
            int expected = 0;
            if (state.compare_exchange_weak(expected, 1)) {
              break;
            }
          }
          skiplist.insert(node);
          state.store(0);
        }
      });
    }
    for (auto& t : loaders) {
      t.join();
    }
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Load IOPS: %.3lf M\n", NUM_LOADS/dur_sec/1000000);
  }

  // Work
  drop_cache();
  {
    static std::atomic<int> state(0);
    auto begin_tp = std::chrono::steady_clock::now();
    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, NUM_WORKS), [&](const tbb::blocked_range<uint64_t> &scope) {
      int tid = tbb::task_arena::current_thread_index();
      Random rnd(tid);
      for (uint64_t i = scope.begin(); i != scope.end(); i++) {
        if (work_ops[i] == OP_UPDATE) {
          static const unsigned int kBranching = 4;
          int height = 1;
          while (height < kMaxHeight && ((rnd.Next() % kBranching) == 0)) {
            height++;
          }
          const size_t alloc_size = Node::compute_alloc_size(work_keys[i], height);
          void* buf = aligned_alloc(8, alloc_size);
          Node* node = Node::init_node((char*) buf, work_keys[i], (1ull<<8|kTypeValue), 0, height, false);
          node->log_paddr = 0;

          while (true) {
            int expected = 0;
            if (state.compare_exchange_weak(expected, -1)) {
              break;
            }
          }
          skiplist.insert(node);
          state.store(0);
        } else if (work_ops[i] == OP_READ) {
          //int expected = 0;
          //while (true) {
          //  int desired = expected + 1;
          //  if (state.compare_exchange_weak(expected, desired)) {
          //    break;
          //  }
          //}
          skiplist.find(work_keys[i]);
          //state.fetch_add(-1);
        }
      }
    });
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Work IOPS: %.3lf M\n", NUM_WORKS/dur_sec/1000000);
  }
  fprintf(stdout, "\n");
}

void Run1_shared_lock(const int num_threads, const std::vector<uint64_t>& load_keys, const std::vector<OpType>& work_ops,
          const std::vector<uint64_t>& work_keys) {
  lockfree_skiplist skiplist;
  using Node = lockfree_skiplist::Node;
  static std::shared_mutex rw_mu;

  fprintf(stdout, "=== lockfree_skiplist + std::shared_mutex ===\n");

  // Load
  {
    static std::atomic<int> state(0);
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> loaders;
    const size_t num_ops_per_thread = NUM_LOADS / num_threads;
    for (int id = 0; id < num_threads; id++) {
      loaders.emplace_back([&, id] {
        Random rnd(id);
        SetAffinity(id);
        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          static const unsigned int kBranching = 4;
          int height = 1;
          while (height < kMaxHeight && ((rnd.Next() % kBranching) == 0)) {
            height++;
          }
          const size_t alloc_size = Node::compute_alloc_size(load_keys[i], height);
          //void* buf = malloc(alloc_size);
          void* buf = aligned_alloc(8, alloc_size);
          //char* buf = arena.allocate(alloc_size);
          Node* node = Node::init_node((char*) buf, load_keys[i], (1ull<<8|kTypeValue), 0, height);
          node->log_paddr = 0;

          std::unique_lock<std::shared_mutex> lk(rw_mu);
          skiplist.insert(node);
          lk.unlock();
        }
      });
    }
    for (auto& t : loaders) {
      t.join();
    }
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Load IOPS: %.3lf M\n", NUM_LOADS/dur_sec/1000000);
  }

  // Work
  drop_cache();
  {
    static std::atomic<int> state(0);
    auto begin_tp = std::chrono::steady_clock::now();
    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, NUM_WORKS), [&](const tbb::blocked_range<uint64_t> &scope) {
      int tid = tbb::task_arena::current_thread_index();
      Random rnd(tid);
      for (uint64_t i = scope.begin(); i != scope.end(); i++) {
        if (work_ops[i] == OP_UPDATE) {
          static const unsigned int kBranching = 4;
          int height = 1;
          while (height < kMaxHeight && ((rnd.Next() % kBranching) == 0)) {
            height++;
          }
          const size_t alloc_size = Node::compute_alloc_size(work_keys[i], height);
          void* buf = aligned_alloc(8, alloc_size);
          Node* node = Node::init_node((char*) buf, work_keys[i], (1ull<<8|kTypeValue), 0, height, false);
          node->log_paddr = 0;

          std::unique_lock<std::shared_mutex> lk(rw_mu);
          skiplist.insert(node);
          lk.unlock();
        } else if (work_ops[i] == OP_READ) {
          std::shared_lock<std::shared_mutex> lk(rw_mu);
          skiplist.find(work_keys[i]);
          lk.unlock();
        }
      }
    });
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Work IOPS: %.3lf M\n", NUM_WORKS/dur_sec/1000000);
  }
  fprintf(stdout, "\n");
}

void Run1_shared_ptr(const int num_threads, const std::vector<uint64_t>& load_keys, const std::vector<OpType>& work_ops,
          const std::vector<uint64_t>& work_keys) {
  std::shared_ptr<lockfree_skiplist> skiplist = std::make_shared<lockfree_skiplist>();
  using Node = lockfree_skiplist::Node;

  fprintf(stdout, "=== lockfree_skiplist std::shared_ptr ===\n");

  // Load
  {
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> loaders;
    const size_t num_ops_per_thread = NUM_LOADS / num_threads;
    for (int id = 0; id < num_threads; id++) {
      loaders.emplace_back([&, id] {
        SetAffinity(id);
        Random rnd(id);
        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          static const unsigned int kBranching = 4;
          int height = 1;
          while (height < kMaxHeight && ((rnd.Next() % kBranching) == 0)) {
            height++;
          }
          const size_t alloc_size = Node::compute_alloc_size(load_keys[i], height);
          //void* buf = malloc(alloc_size);
          void* buf = aligned_alloc(8, alloc_size);
          //char* buf = arena.allocate(alloc_size);
          Node* node = Node::init_node((char*) buf, load_keys[i], (1ull<<8|kTypeValue), 0, height);
          node->log_paddr = 0;

          auto table = skiplist;
          table->insert(node);
        }
      });
    }
    for (auto& t : loaders) {
      t.join();
    }
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Load IOPS: %.3lf M\n", NUM_LOADS/dur_sec/1000000);
  }

  {
    auto begin_tp = std::chrono::steady_clock::now();
    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, NUM_WORKS), [&](const tbb::blocked_range<uint64_t> &scope) {
      int tid = tbb::task_arena::current_thread_index();
      Random rnd(tid);
      for (uint64_t i = scope.begin(); i != scope.end(); i++) {
        auto table = skiplist;
        if (work_ops[i] == OP_UPDATE) {
          static const unsigned int kBranching = 4;
          int height = 1;
          while (height < kMaxHeight && ((rnd.Next() % kBranching) == 0)) {
            height++;
          }
          const size_t alloc_size = Node::compute_alloc_size(load_keys[i], height);
          //void* buf = malloc(alloc_size);
          void* buf = aligned_alloc(8, alloc_size);
          //char* buf = arena.allocate(alloc_size);
          Node* node = Node::init_node((char*) buf, load_keys[i], (1ull<<8|kTypeValue), 0, height);
          node->log_paddr = 0;

          table->insert(node);
        } else if (work_ops[i] == OP_READ) {
          table->find(work_keys[i]);
        }
      }
    });
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Work IOPS: %.3lf M\n", NUM_WORKS/dur_sec/1000000);
  }
  fprintf(stdout, "\n");
}


void Run1_sharded(const int num_threads, const std::vector<uint64_t>& load_keys, const std::vector<OpType>& work_ops,
          const std::vector<uint64_t>& work_keys) {
  lockfree_skiplist skiplist[256];
  using Node = lockfree_skiplist::Node;

  fprintf(stdout, "=== lockfree_skiplist + 256-shard ===\n");

  // Load
  {
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> loaders;
    const size_t num_ops_per_thread = NUM_LOADS / num_threads;
    for (int id = 0; id < num_threads; id++) {
      loaders.emplace_back([&, id] {
        Random rnd(id);
        SetAffinity(id);
        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          static const unsigned int kBranching = 4;
          int height = 1;
          while (height < kMaxHeight && ((rnd.Next() % kBranching) == 0)) {
            height++;
          }
          const size_t alloc_size = Node::compute_alloc_size(load_keys[i], height);
          //void* buf = malloc(alloc_size);
          void* buf = aligned_alloc(8, alloc_size);
          //char* buf = arena.allocate(alloc_size);
          Node* node = Node::init_node((char*) buf, load_keys[i], (1ull<<8|kTypeValue), 0, height);
          node->log_paddr = 0;

          int s = load_keys[i]%256;
          skiplist[s].insert(node);
        }
      });
    }
    for (auto& t : loaders) {
      t.join();
    }
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Load IOPS: %.3lf M\n", NUM_LOADS/dur_sec/1000000);
  }
  fprintf(stdout, "\n");
}

void Run2(const int num_threads, const std::vector<uint64_t>& load_keys, const std::vector<OpType>& work_ops,
          const std::vector<uint64_t>& work_keys) {
  std::map<Key, Value> table;

  fprintf(stdout, "=== std::map + CAS-lock ===\n");

  // Load
  {
    static std::atomic<int> state(0);
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> loaders;
    const size_t num_ops_per_thread = NUM_LOADS / num_threads;
    for (int id = 0; id < num_threads; id++) {
      loaders.emplace_back([&, id] {
        SetAffinity(id);
        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          while (true) {
            int expected = 0;
            if (state.compare_exchange_weak(expected, 1)) {
              break;
            }
          }
          table.emplace(load_keys[i], 0);
          state.store(0);
        }
      });
    }
    for (auto& t : loaders) {
      t.join();
    }
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Load IOPS: %.3lf M\n", NUM_LOADS/dur_sec/1000000);
  }
  // Work
  drop_cache();
  {
    static std::atomic<int> state(0);
    auto begin_tp = std::chrono::steady_clock::now();
    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, NUM_WORKS), [&](const tbb::blocked_range<uint64_t> &scope) {
      for (uint64_t i = scope.begin(); i != scope.end(); i++) {
        if (work_ops[i] == OP_UPDATE) {
          while (true) {
            int expected = 0;
            if (state.compare_exchange_weak(expected, -1)) {
              break;
            }
          }
          table.emplace(work_keys[i], 0);
          state.store(0);
        } else if (work_ops[i] == OP_READ) {
          int expected = 0;
          while (true) {
            int desired = expected + 1;
            if (state.compare_exchange_weak(expected, desired)) {
              break;
            }
          }
          auto it = table.find(work_keys[i]);
          if (it == table.end()) {
            fprintf(stderr, "HHAHAHAHHAHA\n");
          }
          state.fetch_add(-1);
        }
      }
    });
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Work IOPS: %.3lf M\n", NUM_WORKS/dur_sec/1000000);
  }
  fprintf(stdout, "\n");
}

void Run2_shared_lock(const int num_threads, const std::vector<uint64_t>& load_keys, const std::vector<OpType>& work_ops,
          const std::vector<uint64_t>& work_keys) {
  std::map<Key, Value> table;
  static std::shared_mutex rw_mu;

  fprintf(stdout, "=== std::map + shared_mutex ===\n");

  // Load
  {
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> loaders;
    const size_t num_ops_per_thread = NUM_LOADS / num_threads;
    for (int id = 0; id < num_threads; id++) {
      loaders.emplace_back([&, id] {
        SetAffinity(id);
        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          std::unique_lock<std::shared_mutex> lk(rw_mu);
          table.emplace(load_keys[i], 0);
          lk.unlock();
        }
      });
    }
    for (auto& t : loaders) {
      t.join();
    }
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Load IOPS: %.3lf M\n", NUM_LOADS/dur_sec/1000000);
  }
  // Work
  drop_cache();
  {
    static std::atomic<int> state(0);
    auto begin_tp = std::chrono::steady_clock::now();
    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, NUM_WORKS), [&](const tbb::blocked_range<uint64_t> &scope) {
      for (uint64_t i = scope.begin(); i != scope.end(); i++) {
        if (work_ops[i] == OP_UPDATE) {
          std::unique_lock<std::shared_mutex> lk(rw_mu);
          table.emplace(work_keys[i], 0);
          lk.unlock();
        } else if (work_ops[i] == OP_READ) {
          std::shared_lock<std::shared_mutex> lk(rw_mu);
          table.find(work_keys[i]);
          lk.unlock();
        }
      }
    });
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Work IOPS: %.3lf M\n", NUM_WORKS/dur_sec/1000000);
  }
  fprintf(stdout, "\n");
}

void Run3(const int num_threads, const std::vector<uint64_t>& load_keys, const std::vector<OpType>& work_ops,
          const std::vector<uint64_t>& work_keys) {
  SkipList skiplist;
  using Node = SkipList::Node;

  fprintf(stdout, "=== SkipList + CAS-lock ===\n");

  // Load
  {
    static std::atomic<int> state(0);
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> loaders;
    const size_t num_ops_per_thread = NUM_LOADS / num_threads;
    for (int id = 0; id < num_threads; id++) {
      loaders.emplace_back([&, id] {
        Random rnd(id);
        SetAffinity(id);
        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          static const unsigned int kBranching = 4;
          int height = 1;
          while (height < kMaxHeight && ((rnd.Next() % kBranching) == 0)) {
            height++;
          }
          const size_t alloc_size = Node::compute_alloc_size(load_keys[i], height);
          //void* buf = malloc(alloc_size);
          void* buf = aligned_alloc(8, alloc_size);
          //char* buf = arena.allocate(alloc_size);
          Node* node = Node::init_node((char*) buf, load_keys[i], (1ull<<8|kTypeValue), 0, height);
          node->log_paddr = 0;

          while (true) {
            int expected = 0;
            if (state.compare_exchange_weak(expected, -1)) {
              break;
            }
          }
          skiplist.insert(node);
          state.store(0);
        }
      });
    }
    for (auto& t : loaders) {
      t.join();
    }
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Load IOPS: %.3lf M\n", NUM_LOADS/dur_sec/1000000);
  }
  // Work
  drop_cache();
  {
    static std::atomic<int> state(0);
    auto begin_tp = std::chrono::steady_clock::now();
    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, NUM_WORKS), [&](const tbb::blocked_range<uint64_t> &scope) {
      int tid = tbb::task_arena::current_thread_index();
      Random rnd(tid);
      for (uint64_t i = scope.begin(); i != scope.end(); i++) {
        if (work_ops[i] == OP_UPDATE) {
          static const unsigned int kBranching = 4;
          int height = 1;
          while (height < kMaxHeight && ((rnd.Next() % kBranching) == 0)) {
            height++;
          }
          const size_t alloc_size = Node::compute_alloc_size(work_keys[i], height);
          void* buf = aligned_alloc(8, alloc_size);
          Node* node = Node::init_node((char*) buf, work_keys[i], (1ull<<8|kTypeValue), 0, height, false);
          node->log_paddr = 0;

          while (true) {
            int expected = 0;
            if (state.compare_exchange_weak(expected, -1)) {
              break;
            }
          }
          skiplist.insert(node);
          state.store(0);
        } else if (work_ops[i] == OP_READ) {
          int expected = 0;
          while (true) {
            int desired = expected + 1;
            if (state.compare_exchange_weak(expected, desired)) {
              break;
            }
          }
          skiplist.find(work_keys[i]);
          state.fetch_add(-1);
        }
      }
    });
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Work IOPS: %.3lf M\n", NUM_WORKS/dur_sec/1000000);
  }
  fprintf(stdout, "\n");
}

void Run3_shared_lock(const int num_threads, const std::vector<uint64_t>& load_keys, const std::vector<OpType>& work_ops,
          const std::vector<uint64_t>& work_keys) {
  SkipList skiplist;
  using Node = SkipList::Node;
  static std::shared_mutex rw_mu;

  fprintf(stdout, "=== SkipList + std::shared_mutex ===\n");

  // Load
  {
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> loaders;
    const size_t num_ops_per_thread = NUM_LOADS / num_threads;
    for (int id = 0; id < num_threads; id++) {
      loaders.emplace_back([&, id] {
        Random rnd(id);
        SetAffinity(id);
        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          static const unsigned int kBranching = 4;
          int height = 1;
          while (height < kMaxHeight && ((rnd.Next() % kBranching) == 0)) {
            height++;
          }
          const size_t alloc_size = Node::compute_alloc_size(load_keys[i], height);
          void* buf = aligned_alloc(8, alloc_size);
          Node* node = Node::init_node((char*) buf, load_keys[i], (1ull<<8|kTypeValue), 0, height);
          node->log_paddr = 0;

          std::unique_lock<std::shared_mutex> lk(rw_mu);
          skiplist.insert(node);
          lk.unlock();
        }
      });
    }
    for (auto& t : loaders) {
      t.join();
    }
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Load IOPS: %.3lf M\n", NUM_LOADS/dur_sec/1000000);
  }
  // Work
  drop_cache();
  {
    auto begin_tp = std::chrono::steady_clock::now();
    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, NUM_WORKS), [&](const tbb::blocked_range<uint64_t> &scope) {
      int tid = tbb::task_arena::current_thread_index();
      Random rnd(tid);
      for (uint64_t i = scope.begin(); i != scope.end(); i++) {
        if (work_ops[i] == OP_UPDATE) {
          static const unsigned int kBranching = 4;
          int height = 1;
          while (height < kMaxHeight && ((rnd.Next() % kBranching) == 0)) {
            height++;
          }
          const size_t alloc_size = Node::compute_alloc_size(work_keys[i], height);
          void* buf = aligned_alloc(8, alloc_size);
          Node* node = Node::init_node((char*) buf, work_keys[i], (1ull<<8|kTypeValue), 0, height, false);
          node->log_paddr = 0;

          std::unique_lock<std::shared_mutex> lk(rw_mu);
          skiplist.insert(node);
          lk.unlock();
        } else if (work_ops[i] == OP_READ) {
          std::shared_lock<std::shared_mutex> lk(rw_mu);
          skiplist.find(work_keys[i]);
          lk.unlock();
        }
      }
    });
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Work IOPS: %.3lf M\n", NUM_WORKS/dur_sec/1000000);
  }
  fprintf(stdout, "\n");
}

void Run3_sharded(const int num_threads, const std::vector<uint64_t>& load_keys, const std::vector<OpType>& work_ops,
          const std::vector<uint64_t>& work_keys) {
  SkipList skiplist[256];
  using Node = SkipList::Node;

  fprintf(stdout, "=== SkipList 256-shard ===\n");

  // Load
  {
    static std::atomic<int> states[256];
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> loaders;
    const size_t num_ops_per_thread = NUM_LOADS / num_threads;
    for (int id = 0; id < num_threads; id++) {
      loaders.emplace_back([&, id] {
        Random rnd(id);
        SetAffinity(id);
        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          static const unsigned int kBranching = 4;
          int height = 1;
          while (height < kMaxHeight && ((rnd.Next() % kBranching) == 0)) {
            height++;
          }
          const size_t alloc_size = Node::compute_alloc_size(load_keys[i], height);
          //void* buf = malloc(alloc_size);
          void* buf = aligned_alloc(8, alloc_size);
          //char* buf = arena.allocate(alloc_size);
          Node* node = Node::init_node((char*) buf, load_keys[i], (1ull<<8|kTypeValue), 0, height);
          node->log_paddr = 0;

          int s = load_keys[i]%256;
          int expected = 0;
          while (states[s].compare_exchange_weak(expected, 1)) {
            expected = 0;
          }
          skiplist[s].insert(node);
          states[s].store(0);
        }
      });
    }
    for (auto& t : loaders) {
      t.join();
    }
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Load IOPS: %.3lf M\n", NUM_LOADS/dur_sec/1000000);
  }
  fprintf(stdout, "\n");
}

//void Run4(const int num_threads, const std::vector<uint64_t>& load_keys, const std::vector<OpType>& work_ops,
//          const std::vector<uint64_t>& work_keys) {
//  cds::Initialize();
//
//  //cds::gc::HP hp;
//  //cds::container::skip_list::traits my_traits;
//  //typedef cds::container::SkipListMap<cds::gc::HP, Key, Value, cds::container::skip_list::traits> map_type;
//  cds::container::SkipListMap<cds::gc::nogc, uint64_t, uint64_t, cds::container::skip_list::traits> table;
//
//  fprintf(stdout, "=== cds::container::SkipListMap ===\n");
//
//  // Load
//  {
//    static std::atomic<int> state(0);
//    auto begin_tp = std::chrono::steady_clock::now();
//    std::vector<std::thread> loaders;
//    const size_t num_ops_per_thread = NUM_LOADS / num_threads;
//    for (int id = 0; id < num_threads; id++) {
//      loaders.emplace_back([&, id] {
//        //cds::gc::hp::GarbageCollector::Construct( map_type::c_nHazardPtrCount + 1, 1, 16 );
//        cds::threading::Manager::attachThread();
//        //cds::gc::hp::custom_smr<cds::gc::hp::details::StrangeTLSManager>::attach_thread();
//        SetAffinity(id);
//        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
//          table.insert(load_keys[i], 0);
//        }
//        cds::threading::Manager::detachThread();
//      });
//    }
//    for (auto& t : loaders) {
//      t.join();
//    }
//    auto end_tp = std::chrono::steady_clock::now();
//    std::chrono::duration<double> dur = end_tp - begin_tp;
//    double dur_sec = dur.count();
//    fprintf(stdout, "Load IOPS: %.3lf M\n", NUM_LOADS/dur_sec/1000000);
//  }
//  cds::Terminate();
//  fprintf(stdout, "\n");
//}
//
thread_local Random rnd_(1);

class MemTableLockFreeSkipList : public Table {
 public:
  using Node = lockfree_skiplist::Node;
  MemTableLockFreeSkipList(const size_t c) : Table(c) { }
  void* Put(const Key& key, const Value& value) override {
    // Random Height
    static const unsigned int kBranching = 4;
    int height = 1;
    while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
      height++;
    }

    const size_t alloc_size = lockfree_skiplist::Node::compute_alloc_size(key, height);
    void* buf = aligned_alloc(8, alloc_size);
    //void* buf = malloc(alloc_size);
    Node* node = Node::init_node((char*) buf, key, (1ull<<8|kTypeValue), value, height);
    node->log_paddr = 0;

    table_.insert(node);
  }
  bool Get(const Key& key, void** value_out) override {
    return table_.find(key) != nullptr;
  }

 private:
  lockfree_skiplist table_;
};

template <>
MemTableLockFreeSkipList* NewMutable<MemTableLockFreeSkipList>(MemTableLockFreeSkipList* const prev, const size_t capacity) {
  MemTableLockFreeSkipList* new_table = new MemTableLockFreeSkipList(capacity);
  new_table->SetNext(prev);
  return new_table;
}

void Run5(const int num_threads, const std::vector<uint64_t>& load_keys, const std::vector<OpType>& work_ops,
          const std::vector<uint64_t>& work_keys) {
  //MemTableLockFreeSkipList tbl(16*NUM_LOADS/1);
  MemTableLockFreeSkipList* tbl = new MemTableLockFreeSkipList(16*NUM_LOADS/1);

  fprintf(stdout, "=== Table + RefCount ==\n");

  // Load
  {
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> loaders;
    const size_t num_ops_per_thread = NUM_LOADS / num_threads;
    for (int id = 0; id < num_threads; id++) {
      loaders.emplace_back([&, id] {
        SetAffinity(id);
        *((uint32_t*) &rnd_) = id + 1;
        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          tbl->w_Ref(); 
          tbl->Put(load_keys[i], 0);
          tbl->w_UnRef(); 
        }
      });
    }
    for (auto& t : loaders) {
      t.join();
    }
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Load IOPS: %.3lf M\n", NUM_LOADS/dur_sec/1000000);
  }

  // Work
  drop_cache();
  {
    auto begin_tp = std::chrono::steady_clock::now();
    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, NUM_WORKS), [&](const tbb::blocked_range<uint64_t> &scope) {
      int tid = tbb::task_arena::current_thread_index();
      //SetAffinity(tid);
      *((uint32_t*) &rnd_) = tid + 1;
      for (uint64_t i = scope.begin(); i != scope.end(); i++) {
        if (work_ops[i] == OP_UPDATE) {
          tbl->w_Ref(); 
          tbl->Put(work_keys[i], 0);
          tbl->w_UnRef(); 
        } else if (work_ops[i] == OP_READ) {
          tbl->Get(work_keys[i], nullptr);
        }
      }
    });
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Work IOPS: %.3lf M\n", NUM_WORKS/dur_sec/1000000);
  }
  fprintf(stdout, "\n");
}

int main() {
  std::vector<uint64_t> load_keys;
  std::vector<OpType> work_ops;
  std::vector<uint64_t> work_keys;
  load_keys.reserve(NUM_LOADS);
  work_ops.reserve(NUM_WORKS);
  work_keys.reserve(NUM_WORKS);
  fprintf(stdout, "NUM_LOADS: %zu\nNUM_WORKS: %zu\n", NUM_LOADS, NUM_WORKS);
  FillLoadKeys(NUM_LOADS, &load_keys);
  FillWorkKeys('c', NUM_WORKS, &work_ops, &work_keys);

  tbb::task_scheduler_init init(NUM_THREADS);

  //drop_cache();
  //Run2(NUM_THREADS, load_keys, work_ops, work_keys);
  //drop_cache();
  //Run2_shared_lock(NUM_THREADS, load_keys, work_ops, work_keys);
  //drop_cache();
  //Run3_shared_lock(NUM_THREADS, load_keys, work_ops, work_keys);
  drop_cache();
  Run1(NUM_THREADS, load_keys, work_ops, work_keys);
  drop_cache();
  Run1_shared_ptr(NUM_THREADS, load_keys, work_ops, work_keys);
  drop_cache();
  Run5(NUM_THREADS, load_keys, work_ops, work_keys);
  //drop_cache();
  //Run1_lock(NUM_THREADS, load_keys, work_ops, work_keys);
  //drop_cache();
  //Run1_shared_lock(NUM_THREADS, load_keys, work_ops, work_keys);
  //drop_cache();
  //Run1_sharded(NUM_THREADS, load_keys, work_ops, work_keys);
  drop_cache();
  Run3(NUM_THREADS, load_keys, work_ops, work_keys);
  //Run3_sharded(NUM_THREADS, load_keys, work_ops, work_keys);
  //Run4(NUM_THREADS, load_keys, work_ops, work_keys);
  return 0;
}
