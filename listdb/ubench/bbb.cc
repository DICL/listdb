// Test PmemLog performance

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
#include <experimental/filesystem>

#include <xmmintrin.h>

#include "tbb/tbb.h"
#include "tbb/task_scheduler_init.h"

#include "listdb/common.h"
#include "listdb/lib/numa.h"
#include "listdb/core/pmem_log.h"
#include "listdb/util/random.h"

constexpr int NUM_THREADS = 80;
constexpr size_t NUM_LOADS = 10 * 1000 * 1000;
constexpr size_t NUM_WORKS = 1 * 1000 * 1000;

constexpr int NUM_SHARDS = kNumShards;

namespace fs = std::experimental::filesystem::v1;

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

static pmem::obj::pool<pmem_log_root> pool_table[kNumRegions];
static int pool_id_table[kNumRegions];

void InitPoolSet() {
  // Create poolset file
  for (int i = 0; i < kNumRegions; i++) {
    std::stringstream pss;
    pss << "/pmem" << i << "/wkim/pmem_log_test";
    std::string path = pss.str();
    fs::remove_all(path);
    fs::create_directories(path);

    std::string poolset = path + ".set";
    std::fstream strm(poolset, strm.out);
    strm << "PMEMPOOLSET" << std::endl;
    strm << "OPTION SINGLEHDR" << std::endl;
    strm << "400G " << path << "/" << std::endl;
    strm.close();

    int id = Pmem::BindPoolSet<pmem_log_root>(poolset, "");
    pool_table[i] = Pmem::pool<pmem_log_root>(id);
    pool_id_table[i] = id;
  }
}

inline void clwb(const void *addr, const size_t size)
{
  char* a = (char*) addr;
  int s = size;
  while (s > 0) {
    asm volatile(".byte 0x66; xsaveopt %0" : "+m" \
      (*(volatile char *)(a)));
    a += 64;
    s -= 64;
  }
}

void Run1(const int num_threads, const std::vector<uint64_t>& load_keys, const std::vector<OpType>& work_ops,
          const std::vector<uint64_t>& work_keys) {
  fprintf(stdout, "=== PmemLog ===\n");
  
  InitPoolSet();
  PmemLog* log[kNumRegions];
  for (int i = 0; i < kNumRegions; i++) {
    log[i] = new PmemLog(pool_id_table[i], 0);
  }

  // Load
  {
    drop_cache();
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> loaders;
    const size_t num_ops_per_thread = NUM_LOADS / num_threads;
    std::atomic<size_t> total_bytes_written = 0;
    for (int id = 0; id < num_threads; id++) {
      loaders.emplace_back([&, id] {
        size_t bytes_written = 0;
        Random rnd(id);
        SetAffinity(Numa::CpuSequenceRR(id));
        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          static const unsigned int kBranching = 4;
          int height = 1;
          while (height < kMaxHeight && ((rnd.Next() % kBranching) == 0)) {
            height++;
          }
          //const size_t alloc_size = Node::compute_alloc_size(load_keys[i], height);
          const size_t alloc_size = 8 + 8 + 8 + 8 + 8 * height;
          //const size_t alloc_size = 256;

          // Write IUL log entry
          int region = GetChip();
          auto paddr = log[region]->Allocate(alloc_size);
          char* p = (char*) paddr.get();
          struct Entry {
            uint64_t key;
            uint64_t tag;
            uint64_t value;
            int height;
          };
          Entry* ep = (Entry*) p;
          ep->key = load_keys[i];
          ep->tag = 0;
          ep->value = 0;
          ep->height = height;

          clwb(paddr.get(), alloc_size);
          _mm_sfence();

          bytes_written += alloc_size;
        }
        total_bytes_written.fetch_add(bytes_written);
      });
    }
    for (auto& t : loaders) {
      t.join();
    }
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Load IOPS: %.3lf M\n", NUM_LOADS/dur_sec/1000000);
    fprintf(stdout, "Load Throughput: %.3lf MB/s\n", total_bytes_written.load()/dur_sec/1024/1024);
  }
  fprintf(stdout, "\n");
}

void Run2(const int num_threads, const std::vector<uint64_t>& load_keys, const std::vector<OpType>& work_ops,
          const std::vector<uint64_t>& work_keys) {
  fprintf(stdout, "=== PmemLog %d-shard ===\n", NUM_SHARDS);
  
  InitPoolSet();
  PmemLog* log[NUM_SHARDS][kNumRegions];
  for (int s = 0; s < NUM_SHARDS; s++) {
  for (int i = 0; i < kNumRegions; i++) {
    //auto pool = Pmem::pool<pmem_log>(i);
    log[s][i] = new PmemLog(pool_id_table[i], s);
  }
  }

  // Load
  {
    drop_cache();
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> loaders;
    const size_t num_ops_per_thread = NUM_LOADS / num_threads;
    std::atomic<size_t> total_bytes_written = 0;
    for (int id = 0; id < num_threads; id++) {
      loaders.emplace_back([&, id] {
        size_t bytes_written = 0;
        Random rnd(id);
        SetAffinity(Numa::CpuSequenceRR(id));
        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          static const unsigned int kBranching = 4;
          int height = 1;
          while (height < kMaxHeight && ((rnd.Next() % kBranching) == 0)) {
            height++;
          }
          //const size_t alloc_size = Node::compute_alloc_size(load_keys[i], height);
          const size_t alloc_size = 8 + 8 + 8 + 8 + 8 * height;
          //const size_t alloc_size = 256;

          int s = load_keys[i] % NUM_SHARDS;

          // Write IUL log entry
          int region = GetChip();
          auto paddr = log[s][region]->Allocate(alloc_size);
          char* p = (char*) paddr.get();
          struct Entry {
            uint64_t key;
            uint64_t tag;
            uint64_t value;
            int height;
          };
          Entry* ep = (Entry*) p;
          ep->key = load_keys[i];
          ep->tag = 0;
          ep->value = 0;
          ep->height = height;

          clwb(paddr.get(), alloc_size);
          _mm_sfence();
          bytes_written += alloc_size;
        }
        total_bytes_written.fetch_add(bytes_written);
      });
    }
    for (auto& t : loaders) {
      t.join();
    }
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Load IOPS: %.3lf M\n", NUM_LOADS/dur_sec/1000000);
    fprintf(stdout, "Load Throughput: %.3lf MB/s\n", total_bytes_written.load()/dur_sec/1024/1024);
  }
  fprintf(stdout, "\n");
}

int main() {
  Numa::Init();
  std::vector<uint64_t> load_keys;
  std::vector<OpType> work_ops;
  std::vector<uint64_t> work_keys;
  load_keys.reserve(NUM_LOADS);
  work_ops.reserve(NUM_WORKS);
  work_keys.reserve(NUM_WORKS);
  FillLoadKeys(NUM_LOADS, &load_keys);
  FillWorkKeys('c', NUM_WORKS, &work_ops, &work_keys);

  tbb::task_scheduler_init init(NUM_THREADS);

  Run1(NUM_THREADS, load_keys, work_ops, work_keys);
  Run2(NUM_THREADS, load_keys, work_ops, work_keys);
  return 0;
}
