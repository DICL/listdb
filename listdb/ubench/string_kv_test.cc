#if defined(LISTDB_STRING_KEY) && defined(LISTDB_WISCKEY)

#include <chrono>
#include <cstdio>
#include <fstream>
#include <future>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <shared_mutex>
#include <thread>
#include <vector>
#include <experimental/filesystem>
#include <unordered_map>

#include <getopt.h>

#include "listdb/common.h"
#include "listdb/core/pmem_log.h"
#include "listdb/db_client.h"
#include "listdb/index/braided_pmem_skiplist.h"
#include "listdb/index/lockfree_skiplist.h"
#include "listdb/lib/numa.h"
#include "listdb/listdb.h"
#include "listdb/lsm/table.h"
#include "listdb/lsm/table_list.h"
#include "listdb/util.h"
#include "listdb/util/random.h"

//#define COUNT_FOUND

#define LOAD_FILE "/home/wkim/RECIPE/index-microbench/workloads_10M_1M_zipf/ycsbkey_load_workloadc_zipf"
#define WORK_FILE "/home/wkim/RECIPE/index-microbench/workloads_10M_1M_zipf/ycsbkey_run_workloadc_zipf"

constexpr int NUM_THREADS = 40;
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

void FillLoadKeys(const size_t num_loads, std::vector<Key>* load_keys, const std::string& filename) {
  std::ifstream istrm(filename);
  size_t count = 0;
  size_t epoch = 10;
  while ((count < num_loads) && istrm.good()) {
    std::string op;
    std::string key;
    istrm >> op >> key;
    if (op == "INSERT") {
      load_keys->emplace_back(key);
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

void FillWorkKeys(const size_t num_works, std::vector<OpType>* work_ops,
                  std::vector<Key>* work_keys, const std::string& filename) {
  std::ifstream istrm(filename);
  size_t count = 0;
  size_t epoch = 10;
  while ((count < num_works) && istrm.good()) {
    std::string op;
    std::string key;
    istrm >> op >> key;
    if (op == "INSERT") {
      work_ops->push_back(OP_INSERT);
      work_keys->emplace_back(key);
    } else if (op == "UPDATE") {
      work_ops->push_back(OP_UPDATE);
      work_keys->emplace_back(key);
    } else if (op == "READ") {
      work_ops->push_back(OP_READ);
      work_keys->emplace_back(key);
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
  fprintf(stdout, "\rFilling up workload queries: \x1b[32mDONE (count: %zu)\x1b[0m\n", count);
}

void Run2(const int num_threads, const int num_shards, const std::vector<Key>& load_keys, const std::vector<OpType>& work_ops,
          const std::vector<Key>& work_keys) {
  fprintf(stdout, "=== ListDB (%d-shard) ===\n", num_shards);

  ListDB* db = new ListDB();
  db->Init();
  
  // Load
  {
    printf("Load %zu items\n", NUM_LOADS);

    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> loaders;
    const size_t num_ops_per_thread = NUM_LOADS / num_threads;
    for (int id = 0; id < num_threads; id++) {
      loaders.emplace_back([&, id] {
        SetAffinity(Numa::CpuSequenceRR(id));
        int r = GetChip();
        DBClient* client = new DBClient(db, id, r);

        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          client->PutStringKV(load_keys[i].data(), load_keys[i].data());
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

  std::this_thread::sleep_for(std::chrono::seconds(3));
  for (int i = 0; i < num_shards; i++) {
    db->ManualFlushMemTable(i);
  }
  std::this_thread::sleep_for(std::chrono::seconds(35));
  db->PrintDebugLsmState(0);

  // Work
  {
    printf("WORK %zu queries\n", NUM_WORKS);
    //std::vector<std::chrono::duration<double>> latency;
    //latency.reserve(NUM_WORKS);
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
#ifdef COUNT_FOUND
    std::vector<int> cnt(num_threads);
#endif
    std::vector<size_t> pmem_get_cnt(num_threads);
    std::vector<size_t> search_visit_cnt(num_threads);
    std::vector<size_t> height_visit_cnt[kMaxHeight];
    for (int i = 0; i < kMaxHeight; i++) {
      height_visit_cnt[i].reserve(num_threads);
      for (int j = 0; j < num_threads; j++) {
        height_visit_cnt[i][j] = 0;
      }
    }
    const size_t num_ops_per_thread = NUM_WORKS / num_threads;
    for (int id = 0; id < num_threads; id++) {
      workers.emplace_back([&, id] {
        SetAffinity(Numa::CpuSequenceRR(id));
        int r = GetChip();
        DBClient* client = new DBClient(db, id, r);

        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          //auto query_begin = std::chrono::steady_clock::now();
          if (work_ops[i] == OP_INSERT || work_ops[i] == OP_UPDATE) {
            client->PutStringKV(work_keys[i].data(), work_keys[i].data());
          } else if (work_ops[i] == OP_READ) {
            uint64_t val_read;
#ifndef COUNT_FOUND
            [[maybe_unused]] auto ret = client->GetStringKV(work_keys[i].data(), &val_read);
#else
            auto ret = client->Get(work_keys[i].data(), &val_read);
            if (ret) cnt[id]++;
#endif
#if 0
            if (ret) {
              PmemPtr value_paddr(val_read);
              char* value_buf = (char*) value_paddr.get();
              std::string_view value_sv(value_buf + 8, *((size_t*) value_buf));
              fprintf(stdout, "key: %s, value: %s\n", work_keys[i].data(), value_sv.data());
            }
#endif
          }
          //latency[i] = std::chrono::steady_clock::now() - query_begin;
        }

        pmem_get_cnt[id] = client->pmem_get_cnt();
        search_visit_cnt[id] = client->search_visit_cnt();
        for (int h = 0; h < kMaxHeight; h++) {
          height_visit_cnt[h][id] = client->height_visit_cnt(h);
        }
      });
    }
    for (auto& t :  workers) {
      t.join();
    }
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Work IOPS: %.3lf M\n", NUM_WORKS/dur_sec/1000000);
#ifdef COUNT_FOUND
    int cnt_sum = 0;
    for (int i = 0; i < num_threads; i++) {
      cnt_sum += cnt[i];
    }
    fprintf(stdout, "Found %d\n", cnt_sum);
#endif

    size_t pmem_get_cnt_total = 0;
    size_t search_visit_cnt_total = 0;
    size_t height_visit_cnt_total[kMaxHeight] = {};
    for (int i = 0; i < num_threads; i++) {
      pmem_get_cnt_total += pmem_get_cnt[i];
      search_visit_cnt_total += search_visit_cnt[i];
      for (int h = 0; h < kMaxHeight; h++) {
        height_visit_cnt_total[h] += height_visit_cnt[h][i];
      }
    }
    fprintf(stdout, "Number of queries fallen back to pmem search: %zu\n", pmem_get_cnt_total);
    fprintf(stdout, "Pmem node visit count for queries fallen back to pmem search: %zu\n", search_visit_cnt_total);
    fprintf(stdout, "Avg. Pmem node visit count per query fallen back to pmem search: %.3lf\n", (double) search_visit_cnt_total / pmem_get_cnt_total);
    for (int h = 0; h < kMaxHeight; h++) {
      fprintf(stdout, "height: %d - Avg. Pmem node visit count per query fallen back to pmem search: %.3lf\n", h + 1, (double) height_visit_cnt_total[h] / pmem_get_cnt_total);
    }
#ifdef LISTDB_L1_LRU
    fprintf(stdout, "DRAM COPY LAYER SIZE = %zu\n", db->total_sorted_arr_size());
#endif
  }
  fprintf(stdout, "\n");
  delete db;
}

void ParseCLA(int argc, char* argv[], std::unordered_map<std::string, std::string>* props) {
  struct option long_options[] = {
    { "num_threads", required_argument, 0, 0 },
    { "num_shards", required_argument, 0, 0 },
    { "read_ratio", required_argument, 0, 0 },
    { 0, 0, 0, 0 }
  };
  const static char* optstring = "";
  int c;
  int i;
  while ((c = getopt_long(argc, argv, optstring, long_options, &i)) != -1) {
    switch (c) {
      case 0: {
        if (long_options[i].flag != 0) {
          break;
        }
        props->emplace(long_options[i].name, optarg);
        break;
      }
      default: {
        abort();
      }
    }
  }
}

int main(int argc, char* argv[]) {
  std::unordered_map<std::string, std::string> props;
  ParseCLA(argc, argv, &props);
  int num_threads = NUM_THREADS;
  int num_shards = NUM_SHARDS;
  unsigned int read_ratio = 100;
  for (auto& it : props) {
    std::cout << it.first << ": " << it.second << std::endl;
    if (it.first == "num_threads") {
      num_threads = std::stoi(it.second);
    } else if (it.first == "num_shards") {
      num_shards = std::stoi(it.second);
    } else if (it.first == "read_ratio") {
      read_ratio = std::stoi(it.second);
    }
  }

  fprintf(stdout, "num_threads=%d\nread_ratio=%d\n", num_threads, read_ratio);

  Numa::Init();
  std::vector<Key> load_keys;
  std::vector<OpType> work_ops;
  std::vector<Key> work_keys;
  load_keys.reserve(NUM_LOADS);
  work_ops.reserve(NUM_WORKS);
  work_keys.reserve(NUM_WORKS);
  FillLoadKeys(NUM_LOADS, &load_keys, LOAD_FILE);
  FillWorkKeys(NUM_WORKS, &work_ops, &work_keys, WORK_FILE);

  Run2(num_threads, num_shards, load_keys, work_ops, work_keys);

  return 0;
}
#else
#include <iostream>
int main(int argc, char* argv[]) {
  std::cerr << "Compile with both LISTDB_STRING_KEY and LISTDB_WISCKEY defined: cmake .. -DSTRING_KEY=ON -DWISCKEY=ON\n";
  return 1;
}
#endif
