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

//#define LOAD_FILE "/home/wkim/RECIPE/index-microbench/workloads_100M_1M_zipf/loada_zipf_int.dat"
#define LOAD_FILE "/home/wkim/RECIPE/index-microbench/workloads_100M_10M_zipf/loada_zipf_int.dat"

constexpr int NUM_THREADS = 80;
constexpr size_t NUM_LOADS = 100 * 1000 * 1000;
//constexpr size_t NUM_LOADS = 1;
constexpr size_t NUM_WORKS = 10 * 1000 * 1000;

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

void FillLoadKeys(const size_t num_loads, std::vector<uint64_t>* load_keys, const std::string& filename) {
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

void Run2(const int num_threads, const int num_shards, const std::vector<uint64_t>& load_keys) {
  fprintf(stdout, "=== ListDB (%d-shard) ===\n", num_shards);

  ListDB* db = new ListDB();
  db->Init();
  //db->SetL0CompactionSchedulerStatus(ListDB::ServiceStatus::kStop);
  
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
          client->Put(load_keys[i], load_keys[i]);
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

  //std::this_thread::sleep_for(std::chrono::seconds(3));
  //for (int i = 0; i < num_shards; i++) {
  //  db->ManualFlushMemTable(i);
  //}
  //std::this_thread::sleep_for(std::chrono::seconds(35));
  db->PrintDebugLsmState(0);
#if 1
  std::this_thread::sleep_for(std::chrono::seconds(3));
  for (int i = 0; i < num_shards; i++) {
    db->ManualFlushMemTable(i);
  }
  std::this_thread::sleep_for(std::chrono::seconds(10));
  db->PrintDebugLsmState(0);
#endif

  fprintf(stdout, "> delete db;\n");
  delete db;

  fprintf(stdout, "> db = new ListDB();\n");
  db = new ListDB();
  fprintf(stdout, "> db->Open();\n");
  auto open_begin_tp = std::chrono::steady_clock::now();
  db->Open();
  auto open_end_tp = std::chrono::steady_clock::now();
  std::chrono::duration<double> open_dur = open_end_tp - open_begin_tp;
  fprintf(stdout, "Open() time: %.3lf sec\n", open_dur.count());
  fprintf(stdout, "> db->PrintDebugLsmState(0);\n");
  db->PrintDebugLsmState(0);
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
  std::vector<uint64_t> load_keys;
  load_keys.reserve(NUM_LOADS);
  FillLoadKeys(NUM_LOADS, &load_keys, LOAD_FILE);

  Run2(num_threads, num_shards, load_keys);

  return 0;
}
