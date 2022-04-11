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
#include "listdb/lib/random.h"
#include "listdb/listdb.h"
#include "listdb/lsm/table.h"
#include "listdb/lsm/table_list.h"
#include "listdb/util.h"

constexpr int NUM_THREADS = 40;
constexpr size_t NUM_LOADS = 80 * 1000 * 1000;
constexpr size_t NUM_WORKS = 1 * 1000 * 1000;

#define QUERY_DISTRIBUTION "unif"
//#define QUERY_DISTRIBUTION "zipf"

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

void FillWorkKeys(const size_t num_works, std::vector<OpType>* work_ops,
                  std::vector<uint64_t>* work_keys, const std::string& filename) {
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
  fprintf(stdout, "\rFilling up workload queries: \x1b[32mDONE (count: %zu)\x1b[0m\n", count);
}

void FillLoadKeysReadRatio(const size_t num_loads, const size_t num_works, std::vector<uint64_t>* load_keys, unsigned int read_ratio) {
  std::stringstream ss;
  ss << "/home/wkim/RECIPE/index-microbench/workloads_rw_ratio_unif/";
  ss << "load_r" << read_ratio << "_unif_int_" << (num_loads / 1000 / 1000) << "M_" << (num_works / 1000 / 1000) << "M";
  FillLoadKeys(num_loads, load_keys, ss.str());
}

void FillWorkKeysReadRatio(const size_t num_loads, const size_t num_works, std::vector<OpType>* work_ops,
                           std::vector<uint64_t>* work_keys, unsigned int read_ratio) {
  std::stringstream ss;
  ss << "/home/wkim/RECIPE/index-microbench/workloads_rw_ratio_unif/";
  ss << "run_r" << read_ratio << "_unif_int_" << (num_loads / 1000 / 1000) << "M_" << (num_works / 1000 / 1000) << "M";
  FillWorkKeys(num_works, work_ops, work_keys, ss.str());
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

void Run1(const int num_threads, const int num_shards, const std::vector<uint64_t>& load_keys, const std::vector<OpType>& work_ops,
          const std::vector<uint64_t>& work_keys) {

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
  
}

void ParseCLA(int argc, char* argv[], std::unordered_map<std::string, std::string>* props) {
  struct option long_options[] = {
    { "num_threads", required_argument, 0, 0 },
    { "num_shards", required_argument, 0, 0 },
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
  for (auto& it : props) {
    std::cout << it.first << ": " << it.second << std::endl;
    if (it.first == "num_threads") {
      num_threads = std::stoi(it.second);
    } else if (it.first == "num_shards") {
      num_shards = std::stoi(it.second);
    }
  }
  Numa::Init();
  std::vector<uint64_t> load_keys;
  std::vector<OpType> work_ops;
  std::vector<uint64_t> work_keys;
  load_keys.reserve(NUM_LOADS);
  work_ops.reserve(NUM_WORKS);
  work_keys.reserve(NUM_WORKS);
  const int read_ratio = 100;
  FillLoadKeysReadRatio(NUM_LOADS, NUM_WORKS, &load_keys, read_ratio);
  FillWorkKeysReadRatio(NUM_LOADS, NUM_WORKS, &work_ops, &work_keys, read_ratio);

  Run1(num_threads, num_shards, load_keys, work_ops, work_keys);

  return 0;
}
