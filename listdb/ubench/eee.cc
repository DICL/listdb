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

#define QUERY_DISTRIBUTION "unif"
//#define QUERY_DISTRIBUTION "zipf"

//#define LOAD_FILE "/home/wkim/RECIPE/index-microbench/workloads_10M_1M_unif/loada_unif_int.dat"
//#define LOAD_FILE "/home/wkim/RECIPE/index-microbench/workloads_10M_1M_zipf/loada_zipf_int.dat"
#define LOAD_FILE "/home/wkim/RECIPE/index-microbench/workloads_100M_10M_unif/loada_unif_int.dat"
//#define LOAD_FILE "/home/wkim/RECIPE/index-microbench/workloads_100M_10M_zipf/loada_zipf_int.dat"
//#define LOAD_FILE "/home/wkim/RECIPE/index-microbench/workloads_100M_100M_zipf/loada_zipf_int.dat"

//#define WORK_FILE_PREFIX "/home/wkim/RECIPE/index-microbench/workloads_10M_1M_unif/txns"
//#define WORK_FILE_PREFIX "/home/wkim/RECIPE/index-microbench/workloads_10M_1M_zipf/txns"
#define WORK_FILE_PREFIX "/home/wkim/RECIPE/index-microbench/workloads_100M_10M_unif/txns"
//#define WORK_FILE_PREFIX "/home/wkim/RECIPE/index-microbench/workloads_100M_10M_zipf/txns"
//#define WORK_FILE_PREFIX "/home/wkim/RECIPE/index-microbench/workloads_100M_100M_zipf/txns"

constexpr int NUM_THREADS = 40;
constexpr size_t NUM_LOADS = 100 * 1000 * 1000;
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

void FillLoadKeys(const size_t num_loads, std::vector<uint64_t>* load_keys) {
  std::string filename = LOAD_FILE;
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
  ss << WORK_FILE_PREFIX << workload_type << "_" << QUERY_DISTRIBUTION << "_int.dat";
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
  fprintf(stdout, "\rFilling up workload queries: \x1b[32mDONE (count: %zu)\x1b[0m\n", count);
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
  fprintf(stdout, "=== BraidedPmemSkipList (%d-shard) ===\n", num_shards);

  InitPoolSet();
  BraidedPmemSkipList* skiplist[num_shards];
  PmemLog* arena[num_shards][kNumRegions];
  for (int i = 0; i < num_shards; i++) {
    auto sl = new BraidedPmemSkipList(pool_id_table[0]);
    for (int j = 0; j < kNumRegions; j++) {
      arena[i][j] = new PmemLog(pool_id_table[j], i);
      sl->BindArena(pool_id_table[j], arena[i][j]);
    }
    sl->Init();
    skiplist[i] = sl;
  }

  using Node = BraidedPmemSkipList::Node;
  uint64_t shard_size = std::numeric_limits<uint64_t>::max() / num_shards;
  
  // Load
  {
    printf("Load %zu items\n", NUM_LOADS);
    drop_cache();
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> loaders;
    const size_t num_ops_per_thread = NUM_LOADS / num_threads;
    for (int id = 0; id < num_threads; id++) {
      loaders.emplace_back([&, id] {
        Random rnd(id);
        SetAffinity(Numa::CpuSequenceRR(id));
        int r = GetChip();

        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          static const unsigned int kBranching = 4;
          int height = 2;
          //if (height < kMaxHeight && ((rnd.Next() % (kBranching/2)) == 0)) {
          //  height++;
            while (height < kMaxHeight && ((rnd.Next() % kBranching) == 0)) {
              height++;
            }
          //}
          //const size_t alloc_size = Node::compute_alloc_size(load_keys[i], height);
          //size_t node_size = util::AlignedSize(8, sizeof(Node) + (height-1)*sizeof(uint64_t));
          size_t node_size = sizeof(Node) + (height-1)*sizeof(uint64_t);
          
          //int s = load_keys[i] % num_shards;
          int s = load_keys[i] / shard_size;

#if 1
          auto node_paddr = arena[s][r]->Allocate(node_size);
          Node* node = (Node*) node_paddr.get();
#else
          auto pool = Pmem::pool<pmem_log_root>(r);
          pmem::obj::persistent_ptr<char[]> p_buf;
          pmem::obj::make_persistent_atomic<char[]>(pool, p_buf, node_size);
          Node* node = (Node*) p_buf.get();
          PmemPtr node_paddr = PmemPtr(r, ((uintptr_t) node - (uintptr_t) pool.handle()));
#endif
          //Node* node = Node::init_node((char*) buf, load_keys[i], (1ull<<8|kTypeValue), 0, height);
          node->key = load_keys[i];
          node->tag = height;
          node->value = load_keys[i];
          skiplist[s]->Insert(node_paddr);

          //auto ret = skiplist[s]->Lookup(load_keys[i], r);
          //Node* found = (Node*) ret.get();
          //if (!found) {
          //  fprintf(stdout, "Lookup returns NULL for key: %zu\n", load_keys[i]);
          //} else if (load_keys[i] != *((uint64_t*) &found->key)) {
          //  for (auto& lk : load_keys) {
          //    if (lk == load_keys[i]) {
          //      fprintf(stdout, "Key not found %zu (found = %zu)\n", load_keys[i], found->key);
          //      fprintf(stdout, "Inserted\n");
          //      exit(1);
          //    }
          //  }
          //} else if (found->key.Compare(found->value) != 0) {
          //  fprintf(stdout, "Invalid KV-pair found { %zu, %zu } (query key: %zu)\n", found->key, found->value, load_keys[i]);
          //} else {
          //  //fprintf(stdout, "Key found %zu\n", load_keys[i]);
          //}
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

  // Work
  {
    printf("WORK %zu queries\n", NUM_WORKS);
    //drop_cache();
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    std::vector<int> cnt(num_threads);
    const size_t num_ops_per_thread = NUM_WORKS / num_threads;
    for (int id = 0; id < num_threads; id++) {
       workers.emplace_back([&, id] {
        SetAffinity(Numa::CpuSequenceRR(id));
        int r = GetChip();

        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          //int s = work_keys[i] % num_shards;
          int s = work_keys[i] / shard_size;
          skiplist[s]->Lookup(work_keys[i], r);
          //auto ret = skiplist[s]->Lookup(work_keys[i], r);
          //Node* found = (Node*) ret.get();
          //if (found && work_keys[i] == (uint64_t) found->key) {
          //  cnt[id]++;
          //}
          //auto ret = skiplist[s]->Lookup(work_keys[i], r);
          //Node* found = (Node*) ret.get();
          //if (!found) {
          //  fprintf(stdout, "Lookup returns NULL for key: %zu\n", work_keys[i]);
          //} else if (work_keys[i] != *((uint64_t*) &found->key)) {
          //  for (auto& lk : load_keys) {
          //    if (lk == work_keys[i]) {
          //      fprintf(stdout, "Key not found %zu (found = %zu)\n", work_keys[i], found->key);
          //      fprintf(stdout, "Inserted\n");
          //      exit(1);
          //    }
          //  }
          //} else if (found->key.Compare(found->value) != 0) {
          //  fprintf(stdout, "Invalid KV-pair found { %zu, %zu } (query key: %zu)\n", found->key, found->value, work_keys[i]);
          //}
        }
      });
    }
    for (auto& t :  workers) {
      t.join();
    }
    int cnt_sum = 0;
    for (int i = 0; i < num_threads; i++) {
      cnt_sum += cnt[i];
    }
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Work IOPS: %.3lf M\n", NUM_WORKS/dur_sec/1000000);
    //fprintf(stdout, "Found %d\n", cnt_sum);
  }
  fprintf(stdout, "\n");
}

void Run2(const int num_threads, const int num_shards, const std::vector<uint64_t>& load_keys, const std::vector<OpType>& work_ops,
          const std::vector<uint64_t>& work_keys) {
  fprintf(stdout, "=== ListDB (%d-shard, MemTable) ===\n", num_shards);

  ListDB* db = new ListDB();
  db->Init();
  
  // Load
  {
    printf("Load %zu items\n", NUM_LOADS);
    drop_cache();

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
          //uint64_t val_read;
          //client->Get(load_keys[i], &val_read);
          //if (load_keys[i] != val_read) {
          //  fprintf(stdout, "SOMETHING WENT WRONG\n");
          //}
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
    std::vector<std::chrono::duration<double>> latency;
    latency.reserve(NUM_WORKS);
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    std::vector<int> cnt(num_threads);
    const size_t num_ops_per_thread = NUM_WORKS / num_threads;
    for (int id = 0; id < num_threads; id++) {
       workers.emplace_back([&, id] {
        SetAffinity(Numa::CpuSequenceRR(id));
        int r = GetChip();
        DBClient* client = new DBClient(db, id, r);

        //cnt[id] = 0;
        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          auto query_begin = std::chrono::steady_clock::now();
          if (work_ops[i] == OP_INSERT || work_ops[i] == OP_UPDATE) {
            client->Put(work_keys[i], work_keys[i]);
          } else if (work_ops[i] == OP_READ) {
            uint64_t val_read;
            client->Get(work_keys[i], &val_read);
            //auto ret = client->Get(work_keys[i], &val_read);
            //if (ret) cnt[id]++;
          }
          latency[i] = std::chrono::steady_clock::now() - query_begin;
        }
      });
    }
    for (auto& t :  workers) {
      t.join();
    }
    int cnt_sum = 0;
    for (int i = 0; i < num_threads; i++) {
      cnt_sum += cnt[i];
    }
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Work IOPS: %.3lf M\n", NUM_WORKS/dur_sec/1000000);
    //fprintf(stdout, "Found %d\n", cnt_sum);

    std::fstream strm("latency.out", strm.out);
    for (size_t i = 0; i < NUM_WORKS; i++) {
      strm << latency[i].count() << std::endl;
    }
    strm.close();
  }
  fprintf(stdout, "\n");
  delete db;
}

void Run3(const int num_threads, const int num_shards, const std::vector<uint64_t>& load_keys, const std::vector<OpType>& work_ops,
          const std::vector<uint64_t>& work_keys) {
  fprintf(stdout, "=== lockfree_skiplist (%d-shard) ===\n", num_shards);

  lockfree_skiplist skiplist[num_shards];
  using Node = lockfree_skiplist::Node;
  
  // Load
  {
    drop_cache();
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> loaders;
    const size_t num_ops_per_thread = NUM_LOADS / num_threads;
    for (int id = 0; id < num_threads; id++) {
      loaders.emplace_back([&, id] {
        Random rnd(id);
        SetAffinity(Numa::CpuSequenceRR(id));
        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          static const unsigned int kBranching = 4;
          int height = 1;
          while (height < kMaxHeight && ((rnd.Next() % kBranching) == 0)) {
            height++;
          }
          const size_t alloc_size = Node::compute_alloc_size(load_keys[i], height);
          
          int s = load_keys[i] % num_shards;

          void* buf = aligned_alloc(8, alloc_size);
          Node* node = Node::init_node((char*) buf, load_keys[i], 0, kTypeValue, height, 0);
          //node->log_paddr = 0;
          skiplist[s].Insert(node);
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
  {
    printf("WORK %zu queries\n", NUM_WORKS);
    drop_cache();
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    std::vector<int> cnt(num_threads);
    const size_t num_ops_per_thread = NUM_WORKS / num_threads;
    for (int id = 0; id < num_threads; id++) {
       workers.emplace_back([&, id] {
        SetAffinity(Numa::CpuSequenceRR(id));
        //int r = GetChip();

        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          int s = work_keys[i] % num_shards;
          skiplist[s].Lookup(work_keys[i]);
        }
      });
    }
    for (auto& t :  workers) {
      t.join();
    }
    int cnt_sum = 0;
    for (int i = 0; i < num_threads; i++) {
      cnt_sum += cnt[i];
    }
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Work IOPS: %.3lf M\n", NUM_WORKS/dur_sec/1000000);
    //fprintf(stdout, "Found %d\n", cnt_sum);
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
  FillLoadKeys(NUM_LOADS, &load_keys);
  FillWorkKeys('a', NUM_WORKS, &work_ops, &work_keys);

  //Run1(num_threads, num_shards, load_keys, work_ops, work_keys);
  Run2(num_threads, num_shards, load_keys, work_ops, work_keys);
  //Run3(num_threads, num_shards, load_keys, work_ops, work_keys);

  return 0;
}
