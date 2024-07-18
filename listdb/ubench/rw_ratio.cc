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

#include <algorithm>

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

//#define QUERY_DISTRIBUTION "unif"
//#define QUERY_DISTRIBUTION "zipf"

constexpr int NUM_THREADS = 60;
constexpr size_t NUM_LOADS = 100 * 1000 * 1000;
constexpr size_t NUM_WORKS = 10 * 1000 * 1000;

//for user behavior
constexpr size_t NUM_LOADS1 = 200 * 1000 * 1000;
constexpr size_t NUM_LOADS2 = 200 * 1000 * 1000;
constexpr size_t NUM_LOADS3 = 200 * 1000 * 1000;
constexpr size_t NUM_WORKS1 = 200 * 1000 * 1000;
constexpr size_t NUM_WORKS2 = 200 * 1000 * 1000;
constexpr int LOAD1_TIME = 60;
constexpr int LOAD2_TIME = 60;
constexpr int LOAD3_TIME = 60;
constexpr int WORK1_TIME = 80;
constexpr int WORK2_TIME = 80;

constexpr int SLEEP_TIME = 20;//time to waiting l0 compactions end
constexpr int SLEEP_TIME2 = 10;//time to waiting l0 compactions end
constexpr int READ_RATIO = 100;//set 200 to do scan

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
                  std::vector<uint64_t>* work_keys, std::vector<uint64_t>* work_scan_nums, const std::string& filename) {
  std::ifstream istrm(filename);
  size_t count = 0;
  size_t epoch = 10;
  while ((count < num_works) && istrm.good()) {
    std::string op;
    uint64_t key;
    uint64_t scan_num;
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
    } else if (op == "SCAN") {
      work_ops->push_back(OP_SCAN);
      work_keys->push_back(key);
      istrm >> scan_num;
      work_scan_nums->push_back(scan_num);
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
  ss << "/home/juwon/RECIPE/index-microbench/ycsb_workloada/"; //test juwon
  ss << "load_r" << read_ratio << "_unif_int_" << (num_loads / 1000 / 1000) << "M_" << (num_works / 1000 / 1000) << "M";
  FillLoadKeys(num_loads, load_keys, ss.str());
}

void FillWorkKeysReadRatio(const size_t num_loads, const size_t num_works, std::vector<OpType>* work_ops,
                           std::vector<uint64_t>* work_keys, std::vector<uint64_t>* work_scan_nums, unsigned int read_ratio) {
  std::stringstream ss;
  ss << "/home/juwon/RECIPE/index-microbench/ycsb_workloada/"; //test juwon
  ss << "run_r" << read_ratio << "_unif_int_" << (num_loads / 1000 / 1000) << "M_" << (num_works / 1000 / 1000) << "M";
  FillWorkKeys(num_works, work_ops, work_keys, work_scan_nums, ss.str());
}

static pmem::obj::pool<pmem_log_root> pool_table[kNumRegions];
static int pool_id_table[kNumRegions];

void InitPoolSet() {
  // Create poolset file
  for (int i = 0; i < kNumRegions; i++) {
    std::stringstream pss;
    pss << "/mnt/pmem" << i << "/juwon/pmem_log_test";
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

void Run2(const int num_threads, const int num_shards, const std::vector<uint64_t>& load_keys, const std::vector<OpType>& work_ops,
          const std::vector<uint64_t>& work_keys, const std::vector<uint64_t>& work_scan_nums) {
  fprintf(stdout, "=== ListDB (%d-shard) ===\n", num_shards);

  ListDB* db = new ListDB();
  db->Init();

  //test juwon reporter
  Reporter* reporter = nullptr;
  reporter = db->GetOrCreateReporter("reporter_test_juwon.log");
  reporter->Start();
  
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

        ReporterClient* reporter_client = (reporter != nullptr) ? new ReporterClient(reporter) : nullptr; // test juwon reporter

        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          client->Put(load_keys[i], load_keys[i]);

          //test juwon reporter
          if (reporter_client != nullptr) {
            reporter_client->ReportFinishedOps(Reporter::OpType::kPut, 1);
          }
        }
        delete reporter_client;//test juwon reporter
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

  //if using l0 compaction needs trigger
  std::this_thread::sleep_for(std::chrono::seconds(3));
  db->SetL0CompactionSchedulerStatus(ListDB::ServiceStatus::kActive);

  fprintf(stdout, "sleep %d seconds for l0 compaction end...\n",SLEEP_TIME);
  std::this_thread::sleep_for(std::chrono::seconds(SLEEP_TIME));

  //db->SetL1CompactionSchedulerStatus(ListDB::ServiceStatus::kActive);

  //fprintf(stdout, "sleep %d seconds for l1 compaction end...\n",SLEEP_TIME2);
  //std::this_thread::sleep_for(std::chrono::seconds(SLEEP_TIME2));

  std::this_thread::sleep_for(std::chrono::seconds(3));
  db->PrintDebugLsmState(0);

  // Work
  {


    printf("WORK %zu queries\n", NUM_WORKS);
    //size_t* latency = (size_t*)malloc(sizeof(size_t)*NUM_WORKS); //test juwon 
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
#ifdef COUNT_FOUND
    std::vector<int> cnt(num_threads);
#endif
    //std::vector<size_t> pmem_get_cnt(num_threads);
    //std::vector<size_t> search_visit_cnt(num_threads);
    //std::vector<size_t> height_visit_cnt[kMaxHeight];
    //for (int i = 0; i < kMaxHeight; i++) {
    //  height_visit_cnt[i].reserve(num_threads);
    //  for (int j = 0; j < num_threads; j++) {
    //    height_visit_cnt[i][j] = 0;
    //  }
    //}
    const size_t num_ops_per_thread = NUM_WORKS / num_threads;

    //std::atomic<int> lookup_fail_cnt=0;

    for (int id = 0; id < num_threads; id++) {
      workers.emplace_back([&, id] {
        SetAffinity(Numa::CpuSequenceRR(id));
        int r = GetChip();
        DBClient* client = new DBClient(db, id, r);

        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          //auto query_begin = std::chrono::high_resolution_clock::now(); //test juwon
          if (work_ops[i] == OP_INSERT || work_ops[i] == OP_UPDATE) {
            client->Put(work_keys[i], work_keys[i]);
          } else if (work_ops[i] == OP_READ) {
            uint64_t val_read;
#ifndef COUNT_FOUND
            //if(!client->Get(work_keys[i], &val_read)) lookup_fail_cnt.fetch_add(1);
            client->Get(work_keys[i], &val_read);
#else
            auto ret = client->Get(work_keys[i], &val_read);
            if (ret) cnt[id]++;
#endif
          } else if (work_ops[i] == OP_SCAN) {
            std::vector<uint64_t> val_scan;
            val_scan.reserve(work_scan_nums[i]);
            
            client->Scan(work_keys[i], work_scan_nums[i], &val_scan);
          } 
          //auto query_end = std::chrono::high_resolution_clock::now(); //test juwon
          //latency[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(query_end - query_begin).count(); //test juwon
        }

        //pmem_get_cnt[id] = client->pmem_get_cnt();
        //search_visit_cnt[id] = client->search_visit_cnt();
        //for (int h = 0; h < kMaxHeight; h++) {
        //  height_visit_cnt[h][id] = client->height_visit_cnt(h);
        //}
      });
    }
    
    for (auto& t :  workers) {
      t.join();
    }
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();


    fprintf(stdout, "Work IOPS: %.3lf M\n", NUM_WORKS/dur_sec/1000000);
    //fprintf(stdout,"Lookup fail count : %d\n",lookup_fail_cnt.load());
#ifdef COUNT_FOUND
    int cnt_sum = 0;
    for (int i = 0; i < num_threads; i++) {
      cnt_sum += cnt[i];
    }
    fprintf(stdout, "Found %d\n", cnt_sum);
#endif

    //size_t pmem_get_cnt_total = 0;
    //size_t search_visit_cnt_total = 0;
    //size_t height_visit_cnt_total[kMaxHeight] = {};
    //size_t latency_total = 0; //test juwon
    //for (int i = 0; i < num_threads; i++) {
    //  pmem_get_cnt_total += pmem_get_cnt[i];
    //  search_visit_cnt_total += search_visit_cnt[i];
    //  for (int h = 0; h < kMaxHeight; h++) {
    //    height_visit_cnt_total[h] += height_visit_cnt[h][i];
    //  }
    //}

    //std::sort(latency, latency+NUM_WORKS);
    //for(size_t i=0; i<NUM_WORKS; i++){
    //  latency_total += latency[i];//test juwon
    //}
    //fprintf(stdout, "total latency: %zu\n", latency_total);//test juwon
    //fprintf(stdout, "avg latency: %zu\n", latency_total/NUM_WORKS);//test juwon
    //fprintf(stdout, "P95 latency: %zu\n", latency[(size_t)(NUM_WORKS*0.95-1)]);//test juwon
    //fprintf(stdout, "P99 latency: %zu\n", latency[(size_t)(NUM_WORKS*0.99-1)]);//test juwon
    //fprintf(stdout, "Number of queries fallen back to pmem search: %zu\n", pmem_get_cnt_total);
    //fprintf(stdout, "Pmem node visit count for queries fallen back to pmem search: %zu\n", search_visit_cnt_total);
    //fprintf(stdout, "Avg. Pmem node visit count per query fallen back to pmem search: %.3lf\n", (double) search_visit_cnt_total / pmem_get_cnt_total);
    //for (int h = 0; h < kMaxHeight; h++) {
    //   fprintf(stdout, "height: %d - Avg. Pmem node visit count per query fallen back to pmem search: %.3lf\n", h + 1, (double) height_visit_cnt_total[h] / pmem_get_cnt_total);
    //}
    
  }
  fprintf(stdout, "\n");
  std::string buf;
  db->GetStatString("l1_cache_size", &buf);
  fprintf(stdout, "%s\n", buf.c_str());
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
  // Dangerous: Be very careful with system commands that remove files or directories
    int result = std::system("rm -rf /mnt/pmem*/juwon/*");
    if (result != 0) {
      printf("removing db directory failed!\n");
        // Handle error
    }

  std::unordered_map<std::string, std::string> props;
  ParseCLA(argc, argv, &props);
  int num_threads = NUM_THREADS;
  int num_shards = NUM_SHARDS;
  unsigned int read_ratio = READ_RATIO; // set to 200 for scanning workload (load_r200_unif_int_1M_1M , run_r200_unif_int_1M_1M)
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
  {
    fprintf(stdout, "*** Cache Configurations ***\n");

#ifdef LISTDB_SKIPLIST_CACHE
    fprintf(stdout, "l1_cache_size: %zu bytes\n", kSkipListCacheCapacity);

    std::cout << "kSkipListCacheCardinality: " << kSkipListCacheCardinality << std::endl;
#else
    fprintf(stdout, "l1_cache_size: disabled.\n");
#endif
  }

  Numa::Init();

  std::vector<uint64_t> load_keys;
  std::vector<OpType> work_ops;
  std::vector<uint64_t> work_keys;
  std::vector<uint64_t> work_scan_nums;
  load_keys.reserve(NUM_LOADS);
  work_ops.reserve(NUM_WORKS);
  work_keys.reserve(NUM_WORKS);
  
  //work_scan_nums.reserve(NUM_WORKS);
  FillLoadKeysReadRatio(NUM_LOADS, NUM_WORKS, &load_keys, read_ratio);
  //FillLoadKeys(NUM_LOADS, &load_keys, "/juwon/index-microbench/workloads_rw_ratio_unif/load_r20_unif_int_10M_1M");
  FillWorkKeysReadRatio(NUM_LOADS, NUM_WORKS, &work_ops, &work_keys, &work_scan_nums, read_ratio);
  

  Run2(num_threads, num_shards, load_keys, work_ops, work_keys, work_scan_nums);

  return 0;
}