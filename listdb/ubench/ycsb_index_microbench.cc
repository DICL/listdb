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

#include <gflags/gflags.h>

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

DEFINE_string(workload, "c", "ycsb a b c d ");

DEFINE_string(query_dist, "zipfian", "uniform zipfian");

DEFINE_int64(loads, 100'000'000, "Number of key/values to place in database");

DEFINE_int64(works, 10'000'000, "Number of read operations to do.  "
             "If negative, do FLAGS_num reads.");

DEFINE_int32(threads, 80, "Number of concurrent threads to run.");

DEFINE_int32(shards, kNumShards, "num shards");

#if 0
DEFINE_string(load_history_file, "", "query history file");

DEFINE_string(work_history_file, "", "query history file");
#endif

DEFINE_string(report_file, "", "report_file");

DEFINE_bool(load_only, false, "load only");

DEFINE_string(workload_dir, "", "example) ~/RECIPE/index-microbench/workloads_100M_10M_zipf");

DEFINE_string(bind_type, "cpu_numa_rr", "worker thread bind type: <cpu_numa_rr|numa_rr>");

//#define COUNT_FOUND

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

#if 0
void FlushHistoryFile(std::vector<uint64_t>& hist, uint64_t begin_nano, int64_t size, std::string file) {
  std::cout << "FlushHistoryFile is not implemented yet.\n";
}
#endif

void Run2(const int num_threads, const int num_shards, const std::vector<uint64_t>& load_keys, const std::vector<OpType>& work_ops,
          const std::vector<uint64_t>& work_keys) {
  fprintf(stdout, "=== ListDB (%d-shard) ===\n", num_shards);

  ListDB* db = new ListDB();
  db->Init();

  Reporter* reporter = nullptr;
  if (FLAGS_report_file != "") {
    reporter = db->GetOrCreateReporter(FLAGS_report_file);
    reporter->Start();
  }
  // make these default ON(=1)
  // reporter->SetInternalCollectionState("memtable_flush", 1);
  // reporter->SetInternalCollectionState("l0_compaction", 1);

#if 0
  std::vector<uint64_t> load_time;
  std::vector<uint64_t> work_time;
  bool record_load_history = (FLAGS_load_history_file == "") ? false : true;
  if (record_load_history) {
    load_time.resize(FLAGS_loads);
  }
  bool record_work_history = (FLAGS_work_history_file == "" && !FLAGS_load_only) ? false : true;
  if (record_work_history) {
    work_time.resize(FLAGS_works);
  }
#endif

  enum class CpuBindType {
    kNone,
    kCpuNumaRoundRobin,
    kNumaRoundRobin
  };
  CpuBindType bind_type;
  //int num_cpus = numa_num_configured_cpus();
  //int num_sockets = numa_num_configured_nodes();
  std::cout << "Thread bind type: ";
  if (FLAGS_bind_type == "cpu_numa_rr") {
    bind_type = CpuBindType::kCpuNumaRoundRobin;
    std::cout << "CpuBindType::kCpuNumaRoundRobin";
  } else if (FLAGS_bind_type == "numa_rr") {
    bind_type = CpuBindType::kNumaRoundRobin;
    std::cout << "CpuBindType::kNumaRoundRobin";
  } else {
    bind_type = CpuBindType::kNone;
    std::cout << "CpuBindType::kNone";
  }
  std::cout << std::endl;
  
  // Load
  {
    printf("Load %zu items\n", FLAGS_loads);

    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> loaders;
    const size_t num_ops_per_thread = FLAGS_loads / num_threads;
    for (int id = 0; id < num_threads; id++) {
      loaders.emplace_back([&, id] {
        if (bind_type == CpuBindType::kCpuNumaRoundRobin) {
          SetAffinity(Numa::CpuSequenceRR(id));
        } else if (bind_type == CpuBindType::kNumaRoundRobin) {
          numa_run_on_node(id % kNumRegions);
        }
        int r = GetChip();
        DBClient* client = new DBClient(db, id, r);

        ReporterClient* reporter_client = (reporter != nullptr) ? new ReporterClient(reporter) : nullptr;

        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          client->Put(load_keys[i], load_keys[i]);
#if 0
          //if (record_load_history) {
          //  load_time[i] = Clock::NowNanos();
          //}
#endif
          if (reporter_client != nullptr) {
            reporter_client->ReportFinishedOps(Reporter::OpType::kPut, 1);
          }
        }
        delete reporter_client;
      });
    }
    for (auto& t : loaders) {
      t.join();
    }
    auto end_tp = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = end_tp - begin_tp;
    double dur_sec = dur.count();
    fprintf(stdout, "Load IOPS: %.3lf M\n", FLAGS_loads/dur_sec/1000000);

    //uint64_t load_begin = std::chrono::duration_cast<std::chrono::nanoseconds>(begin_tp.time_since_epoch()).count();
    //FlushHistoryFile(load_time, load_begin, FLAGS_loads, FLAGS_load_history_file);
    std::string flush_stat;
    db->GetStatString("flush_stats", &flush_stat);
    fprintf(stdout, "%s\n", flush_stat.c_str());
  }
  fprintf(stdout, "\n");

  if (!FLAGS_load_only) {
    std::this_thread::sleep_for(std::chrono::seconds(3));
    for (int i = 0; i < num_shards; i++) {
      db->ManualFlushMemTable(i);
    }
    std::this_thread::sleep_for(std::chrono::seconds(20));
    db->PrintDebugLsmState(0);
  }

  // Work
  if (!FLAGS_load_only) {
    printf("WORK %zu queries\n", FLAGS_works);
    //std::vector<std::chrono::duration<double>> latency;
    //latency.reserve(FLAGS_works);
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
    const size_t num_ops_per_thread = FLAGS_works / num_threads;
    for (int id = 0; id < num_threads; id++) {
      workers.emplace_back([&, id] {
        if (bind_type == CpuBindType::kCpuNumaRoundRobin) {
          SetAffinity(Numa::CpuSequenceRR(id));
        } else if (bind_type == CpuBindType::kNumaRoundRobin) {
          numa_run_on_node(id % kNumRegions);
        }
        int r = GetChip();
        DBClient* client = new DBClient(db, id, r);

        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          //auto query_begin = std::chrono::steady_clock::now();
          if (work_ops[i] == OP_INSERT || work_ops[i] == OP_UPDATE) {
            client->Put(work_keys[i], work_keys[i]);
          } else if (work_ops[i] == OP_READ) {
            uint64_t val_read;
#ifndef COUNT_FOUND
            client->Get(work_keys[i], &val_read);
#else
            auto ret = client->Get(work_keys[i], &val_read);
            if (ret) cnt[id]++;
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
    fprintf(stdout, "Work IOPS: %.3lf M\n", FLAGS_works/dur_sec/1000000);
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
  std::string buf;
  db->GetStatString("l1_cache_size", &buf);
  fprintf(stdout, "%s\n", buf.c_str());
  delete db;
}

std::string GetFileName(const std::string& base, bool is_load, const std::string& type, const std::string& query_dist) {
  std::string_view dist_short(query_dist.data(), 4);
  std::stringstream ss;
  ss << base << "/" << (is_load ? "load" : "txns") << type << "_" << dist_short << "_int.dat";
  return ss.str();
}

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  int num_threads = FLAGS_threads;
  int num_shards = FLAGS_shards;

  Numa::Init();
  std::vector<uint64_t> load_keys;
  std::vector<OpType> work_ops;
  std::vector<uint64_t> work_keys;
  load_keys.reserve(FLAGS_loads);
  work_ops.reserve(FLAGS_works);
  work_keys.reserve(FLAGS_works);

  auto load_file = GetFileName(FLAGS_workload_dir, true, FLAGS_workload, FLAGS_query_dist);
  auto work_file = GetFileName(FLAGS_workload_dir, false, FLAGS_workload, FLAGS_query_dist);
  std::cout << load_file << std::endl;
  std::cout << work_file << std::endl;

  FillLoadKeys(FLAGS_loads, &load_keys, load_file);
  FillWorkKeys(FLAGS_works, &work_ops, &work_keys, work_file);

  Run2(num_threads, num_shards, load_keys, work_ops, work_keys);

  return 0;
}
