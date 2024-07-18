#if !defined(GFLAGS)
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run db_bench\n");
  return 1;
}
#elif !defined(LISTDB_STRING_KEY) || !defined(LISTDB_WISCKEY)
#include <cstdio>
int main() {
  fprintf(stderr, "Please configure cmake with -DSTRING_KEY=ON -DWISCKEY=ON to run ycsb_stringkey\n");
  return 1;
}
#else
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

#define KEY_SIZE 23
#define VALUE_SIZE 100

constexpr int NUM_THREADS = 32;
constexpr size_t NUM_LOADS = 100 * 1000 * 1000;
constexpr size_t NUM_WORKS = 100 * 1000 * 1000;
constexpr int SLEEP_TIME = 180;
constexpr int SLEEP_TIME2 = 30;

constexpr int NUM_SHARDS = kNumShards;

std::string_view value; // Declare value outside of main function

enum OpType {
    OP_INSERT,
    OP_UPDATE,
    OP_READ,
    OP_SCAN,
    OP_DELETE
};

OpType parseOperation(const std::string& operation) {
    if (operation == "INSERT") return OP_INSERT;
    if (operation == "UPDATE") return OP_UPDATE;
    if (operation == "READ") return OP_READ;
    if (operation == "SCAN") return OP_SCAN;
    std::cerr << "Invalid operation: " << operation << std::endl;
    exit(1);
}

int listdb_Put(DBClient* client, const std::string_view& key, const std::string_view& value) {
    client->PutStringKV(key, value);
    return 0;
  }

int listdb_Get(DBClient* client, const std::string_view& key, std::string* value) {
uint64_t value_addr;
bool ret = client->GetStringKV(key, &value_addr);
if (ret) {
    char* p = (char*) value_addr;
    size_t val_len = *((uint64_t*) p);
    p += sizeof(size_t);
    value->assign(p, val_len);
    return 0;
}
return 1;
  }


void Run(const int num_threads, const int num_shards, const std::vector<std::string_view>& load_keys, const std::vector<OpType>& work_ops,
          const std::vector<std::string_view>& work_keys) {
  fprintf(stdout, "=== ListDB (%d-shard) ===\n", num_shards);

  ListDB* db = new ListDB();
  db->Init();
  
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
            listdb_Put(client, load_keys[i], value);
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

  std::this_thread::sleep_for(std::chrono::seconds(10));
  for (int i = 0; i < num_shards; i++) {
    db->ManualFlushMemTable(i);
  }

  std::this_thread::sleep_for(std::chrono::seconds(10));

  db->SetL0CompactionSchedulerStatus(ListDB::ServiceStatus::kActive);
  std::this_thread::sleep_for(std::chrono::seconds(SLEEP_TIME));


  db->PrintDebugLsmState(0);

  // Work
  {
    printf("WORK %zu queries\n", NUM_WORKS);
    auto begin_tp = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;

    const size_t num_ops_per_thread = NUM_WORKS / num_threads;
    for (int id = 0; id < num_threads; id++) {
      workers.emplace_back([&, id] {
        std::string val_read;
        val_read.reserve(VALUE_SIZE);

        SetAffinity(Numa::CpuSequenceRR(id));
        int r = GetChip();
        DBClient* client = new DBClient(db, id, r);

        for (size_t i = id*num_ops_per_thread; i < (id+1)*num_ops_per_thread; i++) {
          if (work_ops[i] == OP_INSERT || work_ops[i] == OP_UPDATE) {
            listdb_Put(client, work_keys[i], value);
          } else if (work_ops[i] == OP_READ) {
            listdb_Get(client, work_keys[i], &val_read);
          } else if (work_ops[i] == OP_SCAN) {
            //not available yet
            printf("scan is not available!\n");
          } 
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


void remove_directory(const std::string& path) {
    try {
        if (fs::remove_all(path)) {
        } else {
            std::cout << "Directory does not exist: " << path << std::endl;
        }
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Error removing directory: " << e.what() << std::endl;
    }
}

void create_directory(const std::string& path) {
    try {
        if (fs::create_directory(path)) {
        } else {
            std::cout << "Directory already exists or error occurred: " << path << std::endl;
        }
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Error creating directory: " << e.what() << std::endl;
    }
}




int main(int argc, char* argv[]) {
    int num_threads = NUM_THREADS;
    int num_shards = NUM_SHARDS;

    std::string workload_type = "a"; // Default workload

    // Check if an argument is provided
    if (argc >= 2) {
        std::string arg1 = argv[1];
        if (arg1 == "a" || arg1 == "b" || arg1 == "c" || arg1 == "d" || arg1 == "e") {
            workload_type = arg1;
        } else {
            std::cerr << "Invalid workload type. Please use one of: a, b, c, d, e." << std::endl;
            return 1;
        }
    }

    if (argc >= 3) {
        num_threads = std::atoi(argv[2]);
        if (num_threads <= 0) {
            std::cerr << "Invalid number of threads. Please provide a positive integer." << std::endl;
            return 1;
        }
    }

    if (argc > 3) {
        std::cerr << "Usage: " << argv[0] << " [<workload_type (a, b, c, d, or e)>] [<num_threads>]" << std::endl;
        return 1;
    }

     for (int i = 0; i < kNumRegions; i++) {
        std::string dir_path = "/mnt/pmem" + std::to_string(i) + "/wkim";
        remove_directory(dir_path);
        create_directory(dir_path);
    }

    fprintf(stdout, "num_threads=%d\nworkload_type=%s\n", num_threads, workload_type.c_str());
    fprintf(stdout, "key_size=%d\nvalue_size=%d\n", KEY_SIZE, VALUE_SIZE);
  {
    fprintf(stdout, "*** Cache Configurations ***\n");

    #ifdef LISTDB_SKIPLIST_CACHE
        fprintf(stdout, "L1_cache_size: %zu bytes\n", kSkipListCacheCapacity);
    #else
        fprintf(stdout, "L1_cache_size: disabled.\n");
    #endif
    }

    Numa::Init();

    printf("****************************\n");

    // Define file paths based on workload type
    std::string load_file = "/juwon/index-microbench/ycsb_workload" + workload_type + "/load_unif_string_100M_100M.dat";
    std::string work_file = "/juwon/index-microbench/ycsb_workload" + workload_type + "/run_unif_string_100M_100M.dat";

    // Vectors to store the parsed data
    std::vector<std::string_view> load_keys;
    std::vector<OpType> work_ops;
    std::vector<std::string_view> work_keys;

    // Temporary storage for lines and keys
    std::vector<std::string> load_lines(NUM_LOADS);
    std::vector<std::string> work_lines(NUM_WORKS);

    // Dummy value for initialization
    std::string dummy_value(VALUE_SIZE, 'x');
    value = std::string_view(dummy_value);

    // Open and read the load file
    std::ifstream loadFileStream(load_file);
    if (!loadFileStream) {
        std::cerr << "Error opening load file: " << load_file << std::endl;
        return 1;
    }

    std::string line;
    for (size_t i = 0; i < NUM_LOADS && std::getline(loadFileStream, line); ++i) {
        std::istringstream iss(line);
        std::string operation, key;
        if (iss >> operation >> key) {
            load_lines[i] = line;
            load_keys.emplace_back(load_lines[i].data() + operation.length() + 1, std::min(key.length(), static_cast<size_t>(KEY_SIZE)));
        }
    }
    loadFileStream.close();
    printf("Filling up work queries: DONE\n");
   

    // Open and read the work file
    std::ifstream workFileStream(work_file);
    if (!workFileStream) {
        std::cerr << "Error opening work file: " << work_file << std::endl;
        return 1;
    }

    for (size_t i = 0; i < NUM_WORKS && std::getline(workFileStream, line); ++i) {
        std::istringstream iss(line);
        std::string operation, key;
        if (iss >> operation >> key) {
            work_ops.push_back(parseOperation(operation));
            work_lines[i] = line;
            work_keys.emplace_back(work_lines[i].data() + operation.length() + 1, std::min(key.length(), static_cast<size_t>(KEY_SIZE)));
        }
    }
    workFileStream.close();
    printf("Filling up work queries: DONE\n");

    // end of loading files
    Run(num_threads, num_shards, load_keys, work_ops, work_keys);

    

    return 0;
}
#endif//GFLAGS