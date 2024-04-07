#ifndef LISTDB_LISTDB_H_
#define LISTDB_LISTDB_H_

#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <queue>
#include <sstream>
#include <stack>
#include <thread>
#include <unordered_map>

#include <time.h>
#include <unistd.h>


#include <experimental/filesystem>

#include <libpmemobj++/pexceptions.hpp>
#include <numa.h>

#include "listdb/common.h"
#ifdef LISTDB_SKIPLIST_CACHE
#include "listdb/core/skiplist_cache.h"
#endif
#include "listdb/core/pmem_blob.h"
#include "listdb/core/pmem_log.h"
#include "listdb/core/static_hashtable_cache.h"
#include "listdb/core/double_hashing_cache.h"
#include "listdb/core/linear_probing_hashtable_cache.h"
#include "listdb/core/pmem_db.h"
#include "listdb/index/braided_pmem_skiplist.h"
#include "listdb/index/packed_pmem_skiplist.h"
#include "listdb/index/lockfree_skiplist.h"
#include "listdb/index/simple_hash_table.h"
#include "listdb/lsm/level_list.h"
#include "listdb/lsm/memtable_list.h"
#include "listdb/lsm/pmemtable.h"
#include "listdb/lsm/pmemtable_list.h"
#include "listdb/lsm/pmemtable2.h"
#include "listdb/lsm/pmemtable2_list.h"
#include "listdb/util/clock.h"
#include "listdb/util/random.h"
#include "listdb/util/reporter.h"
#include "listdb/util/reporter_client.h"
#ifdef LISTDB_BLOOM_FILTER
#include "listdb/index/bloom_filter.h"
#endif

//for disk write
#include <fcntl.h>

#define L0_COMPACTION_ON_IDLE
// /#define L0_COMPACTION_YIELD

#define REPORT_BACKGROUND_WORKS
#ifdef REPORT_BACKGROUND_WORKS
#define INIT_REPORTER_CLIENT auto reporter_client = new ReporterClient(reporter_)
#define REPORT_FLUSH_OPS(x) reporter_client->ReportFinishedOps(Reporter::OpType::kFlush, x)
#define REPORT_L0_COMPACTION_OPS(x) reporter_client->ReportFinishedOps(Reporter::OpType::kL0Compaction, x)
#define REPORT_L1_COMPACTION_OPS(x) reporter_client->ReportFinishedOps(Reporter::OpType::kL1Compaction, x)
#define REPORT_DONE delete reporter_client
#else
#define INIT_REPORTER_CLIENT
#define REPORT_FLUSH_OPS(x)
#define REPORT_L0_COMPACTION_OPS(x)
#define REPORT_L1_COMPACTION_OPS(x)
#define REPORT_DONE
#endif

//#define L0_COMPACTION_LATENCY_BREAKDOWN
#define L0_COMPACTION_NEEDS_TRIGGER

//#define L0_COMPACTION_ON_IDLE
//#define L0_COMPACTION_YIELD

namespace fs = std::experimental::filesystem::v1;

class ListDB {
 public:
  using MemNode = lockfree_skiplist::Node;
  using PmemNode = BraidedPmemSkipList::Node;
  using PmemNode2 = PackedPmemSkipList::Node;
  using KVpairs = PackedPmemSkipList::KVpairs;
  using HintedPtr = PackedPmemSkipList::HintedPtr;

  struct Task {
    TaskType type;
    int shard;
  };

  struct MemTableFlushTask : Task {
    MemTable* imm;
    MemTableList* memtable_list;
  };

  struct L0CompactionTask : Task {
    PmemTable* l0;
    MemTableList* memtable_list;
  };

  struct L1CompactionTask : Task {
  };

  struct cmp_task_priority {
    bool operator()(Task* a, Task* b) {
      if(b->type == TaskType::kL0Compaction) return false;
      return true;
    }
  };

  struct alignas(64) CompactionWorkerData {
    int id;
    bool stop;
    Random rnd = Random(0);
    std::priority_queue <Task*, std::vector<Task*>, cmp_task_priority> q;
    std::mutex mu;
    std::condition_variable cv;
    Task* current_task;
    uint64_t flush_cnt = 0;
    uint64_t flush_time_usec = 0;
    //uint64_t compaction_cnt = 0;
    //uint64_t compaction_time_usec = 0;
  };

  enum class ServiceStatus {
    kActive,
    kStop,
  };

  ~ListDB();

  void Init();

  void Open();

  void Close();

  //void Put(const Key& key, const Value& value);

  void WaitForStableState();

  Reporter* GetOrCreateReporter(const std::string& fname);

  // private:
  MemTable* GetWritableMemTable(size_t kv_size, int shard);

  MemTable* GetMemTable(int shard);

#if LISTDB_L0_CACHE == L0_CACHE_T_SIMPLE
  SimpleHashTable* GetHashTable(int shard);
#elif LISTDB_L0_CACHE == L0_CACHE_T_STATIC
  StaticHashTableCache* GetHashTable(int shard);
#elif LISTDB_L0_CACHE == L0_CACHE_T_DOUBLE_HASHING
  DoubleHashingCache* GetHashTable(int shard);
#elif LISTDB_L0_CACHE == L0_CACHE_T_LINEAR_PROBING
  LinearProbingHashTableCache* GetHashTable(int shard);
#endif

  TableList* GetTableList(int level, int shard);

  template <typename T>
  T* GetTableList(int level, int shard);

  PmemLog* log(int region, int shard) { return log_[region][shard]; }

#ifdef LISTDB_WISCKEY
  PmemBlob* value_blob(const int region, const int shard) { return value_blob_[region][shard]; }
#endif

  int pool_id_to_region(const int pool_id) { return pool_id_to_region_[pool_id]; }

  int l0_pool_id(const int region) { return l0_pool_id_[region]; }

  int l1_pool_id(const int region) { return l1_pool_id_[region]; }

  // Background Works
  void SetL0CompactionSchedulerStatus(const ServiceStatus& status);

  void SetL1CompactionSchedulerStatus(const ServiceStatus& status);

  void BackgroundThreadLoop();

  void CompactionWorkerThreadLoop(CompactionWorkerData* td);

  void FlushMemTable(MemTableFlushTask* task, CompactionWorkerData* td);

  void ManualFlushMemTable(int shard);
  
  void L1Compaction(int shard);

  void LogStructuredMergeCompactionL0(CompactionWorkerData* td, L0CompactionTask* task);

  // Utility Functions
  void PrintDebugLsmState(int shard);

  int GetStatString(const std::string& name, std::string* buf);

#ifdef LISTDB_SKIPLIST_CACHE
  SkipListCacheRep* skiplist_cache(int s, int r) { return cache_[s][r]; }
#endif

 private:
#ifdef LISTDB_WISCKEY
  PmemBlob* value_blob_[kNumRegions][kNumShards];
#endif
  PmemLog* log_[kNumRegions][kNumShards];
  PmemLog* l0_arena_[kNumRegions][kNumShards];
  PmemLog* l1_arena_[kNumRegions][kNumShards];
  LevelList* ll_[kNumShards];

#if LISTDB_L0_CACHE == L0_CACHE_T_SIMPLE
  SimpleHashTable* hash_table_[kNumShards];
#elif LISTDB_L0_CACHE == L0_CACHE_T_STATIC
  StaticHashTableCache* hash_table_[kNumShards];
#elif LISTDB_L0_CACHE == L0_CACHE_T_DOUBLE_HASHING
  DoubleHashingCache* hash_table_[kNumShards];
#elif LISTDB_L0_CACHE == L0_CACHE_T_LINEAR_PROBING
  LinearProbingHashTableCache* hash_table_[kNumShards];
#endif

  std::unordered_map<int, int> pool_id_to_region_;
  std::unordered_map<int, int> log_pool_id_;
  std::unordered_map<int, int> l0_pool_id_;
  std::unordered_map<int, int> l1_pool_id_;

  //std::queue<MemTableFlushTask*> memtable_flush_queue_;
  //std::mutex bg_mu_;
  //std::condition_variable bg_cv_;

  std::deque<Task*> work_request_queue_;
  std::deque<Task*> work_completion_queue_;
  std::mutex wq_mu_;
  std::condition_variable wq_cv_;

  std::thread bg_thread_;
  bool stop_ = false;
#ifdef L0_COMPACTION_NEEDS_TRIGGER
  ServiceStatus l0_compaction_scheduler_status_ = ServiceStatus::kStop;
#else
  ServiceStatus l0_compaction_scheduler_status_ = ServiceStatus::kActive;
#endif
  ServiceStatus l1_compaction_scheduler_status_ = ServiceStatus::kStop;

  CompactionWorkerData worker_data_[kNumWorkers];
  std::thread worker_threads_[kNumWorkers];

#ifdef LISTDB_SKIPLIST_CACHE
  SkipListCacheRep* cache_[kNumShards][kNumRegions];
#endif

  std::atomic<Reporter*> reporter_;
  std::mutex mu_;
};


ListDB::~ListDB() {
  fprintf(stdout, "D \n");
  Close();
}

void ListDB::Init() {
  std::string db_path = "/pmem0/wkim/listdb";
  fs::remove_all(db_path);
  int root_pool_id = Pmem::BindPool<pmem_db>(db_path, "", 64*1024*1024);
  if (root_pool_id != 0) {
    std::cerr << "root_pool_id must be zero (current: " << root_pool_id << ")\n";
    exit(1);
  }
  auto db_pool = Pmem::pool<pmem_db>(root_pool_id);
  auto db_root = db_pool.root();
  for (int i = 0; i < kNumShards; i++) {
    pmem::obj::persistent_ptr<pmem_db_shard> p_shard_manifest;
    pmem::obj::make_persistent_atomic<pmem_db_shard>(db_pool, p_shard_manifest);
    pmem::obj::persistent_ptr<pmem_l0_info> p_l0_manifest;
    pmem::obj::make_persistent_atomic<pmem_l0_info>(db_pool, p_l0_manifest);
    p_shard_manifest->l0_list_head = p_l0_manifest;
    db_root->shard[i] = p_shard_manifest;
  }
  // TODO(wkim): write log path on db_root

  // Log Pmem Pool
  for (int i = 0; i < kNumRegions; i++) {
    std::stringstream pss;
    pss << "/pmem" << i << "/wkim/listdb_log";
    std::string path = pss.str();
    fs::remove_all(path);
    fs::create_directories(path);

    std::string poolset = path + ".set";
    std::fstream strm(poolset, strm.out);
    strm << "PMEMPOOLSET" << std::endl;
    strm << "OPTION SINGLEHDR" << std::endl;
    strm << "400G " << path << "/" << std::endl;
    strm.close();

    int pool_id = Pmem::BindPoolSet<pmem_log_root>(poolset, "");
    pool_id_to_region_[pool_id] = i;
    log_pool_id_[i] = pool_id;
    auto pool = Pmem::pool<pmem_log_root>(pool_id);

    for (int j = 0; j < kNumShards; j++) {
      log_[i][j] = new PmemLog(pool_id, j);
    }
  }

  // l1 Pmem Pool
  for (int i = 0; i < kNumRegions; i++) {
    std::stringstream pss;
    pss << "/pmem" << i << "/wkim/listdb_l1";
    std::string path = pss.str();
    fs::remove_all(path);
    fs::create_directories(path);

    std::string poolset = path + ".set";
    std::fstream strm(poolset, strm.out);
    strm << "PMEMPOOLSET" << std::endl;
    strm << "OPTION SINGLEHDR" << std::endl;
    strm << "400G " << path << "/" << std::endl;
    strm.close();
    int pool_id = Pmem::BindPoolSet<pmem_log_root>(poolset, "");
    pool_id_to_region_[pool_id] = i;
    l1_pool_id_[i] = pool_id;
    auto pool = Pmem::pool<pmem_log_root>(pool_id);
    for (int j = 0; j < kNumShards; j++) {
      l1_arena_[i][j] = new PmemLog(pool_id, j);
    }
  }
#ifndef LISTDB_WAL
  // IUL
  for (int i = 0; i < kNumRegions; i++) {
    l0_pool_id_[i] = log_pool_id_[i];
    for (int j = 0; j < kNumShards; j++) {
      l0_arena_[i][j] = log_[i][j];
    }
  }
#endif

#ifdef LISTDB_WISCKEY
  for (int i = 0; i < kNumRegions; i++) {
    std::stringstream pss;
    pss << "/pmem" << i << "/wkim/listdb_value";
    std::string path = pss.str();
    fs::remove_all(path);
    fs::create_directories(path);

    std::string poolset = path + ".set";
    std::fstream strm(poolset, strm.out);
    strm << "PMEMPOOLSET" << std::endl;
    strm << "OPTION SINGLEHDR" << std::endl;
    strm << "400G " << path << "/" << std::endl;
    strm.close();

    int pool_id = Pmem::BindPoolSet<pmem_blob_root>(poolset, "");
    pool_id_to_region_[pool_id] = i;
    auto pool = Pmem::pool<pmem_blob_root>(pool_id);

    for (int j = 0; j < kNumShards; j++) {
      value_blob_[i][j] = new PmemBlob(pool_id, j);
    }
  }
#endif


  for (int i = 0; i < kNumShards; i++) {
    ll_[i] = new LevelList();
    // MemTableList
    {
      auto tl = new MemTableList(kMemTableCapacity / kNumShards, i);
      tl->BindEnqueueFunction([&, tl, i](MemTable* mem) {
        //fprintf(stdout, "binded enq fn, mem = %p\n", mem);
        auto task = new MemTableFlushTask();
        task->type = TaskType::kMemTableFlush;
        task->shard = i;
        task->imm = mem;
        task->memtable_list = tl;
        std::unique_lock<std::mutex> lk(wq_mu_);
        work_request_queue_.push_back(task);
        lk.unlock();
        wq_cv_.notify_one();
      });
      for (int j = 0; j < kNumRegions; j++) {
        tl->BindArena(j, l0_arena_[j][i]);
      }
      ll_[i]->SetTableList(0, tl);
    }

    // l1 List
    {
      auto tl = new PmemTable2List(std::numeric_limits<size_t>::max(), l1_arena_[0][0]->pool_id());
      for (int j = 0; j < kNumRegions; j++) {
        tl->BindArena(l1_arena_[j][i]->pool_id(), l1_arena_[j][i]);
      }
      ll_[i]->SetTableList(1, tl);
    }
  }

#if 1

  for (int i = 0; i < kNumShards; i++) {
    auto tl = ll_[i]->GetTableList(0);
    // TODO: impl InitFrontOnce() and use it instead of GetFront()
    [[maybe_unused]] auto table = (MemTable*) tl->NewFront();
  }
  
  for (int i = 0; i < kNumShards; i++) {
    
    auto l1_tl = ll_[i]->GetTableList(1);
    // TODO: impl InitFrontOnce() and use it instead of GetFront()
    [[maybe_unused]] auto l1_table = (PmemTable2*) l1_tl->NewFront();
    
  }
#endif


#ifdef LISTDB_SKIPLIST_CACHE
  for (int i = 0; i < kNumShards; i++) {
    for (int j = 0; j < kNumRegions; j++) {
      cache_[i][j] = new SkipListCacheRep(l1_arena_[j][i]->pool_id(), j, kSkipListCacheCapacity / kNumShards / kNumRegions);
    }
  }
#endif

#if LISTDB_L0_CACHE == L0_CACHE_T_SIMPLE
  for (int i = 0; i < 1; i++) {
    hash_table_[i] = new SimpleHashTable(kHTSize);
    for (size_t j = 0; j < kHTSize; j++) {
      hash_table_[i]->at(j)->version = 1UL;
    }
  }
#elif LISTDB_L0_CACHE == L0_CACHE_T_STATIC
  for (int i = 0; i < kNumShards; i++) {
    hash_table_[i] = new StaticHashTableCache(kHTSize / kNumShards, i);
  }
#elif LISTDB_L0_CACHE == L0_CACHE_T_DOUBLE_HASHING
  for (int i = 0; i < kNumShards; i++) {
    hash_table_[i] = new DoubleHashingCache(kHTSize / kNumShards, i);
  }
#elif LISTDB_L0_CACHE == L0_CACHE_T_LINEAR_PROBING
  for (int i = 0; i < kNumShards; i++) {
    hash_table_[i] = new LinearProbingHashTableCache(kHTSize / kNumShards, i);
  }
#endif

  bg_thread_ = std::thread(std::bind(&ListDB::BackgroundThreadLoop, this));

  for (int i = 0; i < kNumWorkers; i++) {
    worker_data_[i].id = i;
    worker_data_[i].stop = false;
    worker_threads_[i] = std::thread(std::bind(&ListDB::CompactionWorkerThreadLoop, this, &worker_data_[i]));

    //sched_param sch;
    //int policy; 
    //pthread_getschedparam(worker_threads_[i].native_handle(), &policy, &sch);
    //sch.sched_priority = 20;
    //pthread_setschedparam(worker_threads_[i].native_handle(), SCHED_FIFO, &sch);
  }
}

void ListDB::Open() {
  std::string db_path = "/pmem0/wkim/listdb";
  int root_pool_id = Pmem::BindPool<pmem_db>(db_path, "", 64*1024*1024);
  if (root_pool_id != 0) {
    std::cerr << "root_pool_id must be zero (current: " << root_pool_id << ")\n";
    exit(1);
  }
  auto db_pool = Pmem::pool<pmem_db>(root_pool_id);
  auto db_root = db_pool.root();

  // Log Pmem Pool
  for (int i = 0; i < kNumRegions; i++) {
    std::stringstream pss;
    pss << "/pmem" << i << "/wkim/listdb_log";
    std::string path = pss.str();
    std::string poolset = path + ".set";

    int pool_id = Pmem::BindPoolSet<pmem_log_root>(poolset, "");
    pool_id_to_region_[pool_id] = i;
    //auto pool = Pmem::pool<pmem_log_root>(pool_id);
    l0_pool_id_[i] = pool_id;

    for (int j = 0; j < kNumShards; j++) {
      log_[i][j] = new PmemLog(pool_id, j);
    }
  }
#ifndef LISTDB_WAL
  // IUL
  for (int i = 0; i < kNumRegions; i++) {
    l0_pool_id_[i] = log_pool_id_[i];
    for (int j = 0; j < kNumShards; j++) {
      l0_arena_[i][j] = log_[i][j];
    }
  }
#else
  std::cerr << "Open() for LISTDB_WAL is not implemented." << std:endl;
  exit(1);
#endif

#ifdef LISTDB_WISCKEY
  for (int i = 0; i < kNumRegions; i++) {
    std::stringstream pss;
    pss << "/pmem" << i << "/wkim/listdb_value";
    std::string path = pss.str();
    std::string poolset = path + ".set";

    int pool_id = Pmem::BindPoolSet<pmem_blob_root>(poolset, "");
    pool_id_to_region_[pool_id] = i;
    auto pool = Pmem::pool<pmem_blob_root>(pool_id);

    for (int j = 0; j < kNumShards; j++) {
      value_blob_[i][j] = new PmemBlob(pool_id, j);
    }
  }
#endif

  for (int i = 0; i < kNumShards; i++) {
    ll_[i] = new LevelList();
    // MemTableList
    {
      auto tl = new MemTableList(kMemTableCapacity / kNumShards, i);
      tl->BindEnqueueFunction([&, tl, i](MemTable* mem) {
        //fprintf(stdout, "binded enq fn, mem = %p\n", mem);
        auto task = new MemTableFlushTask();
        task->type = TaskType::kMemTableFlush;
        task->shard = i;
        task->imm = mem;
        task->memtable_list = tl;
        std::unique_lock<std::mutex> lk(wq_mu_);
        work_request_queue_.push_back(task);
        lk.unlock();
        wq_cv_.notify_one();
      });
      for (int j = 0; j < kNumRegions; j++) {
        tl->BindArena(j, l0_arena_[j][i]);
      }
      ll_[i]->SetTableList(0, tl);
    }

    // l1 List
    {
      auto tl = new PmemTable2List(std::numeric_limits<size_t>::max(), l1_arena_[0][0]->pool_id());
      for (int j = 0; j < kNumRegions; j++) {
        tl->BindArena(l1_arena_[j][i]->pool_id(), l1_arena_[j][i]);
      }
      ll_[i]->SetTableList(1, tl);
    }
  }

  std::atomic<int> memtable_recovery_cnt_total = 0;
  std::atomic<int> l0_recovery_cnt_total = 0;
  std::atomic<int> l0_persisted_cnt_total = 0;
  std::atomic<int> merge_done_cnt_total = 0;
  std::atomic<size_t> mem_insert_cnt_total = 0;
  std::atomic<size_t> l0_insert_cnt_total = 0;

  auto recovery_begin_tp = std::chrono::steady_clock::now();
  // read l1 info
  // read l0 info
  // check the status
  // classify the L0 tables according to the status
  //  - recover as memtable
  //  - recover as L0
  //  - recover as L1

  // TODO(wkim): Parallel execution.
  std::vector<std::thread> recovery_workers;
  static const int kNumRecoveryThreads = kNumShards;
  for (int wid = 0; wid < kNumRecoveryThreads; wid++) {
    recovery_workers.push_back(std::thread([&, wid] {
      int memtable_recovery_cnt = 0;
      int l0_recovery_cnt = 0;
      int l0_persisted_cnt = 0;
      int merge_done_cnt = 0;
      size_t mem_insert_cnt = 0;
      size_t l0_insert_cnt = 0;
      int shard_begin = (kNumShards / kNumRecoveryThreads) * wid;
      int shard_end = (kNumShards / kNumRecoveryThreads) * (wid + 1);
      for (int i = shard_begin ; i < shard_end; i++) {
        auto shard = db_root->shard[i];

        auto memtable_list = GetTableList<MemTableList>(0, i);
        // L0 list
        // Initial -> Persist -> Persist -> ... -> Merging -> Merged -> Merged -> ...
        auto pred_l0_info = shard->l0_list_head;
        auto curr_l0_info = pred_l0_info->next;
        std::deque<pmem::obj::persistent_ptr<pmem_l0_info>> l0_manifests;
        uint64_t min_l0_id = std::numeric_limits<uint64_t>::max();
        while (curr_l0_info) {
          if (curr_l0_info->status == Level0or1Status::kMergeDone) {
            merge_done_cnt++;
            for (int j = 0; j < kNumRegions; j++) {
              size_t head_node_size = sizeof(PmemNode) + (kMaxHeight - 1) * sizeof(uint64_t);
              pmem::obj::delete_persistent_atomic<char[]>(curr_l0_info->head[j], head_node_size);
            }
            auto succ_l0_info = curr_l0_info->next;
            // TODO(wkim): do the followings as a transaction
            pred_l0_info->next = succ_l0_info;
            pmem::obj::delete_persistent_atomic<pmem_l0_info>(curr_l0_info);
            curr_l0_info = succ_l0_info;
            continue;
          }
          min_l0_id = curr_l0_info->id;
          l0_manifests.push_front(curr_l0_info);
          curr_l0_info = curr_l0_info->next;
        }

        // Collect log blocks
        std::deque<pmem::obj::persistent_ptr<pmem_log_block>> log_blocks[kNumRegions];
        for (int j = 0; j < kNumRegions; j++) {
          //int pool_id = log_[j][i]->pool_id();
          auto pool = log_[j][i]->pool();
          auto log_shard = pool.root()->shard[i];
          //curr_log_block[j] = log_shard->head;
          //cursor[j].p_block = log_shard->head;
          //cursor[j].data = cursor[j].p_block->data;
          auto curr_block = log_shard->head;
          while (curr_block) {
            PmemNode* first_record = (PmemNode*) curr_block->data;
            log_blocks[j].push_front(curr_block);
            if (first_record->l0_id() < min_l0_id) {
              break;
            }
            curr_block = curr_block->next;
          }
        }
        // Init log cursor
        struct LogCursor {
          char* data = nullptr;
          uint64_t offset = 0;
          std::deque<pmem::obj::persistent_ptr<pmem_log_block>>::iterator block_iter;
          //pmem::obj::persistent_ptr<pmem_log_block> p_block = nullptr;

          char* p() { return data + offset; }
        } cursor[kNumRegions];
        for (int j = 0; j < kNumRegions; j++) {
          cursor[j].block_iter = log_blocks[j].begin();
          cursor[j].data = (*(cursor[j].block_iter))->data;
        }

        std::deque<Table*> tables;
        // l0_manifests: oldest to newest
        for (auto& l0 : l0_manifests) {
          // Prepare L0 SkipList
          auto l0_skiplist = new BraidedPmemSkipList(l0_arena_[0][0]->pool_id());
          for (int j = 0; j < kNumRegions; j++) {
            int pool_id = l0_arena_[j][i]->pool_id();
            int region = pool_id_to_region_[pool_id];
            l0_skiplist->BindArena(pool_id, l0_arena_[j][i]);
            l0_skiplist->BindHead(pool_id, (void*) l0->head[region].get());
          }

          if (l0->status == Level0or1Status::kMergeInitiated) {
            fprintf(stdout, "Level0or1Status::kMergeInitiated.\n");
            exit(1);
            // TODO(wkim): Continue and finish L0 to L1 compaction
          } else if (l0->status == Level0or1Status::kPersisted) {
            l0_persisted_cnt++;
            auto l0_table = new PmemTable(kMemTableCapacity, l0_skiplist);
            l0_table->SetSize(kMemTableCapacity);
            //memtable_list->PushFront(l0_table);
            tables.push_back((Table*) l0_table);
          } else if (l0->status == Level0or1Status::kFull) {
            l0_recovery_cnt++;
            // Reset L0 skiplist
            for (int j = 0; j < kNumRegions; j++) {
              int pool_id = log_[j][i]->pool_id();
              auto p_head = l0_skiplist->head(pool_id);
              for (int k = 0; k < kMaxHeight; k++) {
                p_head->next[k] = 0;
              }
            }

            // Replay Log
            for (int j = 0; j < kNumRegions; j++) {
              int pool_id = log_[j][i]->pool_id();
              auto pool = log_[j][i]->pool();
              //auto log_shard = pool.root()->shard[i];

              bool current_table_done = false;
              while (cursor[j].block_iter != log_blocks[j].end()) {
                while (cursor[j].offset < kPmemLogBlockSize - 7) {
                  char* p = cursor[j].p();
                  PmemNode* p_node = (PmemNode*) p;
                  if (!p_node->key.Valid()) {
                    break;
                  }
        //fprintf(stdout, "key: %s,%zu\nheight=%d\nl0_id=%u/%lu\nvalue=%zu\n", std::string(p_node->key.data(), 8).c_str(), *((uint64_t*) &p_node->key), p_node->height(), p_node->l0_id(), l0->id, p_node->value);
                  if (p_node->l0_id() > l0->id) {
                    current_table_done = true;
                    break;
                  }
                  if (p_node->l0_id() == l0->id) {
                    // DO REPLAY
                    PmemPtr node_paddr(pool_id, (uint64_t) ((uintptr_t) p - (uintptr_t) pool.handle()));
                    l0_skiplist->Insert(node_paddr);
                    l0_insert_cnt++;
                  }
                  int height = p_node->height();
                  size_t iul_entry_size = sizeof(PmemNode) + (height - 1) * sizeof(uint64_t);
                  cursor[j].offset += iul_entry_size;
                }
                if (current_table_done)  {
                  break;
                }
                ++cursor[j].block_iter;
                if (cursor[j].block_iter != log_blocks[j].end()) {
                  cursor[j].data = (*(cursor[j].block_iter))->data;
                  cursor[j].offset = 0;
                }
              }
            }

            auto l0_table = new PmemTable(kMemTableCapacity, l0_skiplist);
            l0_table->SetSize(kMemTableCapacity);
            //memtable_list->PushFront(l0_table);
            tables.push_back((Table*) l0_table);
          } else if (l0->status == Level0or1Status::kInitialized) {
            memtable_recovery_cnt++;
            // Reset L0 skiplist
            for (int j = 0; j < kNumRegions; j++) {
              int pool_id = log_[j][i]->pool_id();
              auto p_head = l0_skiplist->head(pool_id);
              for (int k = 0; k < kMaxHeight; k++) {
                p_head->next[k] = 0;
              }
            }

            // Init MemTable SkipList
            auto memtable = new MemTable(kMemTableCapacity);
            memtable->SetL0SkipList(l0_skiplist);
            auto skiplist = memtable->skiplist();
            size_t kv_size_total = 0;

            // Replay Log
            for (int j = 0; j < kNumRegions; j++) {
              int pool_id = log_[j][i]->pool_id();
              auto pool = log_[j][i]->pool();
              //auto log_shard = pool.root()->shard[i];

              bool current_table_done = false;
              while (cursor[j].block_iter != log_blocks[j].end()) {
                while (cursor[j].offset < kPmemLogBlockSize - 7) {
                  char* p = cursor[j].p();
                  PmemNode* p_node = (PmemNode*) p;
                  if (!p_node->key.Valid()) {
                    break;
                  }
                  if (p_node->l0_id() > l0->id) {
                    current_table_done = true;
                    break;
                  }
                  int height = p_node->height();
                  if (p_node->l0_id() == l0->id) {
                    // DO REPLAY
                    PmemPtr log_paddr(pool_id, (uint64_t) ((uintptr_t) p - (uintptr_t) pool.handle()));

                    // Create skiplist node
                    MemNode* node = (MemNode*) malloc(sizeof(MemNode) + (height - 1) * sizeof(uint64_t));
                    node->key = p_node->key;
                    node->tag = height;
                    node->value = log_paddr.dump();
                    memset((void*) &node->next[0], 0, height * sizeof(uint64_t));

                    kv_size_total += node->key.size() + sizeof(Value);

                    skiplist->Insert(node);
                    mem_insert_cnt++;
                  }
                  size_t iul_entry_size = sizeof(PmemNode) + (height - 1) * sizeof(uint64_t);
                  cursor[j].offset += iul_entry_size;
                }
                if (current_table_done)  {
                  break;
                }
                ++cursor[j].block_iter;
                if (cursor[j].block_iter != log_blocks[j].end()) {
                  cursor[j].data = (*(cursor[j].block_iter))->data;
                  cursor[j].offset = 0;
                }
              }
            }
            memtable->SetSize(kv_size_total);
            //memtable_list->PushFront(memtable);
            tables.push_back((Table*) memtable);
          } else {
            std::cerr << "Unknown L0 status.\n";
            exit(1);
          }

        }

        // Build a MemTable List for this shard with individually initialized tables
        // tables: oldest to newest
        for (size_t i = 0; i < tables.size(); i++) {
          if (i > 0) {
           tables[i]->SetNext(tables[i - 1]);
          }
          memtable_list->PushFront(tables[i]);
        }

      }

      memtable_recovery_cnt_total.fetch_add(memtable_recovery_cnt);
      l0_recovery_cnt_total.fetch_add(l0_recovery_cnt);
      l0_persisted_cnt_total.fetch_add(l0_persisted_cnt);
      merge_done_cnt_total.fetch_add(merge_done_cnt);
      mem_insert_cnt_total.fetch_add(mem_insert_cnt);
      l0_insert_cnt_total.fetch_add(l0_insert_cnt);
    }));
  }
  for (auto& rw : recovery_workers) {
    if (rw.joinable()) rw.join();
  }
  auto recovery_end_tp = std::chrono::steady_clock::now();
  std::chrono::duration<double> recovery_duration = recovery_end_tp - recovery_begin_tp;
  fprintf(stdout, "recovery time : %.3lf sec\n", recovery_duration.count());
  fprintf(stdout, "recovery count:\n");
  fprintf(stdout, "  -     memtable: %d\n", memtable_recovery_cnt_total.load());
  fprintf(stdout, "  -           l0: %d\n", l0_recovery_cnt_total.load());
  fprintf(stdout, "  - persisted l0: %d\n", l0_persisted_cnt_total.load());
  fprintf(stdout, "  -    merged l0: %d\n", merge_done_cnt_total.load());
  fprintf(stdout, "mem insert cnt: %zu\n", mem_insert_cnt_total.load());
  fprintf(stdout, " l0 insert cnt: %zu\n", l0_insert_cnt_total.load());
}

void ListDB::Close() {
  stop_ = true;
  if (bg_thread_.joinable()) {
    bg_thread_.join();
  }

  for (int i = 0; i < kNumWorkers; i++) {
    worker_data_[i].stop = true;
    worker_data_[i].cv.notify_one();
    if (worker_threads_[i].joinable()) {
      worker_threads_[i].join();
    }
  }

  {
    std::lock_guard<std::mutex> lk(mu_);
    auto reporter = reporter_.load();
    if (reporter != nullptr) {
      delete reporter_;
    }
  }

  // Save log cursor info
  for (int i = 0; i < kNumShards; i++) {
    for (int j = 0; j < kNumRegions; j++) {
      delete log_[j][i];
    }
  }

  Pmem::Clear();
}

void ListDB::WaitForStableState() {
  // TODO(wkim): communicate with the background thread to get informed about the db state
  return;
}

Reporter* ListDB::GetOrCreateReporter(const std::string& fname) {
  auto rv = reporter_.load();
  if (rv == nullptr) {
    std::unique_lock<std::mutex> lk(mu_);
    rv = reporter_.load();
    if (rv == nullptr) {
      rv = new Reporter(fname);
      reporter_.store(rv);
    }
  }
  return rv;
}

void ListDB::SetL0CompactionSchedulerStatus(const ServiceStatus& status) {
  std::lock_guard<std::mutex> guard(wq_mu_);
  l0_compaction_scheduler_status_ = status;
}

void ListDB::SetL1CompactionSchedulerStatus(const ServiceStatus& status) {
  std::lock_guard<std::mutex> guard(wq_mu_);
  l1_compaction_scheduler_status_ = status;
}

void ListDB::BackgroundThreadLoop() {
#if 0
  numa_run_on_node(0);
#endif
  static const int kWorkerQueueDepth = 5;
  std::vector<int> num_assigned_tasks(kNumWorkers);
  std::unordered_map<Task*, int> task_to_worker;
  std::deque<Task*> memtable_flush_requests;
  std::deque<Task*> l0_compaction_requests;
  std::deque<Task*> l1_compaction_requests;
  std::vector<int> l0_compaction_state(kNumShards);
  std::vector<int> l1_compaction_state(kNumShards);

  struct ReqCompCounter {
    size_t req_cnt = 0;
    size_t comp_cnt = 0;
  };
  std::map<TaskType, ReqCompCounter> req_comp_cnt;
  req_comp_cnt.emplace(TaskType::kMemTableFlush, ReqCompCounter());

  while (true) {
    std::deque<Task*> new_work_requests;
    std::deque<Task*> work_completions;
    std::unique_lock<std::mutex> lk(wq_mu_);
    bool schedule_l0_compaction;
    bool schedule_l1_compaction;
    wq_cv_.wait_for(lk, std::chrono::seconds(1), [&]{//1초동안 안들어오면 timeout을 발생시킨다., 그럼 넘어가는 것. 통지가 오면 멈춘다!
      //return !work_request_queue_.empty() || !work_completion_queue_.empty();
      return !work_request_queue_.empty() || !memtable_flush_requests.empty();
    });
    new_work_requests.swap(work_request_queue_);
    work_completions.swap(work_completion_queue_);//queue를 mutex를 받아와서 업데이트한다.
    schedule_l0_compaction = (l0_compaction_scheduler_status_ == ServiceStatus::kActive);
    schedule_l1_compaction = (l1_compaction_scheduler_status_ == ServiceStatus::kActive);
    lk.unlock();

    for (auto& task : work_completions) {
      auto it = task_to_worker.find(task);
      int worker_id = it->second;
      task_to_worker.erase(it);
      if (task->type == TaskType::kL0Compaction) {//다른 worker가 kL0Compaction을 complete 해놨다면 이를 확인하고 해당 l0 compaction state를 0으로 바꿔놓는다. 이제 컴팩션 해도 된다는 뜻
        l0_compaction_state[task->shard] = 0;
      }
      //if (task->type == TaskType::kL1Compaction) {//다른 worker가 kL0Compaction을 complete 해놨다면 이를 확인하고 해당 l0 compaction state를 0으로 바꿔놓는다. 이제 컴팩션 해도 된다는 뜻
      //  l1_compaction_state[task->shard] = 0;
      //}
      num_assigned_tasks[worker_id]--;//해당 워커에 할당된 일 수를 줄요준다
      req_comp_cnt[task->type].comp_cnt++;//해당 타입 완료됐다고 올려준다.
      delete task;
    }//일단 처리 완료한 task에 대한 상태를 업데이트 해주는 일부터 한다.

    for (auto& task : new_work_requests) {
      if (task->type == TaskType::kMemTableFlush) {
        memtable_flush_requests.push_back(task);
      } else {
        fprintf(stdout, "unknown task request: %d\n", (int) task->type);
        exit(1);
      }
      req_comp_cnt[task->type].req_cnt++;
    }//사실 new_work_request에는 flush해달라는 type만 있어야 한다. 그렇지 않으면 오류가 나게 된다.
    if (schedule_l0_compaction) {//이건 무조건 켜있게 된다 계속 일어난다는 의미 --> 그렇다면 l1 compaction이 일어나는 기준은 어떻게 해야 하는가??
      for (int i = 0; i < kNumShards; i++) {
        if (l0_compaction_state[i] == 0) {//아니다 컴팩션 안되어 있는 것만 이렇다
          auto tl = ll_[i]->GetTableList(0);
          auto table = tl->GetFront();
          while (true) {
            auto next_table = table->Next();
            if (next_table) {
              table = next_table;
            } else {
              break;
            }
          }//level list의 해당 샤드의 wal table list의 맨 마지막 table만 가져온다.
          if (table->type() == TableType::kPmemTable) {
            auto task = new L0CompactionTask();
            task->type = TaskType::kL0Compaction;
            task->shard = i;
            task->l0 = (PmemTable*) table;//합쳐야 하는 마지막 테이블을 l0에 넣어준다. l1으로 컴팩션 해주어야 함.
            task->memtable_list = (MemTableList*) tl;//모든 테이블 리스트를 넣어준다,
            l0_compaction_state[i] = 1;  // configured// 컴팩션 진행중 건들지 마세요 컴팩션 안해도 됩니다~
            l0_compaction_requests.push_back(task);//이제 요청하기 위해서 만든 kl0compaction task를 넣어준다.
            req_comp_cnt[task->type].req_cnt++;
          }
        }
      }
    }
    if (schedule_l1_compaction) {//이건 무조건 켜있게 된다 계속 일어난다는 의미 --> 그렇다면 l1 compaction이 일어나는 기준은 어떻게 해야 하는가??
      for (int i = 0; i < kNumShards; i++) {
        if (l1_compaction_state[i] == 0) {//아니다 컴팩션 안되어 있는 것만 이렇다
            auto task = new L1CompactionTask();
            task->type = TaskType::kL1Compaction;
            task->shard = i;
            l1_compaction_state[i] = 1;  // configured// 컴팩션 진행중 건들지 마세요 컴팩션 안해도 됩니다~
            l1_compaction_requests.push_back(task);//이제 요청하기 위해서 만든 kl0compaction task를 넣어준다.
            req_comp_cnt[task->type].req_cnt++;

        }
      }
    }

    std::vector<CompactionWorkerData*> available_workers;
    for (int i = 0; i < kNumWorkers; i++) {
      if (num_assigned_tasks[i] < kWorkerQueueDepth) {
        available_workers.push_back(&worker_data_[i]);
      }
    }//여유있는 worker을 찾아서 pushback한다.

    if (!memtable_flush_requests.empty()) {
      while (!available_workers.empty() && !memtable_flush_requests.empty()) {
        std::sort(available_workers.begin(), available_workers.end(), [&](auto& a, auto& b) {
          return num_assigned_tasks[a->id] > num_assigned_tasks[b->id];
        });//worker id 내림차순 정렬
        auto& worker = available_workers.back();
        auto& task = memtable_flush_requests.front();
        memtable_flush_requests.pop_front();
        std::unique_lock<std::mutex> wlk(worker->mu);
        worker->q.push(task);
        wlk.unlock();
        worker->cv.notify_one();
        task_to_worker[task] = worker->id;
        num_assigned_tasks[worker->id]++;
        if (num_assigned_tasks[worker->id] >= kWorkerQueueDepth) {
          available_workers.pop_back();
        }//num_assigned_tasks가 꽉찻으니 가서 일하라고 명령
        //memtable flush 일을 하나 뽑아서 일할수 있는 worker한명에게 할당. 이 과정을 worker나 memtable flush 작업 소진시까지 반복.
      }
#ifdef L0_COMPACTION_ON_IDLE
      continue;
#endif
    }

    //available_workers.clear();
    //for (int i = 0; i < kNumWorkers; i++) {
    //  if (num_assigned_tasks[i] == 0) {
    //    available_workers.push_back(&worker_data_[i]);
    //  }
    //}

#ifndef LISTDB_NO_L0_COMPACTION
    while (!available_workers.empty() && !l0_compaction_requests.empty()) {
      std::sort(available_workers.begin(), available_workers.end(), [&](auto& a, auto& b) {
        return num_assigned_tasks[a->id] > num_assigned_tasks[b->id];
      });
      auto& worker = available_workers.back();
      auto& task = l0_compaction_requests.front();
      l0_compaction_requests.pop_front();

      std::unique_lock<std::mutex> wlk(worker->mu);
      worker->q.push(task);
      wlk.unlock();
      worker->cv.notify_one();

      task_to_worker[task] = worker->id;
      num_assigned_tasks[worker->id]++;
      l0_compaction_state[task->shard] = 2;  // assigned

      if (num_assigned_tasks[worker->id] >= kWorkerQueueDepth) {
        available_workers.pop_back();
      }
    }
#endif

#ifndef LISTDB_NO_L1_COMPACTION
    while (!available_workers.empty() && !l1_compaction_requests.empty()) {
      std::sort(available_workers.begin(), available_workers.end(), [&](auto& a, auto& b) {
        return num_assigned_tasks[a->id] > num_assigned_tasks[b->id];
      });
      auto& worker = available_workers.back();
      auto& task = l1_compaction_requests.front();
      l1_compaction_requests.pop_front();

      std::unique_lock<std::mutex> wlk(worker->mu);
      worker->q.push(task);
      wlk.unlock();
      worker->cv.notify_one();

      task_to_worker[task] = worker->id;
      num_assigned_tasks[worker->id]++;
      l1_compaction_state[task->shard] = 2;  // assigned

      if (num_assigned_tasks[worker->id] >= kWorkerQueueDepth) {
        available_workers.pop_back();
      }
    }
#endif

    if (stop_) {
      fprintf(stdout, "bg thread terminating\n");
      break;
    }
  }
}//background는 전체적으로 worker를 manage 해주면서 샤드의 compaction와 flush 필요여부를 파악해서 계속해서 worker에 일을 배정해준다.

void ListDB::CompactionWorkerThreadLoop(CompactionWorkerData* td) {
  td->rnd.Reset((td->id + 1) * (td->id + 1));
  while (true) {
    std::unique_lock<std::mutex> lk(td->mu);
    td->cv.wait(lk, [&]{ return td->stop || !td->q.empty(); });
    if (td->stop) {
      break;
    }
    auto task = td->q.top();
    td->q.pop();
    td->current_task = task;
    lk.unlock();



    if (task->type == TaskType::kMemTableFlush) {
      FlushMemTable((MemTableFlushTask*) task, td);
      td->current_task = nullptr;
    } else if (task->type == TaskType::kL0Compaction) {
      LogStructuredMergeCompactionL0(td, (L0CompactionTask*) task);
      td->current_task = nullptr;
    } else if (task->type == TaskType::kL1Compaction) {
      L1Compaction(task->shard);
      td->current_task = nullptr;
    }
    //task안에 
    std::unique_lock<std::mutex> bg_lk(wq_mu_);
    work_completion_queue_.push_back(task);
    bg_lk.unlock();
    wq_cv_.notify_one();//일 끝내고 completetion queue를 작성하기 위해 mutex를 뺏고 wq_mu_를 써야하는 친구에게 이 자원을 돌려준다. 아마도 write queue mutex로 추정됨.
  }
}

//void ListDB::Put(const Key& key, const Value& value) {
//}

// Any table this function returns must be unreferenced manually
// TODO(wkim): Make this function to return a wrapper of table
//   table is unreferenced on the destruction of its wrapper
inline MemTable* ListDB::GetWritableMemTable(size_t kv_size, int shard) {
  auto tl = ll_[shard]->GetTableList(0);
  auto mem = tl->GetMutable(kv_size);
  return (MemTable*) mem;
}

inline MemTable* ListDB::GetMemTable(int shard) {
  auto tl = ll_[shard]->GetTableList(0);
  auto mem = tl->GetFront();
  return (MemTable*) mem;
}

#if LISTDB_L0_CACHE == L0_CACHE_T_SIMPLE
  inline SimpleHashTable* ListDB::GetHashTable(int shard) {
    //return hash_table_[shard];
    return hash_table_[0];
  }
#elif LISTDB_L0_CACHE == L0_CACHE_T_STATIC
  inline StaticHashTableCache* ListDB::GetHashTable(int shard) {
    return hash_table_[shard];
  }
#elif LISTDB_L0_CACHE == L0_CACHE_T_DOUBLE_HASHING
  inline DoubleHashingCache* ListDB::GetHashTable(int shard) {
    return hash_table_[shard];
  }
#elif LISTDB_L0_CACHE == L0_CACHE_T_LINEAR_PROBING
  inline LinearProbingHashTableCache* ListDB::GetHashTable(int shard) {
    return hash_table_[shard];
  }
#endif

inline TableList* ListDB::GetTableList(int level, int shard) {
  auto tl = ll_[shard]->GetTableList(level);
  return tl;
}

template <typename T>
inline T* ListDB::GetTableList(int level, int shard) {
  auto tl = ll_[shard]->GetTableList(level);
  return (T*) tl;
}

void ListDB::FlushMemTable(MemTableFlushTask* task, CompactionWorkerData* td) {
#if LISTDB_FLUSH_MEMTABLE_TO_L1 == 1
  fprintf(stdout, "FlushMemTableToL1 is not implemented for IUL.\n");
  abort();
#endif
  if (task->shard == 0) fprintf(stdout, "FlushMemTable: %p\n", task->imm); //show progress juwon
  // Check Reference Counter
  while (task->imm->w_RefCount() > 0) {
    continue;
  }

  // Flush (IUL)
#if 0
  BraidedPmemSkipList* l0_skiplist = new BraidedPmemSkipList();
  for (int i = 0; i < kNumRegions; i++) {
    auto& arena = log_[i][task->shard];
    l0_skiplist->BindArena(arena->pool_id(), arena);
  }
  l0_skiplist->Init();
#else
  auto l0_skiplist = task->imm->l0_skiplist();
#endif

  auto skiplist = task->imm->skiplist();
  auto mem_node = skiplist->head();
  mem_node = mem_node->next[0].load(MO_RELAXED);

  using Node = PmemNode;
  Node* preds[kNumRegions][kMaxHeight];
  Node* pred;
  pred = l0_skiplist->head(l0_arena_[0][0]->pool_id());
  for (int i = 0; i < kNumRegions; i++) {
    int pool_id = l0_arena_[i][0]->pool_id();
    for (int j = 1; j < kMaxHeight; j++) {
      preds[i][j] = l0_skiplist->head(pool_id);
    }
  }

#ifdef LISTDB_L0_CACHE
  auto hash_table = GetHashTable(task->shard);
#endif

  uint64_t flush_cnt = 0;
  uint64_t begin_micros = Clock::NowMicros();
  INIT_REPORTER_CLIENT;

#ifdef LISTDB_BLOOM_FILTER
  BloomFilter* l0_bloom_filter = new BloomFilter(10,kMemTableCapacity/kNumShards/sizeof(PmemNode));
#endif

  while (mem_node) {
    //std::this_thread::yield();
#ifdef GROUP_LOGGING
    while (((std::atomic<uint64_t>*) &mem_node->value)->load(std::memory_order_relaxed) == 0) continue;
#endif
    int pool_id = ((PmemPtr*) &mem_node->value)->pool_id();
    int region = pool_id_to_region_[pool_id];
    Node* node = ((PmemPtr*) &mem_node->value)->get<Node>();
    int height = node->height();
    for (int i = 1; i < height; i++) {
      //std::this_thread::yield();
      preds[region][i]->next[i] = mem_node->value;
      preds[region][i] = ((PmemPtr*) &(preds[region][i]->next[i]))->get<Node>();
    }
    pred->next[0] = mem_node->value;
    pred = ((PmemPtr*) &(pred->next[0]))->get<Node>();

#if LISTDB_L0_CACHE == L0_CACHE_T_SIMPLE
    hash_table->Add(mem_node->key, mem_node->value);
#elif LISTDB_L0_CACHE == L0_CACHE_T_STATIC
    hash_table->Insert(mem_node->key, node);
#elif LISTDB_L0_CACHE == L0_CACHE_T_DOUBLE_HASHING
    hash_table->Insert(mem_node->key, node);
#elif LISTDB_L0_CACHE == L0_CACHE_T_LINEAR_PROBING
    hash_table->Insert(mem_node->key, node);
#endif

#ifdef LISTDB_BLOOM_FILTER
  l0_bloom_filter->AddKey(mem_node->key);
#endif

    REPORT_FLUSH_OPS(1);
    flush_cnt++;

    //std::this_thread::yield();
    mem_node = mem_node->next[0].load(MO_RELAXED);
  }
  REPORT_DONE;  // Up report all remainings
  uint64_t end_micros = Clock::NowMicros();
  td->flush_cnt += flush_cnt;
  td->flush_time_usec += (end_micros - begin_micros);

#ifdef LISTDB_BLOOM_FILTER
  PmemTable* l0_table = new PmemTable(kMemTableCapacity / kNumShards, l0_skiplist, l0_bloom_filter);
#else
  PmemTable* l0_table = new PmemTable(kMemTableCapacity / kNumShards, l0_skiplist);
#endif
  l0_table->SetManifest(task->imm->l0_manifest());

  task->imm->SetPersistentTable((Table*) l0_table);
  // TODO(wkim): Log this L0 table for recovery
  //task->imm->FinalizeFlush();
  task->memtable_list->CleanUpFlushedImmutables();
}

void ListDB::ManualFlushMemTable(int shard) {
#ifndef LISTDB_WAL
  auto tl = reinterpret_cast<MemTableList*>(ll_[shard]->GetTableList(0));

  auto table = tl->GetFront();
  auto table2 = table->Next();

  if (table->type() != TableType::kMemTable || (table2 != nullptr && table2->type() != TableType::kPmemTable)) {
    fprintf(stdout, "ManualFlushMemTable failed\n");
    return;
  }

  tl->CreateNewFront();  // Level0or1Status is set to kFull.
  
  // Flush (IUL)
  auto l0_skiplist = reinterpret_cast<MemTable*>(table)->l0_skiplist();

  auto skiplist = reinterpret_cast<MemTable*>(table)->skiplist();
  auto mem_node = skiplist->head();
  mem_node = mem_node->next[0].load(MO_RELAXED);

  using Node = PmemNode;
  Node* preds[kNumRegions][kMaxHeight];
  Node* pred;
  pred = l0_skiplist->head(l0_arena_[0][0]->pool_id());
  for (int i = 0; i < kNumRegions; i++) {
    int pool_id = l0_arena_[i][0]->pool_id();
    for (int j = 1; j < kMaxHeight; j++) {
      preds[i][j] = l0_skiplist->head(pool_id);
    }
  }

#ifdef LISTDB_L0_CACHE
  auto hash_table = GetHashTable(shard);
#endif

#ifdef LISTDB_BLOOM_FILTER
  BloomFilter* l0_bloom_filter = new BloomFilter(10,kMemTableCapacity/kNumShards/sizeof(PmemNode));
#endif

  INIT_REPORTER_CLIENT;
  while (mem_node) {
#ifdef GROUP_LOGGING
    while (((std::atomic<uint64_t>*) &mem_node->value)->load(std::memory_order_relaxed) == 0) continue;
#endif
    int pool_id = ((PmemPtr*) &mem_node->value)->pool_id();
    int region = pool_id_to_region_[pool_id];
    Node* node = ((PmemPtr*) &mem_node->value)->get<Node>();
    int height = node->height();
    for (int i = 1; i < height; i++) {
      preds[region][i]->next[i] = mem_node->value;
      preds[region][i] = ((PmemPtr*) &(preds[region][i]->next[i]))->get<Node>();
    }
    pred->next[0] = mem_node->value;
    pred = ((PmemPtr*) &(pred->next[0]))->get<Node>();

#if LISTDB_L0_CACHE == L0_CACHE_T_SIMPLE
    hash_table->Add(mem_node->key, mem_node->value);
#elif LISTDB_L0_CACHE == L0_CACHE_T_STATIC
    hash_table->Insert(mem_node->key, node);
#elif LISTDB_L0_CACHE == L0_CACHE_T_DOUBLE_HASHING
    hash_table->Insert(mem_node->key, node);
#elif LISTDB_L0_CACHE == L0_CACHE_T_LINEAR_PROBING
    hash_table->Insert(mem_node->key, node);
#endif

#ifdef LISTDB_BLOOM_FILTER
  l0_bloom_filter->AddKey(mem_node->key);
#endif

    REPORT_FLUSH_OPS(1);

    //std::this_thread::yield();
    mem_node = mem_node->next[0].load(MO_RELAXED);
  }
  REPORT_DONE;  // Up report all remainings

#ifdef LISTDB_BLOOM_FILTER
  PmemTable* l0_table = new PmemTable(kMemTableCapacity / kNumShards, l0_skiplist, l0_bloom_filter);
#else
  PmemTable* l0_table = new PmemTable(kMemTableCapacity / kNumShards, l0_skiplist);
#endif
  l0_table->SetManifest(reinterpret_cast<MemTable*>(table)->l0_manifest());
  reinterpret_cast<MemTable*>(table)->SetPersistentTable((Table*) l0_table);
  // TODO(wkim): Log this L0 table for recovery
  tl->CleanUpFlushedImmutables();
#else

  auto tl = reinterpret_cast<MemTableList*>(ll_[shard]->GetTableList(0));

  auto table = tl->GetFront();
  auto table2 = table->Next();

  if (table->type() != TableType::kMemTable || (table2 != nullptr && table2->type() != TableType::kPmemTable)) {
    fprintf(stdout, "ManualFlushMemTable failed\n");
    return;
  }

  tl->NewFront();
  
  // Flush (WAL)
  auto l0_skiplist = reinterpret_cast<MemTable*>(table)->l0_skiplist();

  auto skiplist = reinterpret_cast<MemTable*>(table)->skiplist();
  auto mem_node = skiplist->head();
  mem_node = mem_node->next[0].load(MO_RELAXED);

  using Node = PmemNode;
  Node* preds[kNumRegions][kMaxHeight];
  Node* pred;
  pred = l0_skiplist->head(l0_arena_[0][0]->pool_id());
  for (int i = 0; i < kNumRegions; i++) {
    int pool_id = l0_arena_[i][0]->pool_id();
    for (int j = 1; j < kMaxHeight; j++) {
      preds[i][j] = l0_skiplist->head(pool_id);
    }
  }

#ifdef LISTDB_L0_CACHE
  auto hash_table = GetHashTable(shard);
#endif

  INIT_REPORTER_CLIENT;
  while (mem_node) {
#ifdef GROUP_LOGGING
    while (((std::atomic<uint64_t>*) &mem_node->value)->load(std::memory_order_relaxed) == 0) continue;
#endif
    int mem_value_pool_id = ((PmemPtr*) &mem_node->value)->pool_id();
    int region = pool_id_to_region_[mem_value_pool_id];

    auto rnd = Random::GetTLSInstance();

#ifdef LISTDB_SKIPLIST_CACHE
    static const unsigned int kBranching = 2;
#else
    static const unsigned int kBranching = 4;
#endif
    int height = 1;
    if (rnd->Next() % std::max<int>(1, (kBranching / kNumRegions)) == 0) {
      height++;
      while (height < kMaxHeight && ((rnd->Next() % kBranching) == 0)) {
        height++;
      }
    }

    size_t node_size = sizeof(PmemNode) + (height - 1) * sizeof(uint64_t);
    //auto node_paddr = l0_arena_[region][shard]->Allocate(node_size);
    //Node* node = node_paddr.get<PmemNode>();
    auto pool = Pmem::pool<pmem_log_root>(mem_value_pool_id);
    pmem::obj::persistent_ptr<char[]> p_buf;
    pmem::obj::make_persistent_atomic<char[]>(pool, p_buf, node_size);
    Node* node = (PmemNode*) p_buf.get();
    PmemPtr node_paddr = PmemPtr(mem_value_pool_id, ((uintptr_t) node - (uintptr_t) pool.handle()));

    node->tag = height;
    node->value = mem_node->value;
    _mm_sfence();
    node->key = mem_node->key;
    clwb(node, sizeof(PmemNode) - sizeof(uint64_t));

    pred->next[0] = node_paddr.dump();
    _mm_sfence();
    clwb(&pred->next[0], 8);
    for (int i = 1; i < height; i++) {
      preds[region][i]->next[i] = node_paddr.dump();
      preds[region][i] = ((PmemPtr*) &(preds[region][i]->next[i]))->get<Node>();
    }
    pred = ((PmemPtr*) &(pred->next[0]))->get<Node>();

#if LISTDB_L0_CACHE == L0_CACHE_T_SIMPLE
    hash_table->Add(mem_node->key, mem_node->value);
#elif LISTDB_L0_CACHE == L0_CACHE_T_STATIC
    hash_table->Insert(mem_node->key, node);
#elif LISTDB_L0_CACHE == L0_CACHE_T_DOUBLE_HASHING
    hash_table->Insert(mem_node->key, node);
#elif LISTDB_L0_CACHE == L0_CACHE_T_LINEAR_PROBING
    hash_table->Insert(mem_node->key, node);
#endif

    REPORT_FLUSH_OPS(1);

    //std::this_thread::yield();
    mem_node = mem_node->next[0].load(MO_RELAXED);
  }
  REPORT_DONE;  // Up report all remainings

  PmemTable* l0_table = new PmemTable(kMemTableCapacity, l0_skiplist);
  l0_table->SetManifest(reinterpret_cast<MemTable*>(table)->l0_manifest());
  reinterpret_cast<MemTable*>(table)->SetPersistentTable((Table*) l0_table);
  // TODO(wkim): Log this L0 table for recovery
  tl->CleanUpFlushedImmutables();
#endif
}

void ListDB::L1Compaction(int shard) {
  fprintf(stdout, "Manual L1 Compaction : shard %d\n",shard); //show progress juwon
  
  using Node2 = PmemNode2;

  //Get Table of L1
  auto l1_tl = ll_[shard]->GetTableList(1);
  auto l1_table = (PmemTable2*) l1_tl->GetFront();
  auto l1_skiplist = l1_table->skiplist();

  //Get first node of L1
  int pool_id = l1_arena_[0][shard]->pool_id();
  auto l1_node = l1_skiplist->head(pool_id);
  auto l1_node_paddr_dump = l1_node->next[0].next_ptr;
  l1_node = (Node2*) ((PmemPtr*) &l1_node_paddr_dump)->get();

  //preparation for write to disk
  //declare temporal structor for kvpair (not kvpairs)
  struct KVpair {
    Key key;
    Value value;
  };


  int num_kvpairs_in_buffer = kDiskWriteBatchSize/sizeof(KVpair);
  if(kDiskWriteBatchSize%sizeof(KVpair) != 0) std::cerr << "KVpair size is not match to Disk Write Batch Size!\n"  << std::endl;

  KVpair* aligned_buffer = (KVpair*)std::aligned_alloc(kDiskBlockSize, kDiskWriteBatchSize);
  if (aligned_buffer == nullptr) {
      std::cerr << "Failed to allocate memory." << std::endl;
  }

  
  int aligned_buffer_index = 0;

  // Open the file with O_DIRECT
  char pathname[100]; // adjust the size as needed
  sprintf(pathname, "/juwon/SSTable/L2_SSTable_shard_%d", shard);
  int fd = open(pathname, O_WRONLY | O_CREAT | O_DIRECT, S_IRUSR | S_IWUSR);
  if (fd == -1) {
      // Handle error
      printf("file discriptor error!\n");
      return;
  }

  INIT_REPORTER_CLIENT; //REPORT_FLUSH_OPS(1);
//start loop
  while(l1_node){
    auto l1_node_kvpairs_paddr = l1_node_paddr_dump + sizeof(PmemNode2) + (l1_node->height-1)*sizeof(HintedPtr);
    auto l1_node_kvpairs = (KVpairs*) ((PmemPtr*) &l1_node_kvpairs_paddr)->get();
    uint64_t l1_node_cnt = l1_node_kvpairs->cnt;

    //start nested loop
    for(uint64_t k=0;k<l1_node_cnt;k++){
      aligned_buffer[aligned_buffer_index].key = l1_node_kvpairs->key[k];
      aligned_buffer[aligned_buffer_index].value = l1_node_kvpairs->value[k];
      aligned_buffer_index++;
      //if buffer full, do write to disk
      if(aligned_buffer_index >= num_kvpairs_in_buffer){
        //ssize_t written = write(fd, aligned_buffer, kDiskWriteBatchSize);
        REPORT_L1_COMPACTION_OPS(aligned_buffer_index);
        //if (written == -1) {
            // Handle error
        //    printf("write system call error!\n");
        //}
        aligned_buffer_index = 0;
      }
    }

    l1_node_paddr_dump = l1_node->next[0].next_ptr;
    l1_node = (Node2*) ((PmemPtr*) &l1_node_paddr_dump)->get();
  }
//write remaining keys (for stylist db)
  if(aligned_buffer_index > 0){
      //ssize_t written = write(fd, aligned_buffer, ((aligned_buffer_index*sizeof(KVpair))/kDiskBlockSize + 1)*kDiskBlockSize);
      //if (written == -1) {
          // Handle error
      //    printf("write system call error!2\n");
      //}
      aligned_buffer_index = 0;
    }
  // Clean up buffer
  close(fd);

  REPORT_DONE;

  fprintf(stdout, "Manual L1 Compaction done. : shard %d\n",shard); //show progress juwon
}

void ListDB::LogStructuredMergeCompactionL0(CompactionWorkerData* td, L0CompactionTask* task){

#ifdef L0_COMPACTION_LATENCY_BREAKDOWN
  auto compaction_begin_tp = std::chrono::steady_clock::now();
  auto prev_tp = std::chrono::high_resolution_clock::now();
  size_t merge_latency_nanoseconds = 0;
  size_t scan_latency_nanoseconds = 0;
#endif

  if (task->shard == 0) fprintf(stdout, "L0 compaction\n");

  auto l0_manifest = task->l0->manifest<pmem_l0_info>();
  l0_manifest->status = Level0or1Status::kMergeInitiated;
  

  using Node = PmemNode;
  using Node2 = PmemNode2;
    //initialize node_cnt for checking number of compation nodes
  //uint64_t node_cnt = 0;
  //uint64_t new_node_cnt = 0;
  uint64_t pred_paddr_dump;
  
    //get l0 skiplist
    auto l0_skiplist = task->l0->skiplist();
    PmemPtr node_paddr = l0_skiplist->head_paddr();
    auto l0_node = node_paddr.get<Node>();
    //get l1 skiplist
    auto l1_tl = ll_[task->shard]->GetTableList(1);

    auto l1_table = (PmemTable2*) l1_tl->GetFront();
    auto l1_skiplist = l1_table->skiplist();
    auto l1_manifest = l1_table->manifest<pmem_l1_info>();
    
  #if defined(LISTDB_SKIPLIST_CACHE)
      static const unsigned int tmp_kBranching = 4;
  #else
      static const unsigned int tmp_kBranching = 4;
  #endif
    //set preds
    Node2* heads[kNumRegions];
    Node2* preds[kNumRegions][kMaxHeight];
    uint64_t cnts[kNumRegions][kMaxHeight];
    //각 height에 대한 head를 preds에 담아준다.
    bool head_only_flag = true;
    for (int i = 0; i < kNumRegions; i++) {
      int pool_id = l1_arena_[i][task->shard]->pool_id();
      heads[i] = l1_skiplist->head(pool_id);
      //check there is only head
    }
    for(int i=0; i<kNumRegions; i++){ 
      uint64_t l1_node_paddr_dump = heads[i]->next[0].next_ptr;
      auto l1_node = (Node2*) ((PmemPtr*) &l1_node_paddr_dump)->get();
      if(l1_node != nullptr){
        for(int j=0; j<kMaxHeight; j++){
          preds[i][j] = l1_node;
        }
        head_only_flag = false;
        pred_paddr_dump = l1_node_paddr_dump;
      }
    }
    //if no head, make head
    if(head_only_flag == true){
      //Allocate new manifest
      pmem::obj::persistent_ptr<pmem_l1_info> tmp_manifest;
      auto db_pool = Pmem::pool<pmem_db>(0);
      pmem::obj::make_persistent_atomic<pmem_l1_info>(db_pool, tmp_manifest);
      l1_table->SetManifest(tmp_manifest);
      l1_manifest = tmp_manifest;

      //first nodes of each numa
      Node2* first_node[kNumRegions];
      PmemPtr first_node_paddr[kNumRegions];

      for(int i=0; i<kNumRegions; i++){
        //set random numa region and height
        int height = kMaxHeight;
        int region = i;

        size_t node_size = sizeof(PmemNode2) + (height - 1) * sizeof(HintedPtr);
        //strictly adjacent allocation of node and kvpairs
        auto l1_node_paddr = l1_arena_[region][task->shard]->Allocate(node_size+sizeof(KVpairs));
        auto l1_node = (Node2*) ((PmemPtr*) &l1_node_paddr)->get();
        //new_node_cnt++;

        first_node[i] = l1_node;
        first_node_paddr[i] = l1_node_paddr;

        l1_node->min_key = i;
        //set height of l1 node
        l1_node->height = height;

        //allocate kvpairs to l1 node
        uint64_t kvpairs_paddr_dump = l1_node_paddr.dump()+node_size;
        auto kvpairs = (KVpairs*) ((PmemPtr*) &kvpairs_paddr_dump)->get();

        //initialize key/value/cnt of kvpairs
        kvpairs->key[0] = i;
        kvpairs->value[0] = i;
        kvpairs->cnt = 1;
        clwb(&kvpairs->key[0], sizeof(Key));
        clwb(&kvpairs->value[0], sizeof(uint64_t));
        clwb(&kvpairs->cnt, sizeof(uint64_t));
        _mm_sfence();

        
        clwb(l1_node, node_size);
        _mm_sfence();


        

        for(int j=0; j<kMaxHeight; j++){
          preds[i][j] = l1_node;
        }
        pred_paddr_dump = l1_node_paddr.dump();
        heads[i]->next[0].next_ptr = l1_node_paddr.dump();
        heads[i]->next[0].next_key = l1_node->min_key;
        clwb(&(heads[i]->next[0]), sizeof(HintedPtr));
        _mm_sfence();
      }

      //connect all first skiplist nodes of each numa
      for(int i=0; i<kNumRegions-1; i++){
        first_node[i]->next[0].next_ptr = first_node_paddr[i+1].dump();
        first_node[i]->next[0].next_key = first_node[i+1]->min_key;
      }

      //heads의 value를 각 numa당 skiplist node 개수로 설정해준다.
      for(int i=0; i<kNumRegions; i++){
        for(int j=0; j<kMaxHeight; j++){
          l1_manifest->cnt[i][j] = 1;
        }
        l1_manifest->recent_cnt_before_update[i] = 1;
      }

      head_only_flag = false;
    }
    //make head is done.
    //set cnts
    for(int i=0; i<kNumRegions; i++){
      for(int j=0; j<kMaxHeight; j++){
        cnts[i][j] = l1_manifest->cnt[i][j];
      }
    }

    //set next new node's region
    int region = 0;
    uint64_t min_cnt = cnts[0][0]; 
    for(int k=1; k<kNumRegions; k++){
      if(cnts[k][0]<min_cnt){
        min_cnt = cnts[k][0];
        region = k;
      }
    }

    //variables for compaction
    //buffers for newly inserting key/value
    Key key_buffer[NPAIRS];
    Value value_buffer[NPAIRS];
    uint64_t buffer_cnt = 0;

    //variables use for decide when to do insert or split
    Node2* curr = nullptr;
    uint64_t key_total_cnt = 1; // total Number of keys to insert a node(up to 2*NPAIRS) -> buffer_cnt + prev_cnt

    //variables for keep kvpairs for insert
    
    KVpairs* pred_kvpairs = nullptr;

    //update first node info
    while(l0_node != nullptr && l0_node->key.key_num() == 0){
      node_paddr = l0_node->next[0];
      l0_node = node_paddr.get<Node>();
    }//remove head nodes of l0
   //initializing of next_min_key, space_cnt is done.


    //
    // Start Compaction
    //
    while(l0_node != nullptr){
#ifdef L0_COMPACTION_YIELD
    std::this_thread::yield();
#endif
      //
      //1. Set L1 node
      //

      int search_start_height=0;
      while(search_start_height<kMaxHeight-1){
        if(preds[region][search_start_height+1]->next[search_start_height+1].next_key.Compare(l0_node->key) > 0) break;
        search_start_height++;
      }

      //search preds and succes at upper level
      bool move_next = false;
      for (int t = search_start_height; t > 0; t--) {
        while (true) { 
          if (preds[region][t]->next[t].next_key.Compare(l0_node->key) <= 0) {
            uint64_t curr_paddr_paddr = preds[region][t]->next[t].next_ptr;
            curr = (Node2*) ((PmemPtr*) &curr_paddr_paddr)->get();
            if(curr){
              preds[region][t] = curr;
              pred_paddr_dump = curr_paddr_paddr;
              move_next=true;
              continue;
            }
          }
          break;
        }
        //if higher preds is nearer to new node than lower preds, use result of searching higher preds 
        if(move_next){
          if(t>1) preds[region][t-1] = preds[region][t];
          else preds[0][0] = preds[region][t];
        }
      }


      //search preds and succes at braided level
        uint64_t curr_paddr_dump = preds[0][0]->next[0].next_ptr;
        curr = (Node2*) ((PmemPtr*) &curr_paddr_dump)->get();
        while (curr) {
          if (curr->min_key.Compare(l0_node->key) <= 0) {
            preds[0][0] = curr;
            pred_paddr_dump = curr_paddr_dump;
            curr_paddr_dump = curr->next[0].next_ptr;
            curr = (Node2*) ((PmemPtr*) &curr_paddr_dump)->get();
            continue;
          }
          break;
        }

        uint64_t pred_kvpairs_paddr = pred_paddr_dump + sizeof(PmemNode2) + (preds[0][0]->height - 1) * sizeof(HintedPtr);
        pred_kvpairs = (KVpairs*) ((PmemPtr*) &pred_kvpairs_paddr)->get();
        key_total_cnt = pred_kvpairs->cnt;

      //
      //2. Scan L0 nodes
      //
      while(true){
        if(l0_node==nullptr || (curr && (curr->min_key.Compare(l0_node->key) <= 0)) || key_total_cnt > NPAIRS) break; //conditions for merge and split
        //add l0_node key/value to buffer and increase buffer counter and total counter
        key_buffer[buffer_cnt] = l0_node->key;
        value_buffer[buffer_cnt] = l0_node->value;
        buffer_cnt++;
        key_total_cnt++;

        // Move on to next node of l0
        node_paddr = l0_node->next[0];
        l0_node = node_paddr.get<Node>();
      }


      //
      //2. Start (Split and ) Merge
      //
        #ifdef L0_COMPACTION_LATENCY_BREAKDOWN
          auto curr_tp = std::chrono::high_resolution_clock::now();
          auto scan_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(curr_tp - prev_tp);
          scan_latency_nanoseconds += scan_duration.count();
          prev_tp = curr_tp;
        #endif
        INIT_REPORTER_CLIENT;

        //
        //2-1. Start Split
        //
        if(key_total_cnt > NPAIRS){
          // 1) decide height deterministically
          int height = 2;
          uint64_t branching_factor = tmp_kBranching;
          while (height < kMaxHeight) {
            if(cnts[region][0]%branching_factor!=0) break;
            height++;
            branching_factor = branching_factor*tmp_kBranching;
          }

          // 2) calculate node size and allocate node and kvpair
          size_t node_size = sizeof(PmemNode2) + (height - 1) * sizeof(HintedPtr);
          auto l1_node_paddr = l1_arena_[region][task->shard]->Allocate(node_size+sizeof(KVpairs));
          auto l1_node = (Node2*) ((PmemPtr*) &l1_node_paddr)->get();
          uint64_t kvpairs_paddr_dump = l1_node_paddr.dump()+node_size;
          auto kvpairs = (KVpairs*) ((PmemPtr*) &kvpairs_paddr_dump)->get();
          //new_node_cnt++;

          //do insert in kvpair and decide min key of l1 node
          //일단 개수는 새로 생긴 l1 node의 kvpairs->cnt는 key_total_cnt/2 개이다 (3개면 1개)
          //넣어주기 위해서 넣어주는 곳을 pos = key_total_cnt/2 (혹은 kvpairs->cnt) 로 설정해준다. (pos는 넣어줘야 하는 곳을 의미한다.)
          kvpairs->cnt = key_total_cnt/2;
          uint64_t insert_pos = key_total_cnt/2;
          //zero for no duplicate key, non-zero for duplicate key deteced.
          uint64_t leftmost_duplicate_pos = 0;
          //counter temorarily representing prededecessor node's kvpairs counter
          uint64_t tmp_pred_kvpairs_cnt = pred_kvpairs->cnt;
          //save buffer_cnt for use REPORT_L0_COMPACTION
#ifdef REPORT_BACKGROUND_WORKS
          uint64_t tmp_buffer_cnt = buffer_cnt;
#endif

          //buffer_cnt와 tmp_pred_kvpairs_cnt 하나씩 줄이면서 끝에서 큰거를 하나씩 넣어준다. (buffer_cnt가 0에 도달하거나 pos가 0에 도달하면 break)
          while(buffer_cnt>0 && insert_pos>0){
            insert_pos--;
            //if biggest key of key_buffer is bigger
            int rv = key_buffer[buffer_cnt-1].Compare(pred_kvpairs->key[tmp_pred_kvpairs_cnt-1]);
            if(rv > 0){
              kvpairs->key[insert_pos] = key_buffer[buffer_cnt-1];
              kvpairs->value[insert_pos] = value_buffer[buffer_cnt-1];
              buffer_cnt--;              
            }
            //if biggest key of pred is bigger
            else if(rv < 0){
              kvpairs->key[insert_pos] = pred_kvpairs->key[tmp_pred_kvpairs_cnt-1];
              kvpairs->value[insert_pos] = pred_kvpairs->value[tmp_pred_kvpairs_cnt-1];
              tmp_pred_kvpairs_cnt--;                 
            }
            //if duplicate key detected
            else{
              //if first key of new node is duplicated, simply insert new version of key
              if(insert_pos==0){
                kvpairs->key[insert_pos] = key_buffer[buffer_cnt-1];
                kvpairs->value[insert_pos] = value_buffer[buffer_cnt-1];
                buffer_cnt--;
                //evict duplicated key
                tmp_pred_kvpairs_cnt--;      
              }
              else{
                kvpairs->key[insert_pos] = pred_kvpairs->key[tmp_pred_kvpairs_cnt-1];
                kvpairs->value[insert_pos] = pred_kvpairs->value[tmp_pred_kvpairs_cnt-1];
                tmp_pred_kvpairs_cnt--;
                leftmost_duplicate_pos = insert_pos;
              }
            }
          }
          //pos가 0에 도달안했으면 남아있는 tmp_pred_kvpairs_cnt 넣어준다. pos 0에 도달시 break;
          while(insert_pos>0){
            insert_pos--;
            kvpairs->key[insert_pos] = pred_kvpairs->key[tmp_pred_kvpairs_cnt-1];
            kvpairs->value[insert_pos] = pred_kvpairs->value[tmp_pred_kvpairs_cnt-1];
            tmp_pred_kvpairs_cnt--;   
          }

          //evict duplicate keys if exist
          if(leftmost_duplicate_pos!=0){
            uint64_t src = leftmost_duplicate_pos + 1;
            uint64_t dest = leftmost_duplicate_pos;
            while(src<kvpairs->cnt){
              //if key of src is duplicate key, skip copy it
              if(kvpairs->key[src].Compare(kvpairs->key[src-1])!=0){
                kvpairs->key[dest] = kvpairs->key[src];
                kvpairs->value[dest] = kvpairs->value[src];
                dest++;
              }
              src++;
            }
            kvpairs->cnt = dest;
          }

          //kvpairs->key[0] 가 l1_node의 min_key가 된다.
          l1_node->min_key = kvpairs->key[0];

          #ifdef L0_COMPACTION_LATENCY_BREAKDOWN
            curr_tp = std::chrono::high_resolution_clock::now();
            auto merge_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(curr_tp - prev_tp);
            merge_latency_nanoseconds += merge_duration.count();
            prev_tp = curr_tp;
          #endif

          // 3) find remain preds
          bool move_next = false;
          for (int t = kMaxHeight-1; t > 0; t--) {
            while (true) { 
              if (preds[region][t]->next[t].next_key.Compare(l1_node->min_key) <= 0) {
                uint64_t curr_paddr_paddr = preds[region][t]->next[t].next_ptr;
                curr = (Node2*) ((PmemPtr*) &curr_paddr_paddr)->get();
                if(curr){
                  preds[region][t] = curr;
                  pred_paddr_dump = curr_paddr_paddr;
                  move_next=true;
                  continue;
                }
              }
              break;
            }
            //if higher preds is nearer to new node than lower preds, use result of searching higher preds 
            if(move_next){
              if(t>1) preds[region][t-1] = preds[region][t];
              else preds[0][0] = preds[region][t];
            }
          }

          #ifdef L0_COMPACTION_LATENCY_BREAKDOWN
            curr_tp = std::chrono::high_resolution_clock::now();
            auto scan_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(curr_tp - prev_tp);
            scan_latency_nanoseconds += scan_duration.count();
            prev_tp = curr_tp;
          #endif
        
          // 4) update next pointers and kvpair_ptr of new l1_node
          l1_node->next[0].next_key = preds[0][0]->next[0].next_key;
          l1_node->next[0].next_ptr = preds[0][0]->next[0].next_ptr;
          for (int i = 1; i < height; i++) {
            l1_node->next[i].next_key =  preds[region][i]->next[i].next_key;
            l1_node->next[i].next_ptr =  preds[region][i]->next[i].next_ptr;
          }
          l1_node->height = height;

          // 5) update next pointers of predecessor nodes
          preds[0][0]->next[0].next_key = l1_node->min_key;
          preds[0][0]->next[0].next_ptr = l1_node_paddr.dump();
          clwb(&(preds[0][0]->next[0]), sizeof(HintedPtr));
          _mm_sfence();
          for (int i = 1 ;i < height; i++) {
            preds[region][i]->next[i].next_key = l1_node->min_key;
            preds[region][i]->next[i].next_ptr = l1_node_paddr.dump();
            clwb(&(preds[region][i]->next[i]), sizeof(HintedPtr));
            _mm_sfence();
            //clwb changed next pointer of preds node
          }

          // 6) clwb kvpairs and l1 node
          clwb(kvpairs, sizeof(KVpairs));
          _mm_sfence();
          clwb(l1_node, node_size);
          _mm_sfence();

          //new node가 연결되지 않았는데 l1 node의 cnt 부터 줄어들면 key를 볼 수 없다.
          pred_kvpairs->cnt = tmp_pred_kvpairs_cnt;
          key_total_cnt = pred_kvpairs->cnt + buffer_cnt;

          //report number of inserted keys for monitoring
          REPORT_L0_COMPACTION_OPS(tmp_buffer_cnt-buffer_cnt);
          
          // update counter of skiplist2 node
          for(int k=height-1; k>=0; k--){
            cnts[region][k]++;
          }

          //set next new node's region
          region = 0;
          uint64_t min_cnt = cnts[0][0]; 
          for(int k=1; k<kNumRegions; k++){
            if(cnts[k][0]<min_cnt){
              min_cnt = cnts[k][0];
              region = k;
            }
          }
        } //split of pred node is done.
        
        //
        //2-2. Start Merge of original Node (Overflow Node)
        //

        //insert position 은 key_total_cnt로 한다.
        uint64_t insert_pos = key_total_cnt;
        //zero for no duplicate key, non-zero for duplicate key deteced.
        uint64_t leftmost_duplicate_pos = 0;
        //counter temorarily representing prededecessor node's kvpairs counter
        uint64_t tmp_pred_kvpairs_cnt = pred_kvpairs->cnt;
        //save buffer_cnt for use REPORT_L0_COMPACTION
#ifdef REPORT_BACKGROUND_WORKS
        uint64_t tmp_buffer_cnt = buffer_cnt;
#endif
        //pred_kvpairs->cnt를 미리 key_total_cnt로 설정해준다. (삽입과정 중 검색하는 client가 가능한 모든 key를 볼수있게끔 하기 위해)
        pred_kvpairs->cnt = key_total_cnt;

        //insert position 상관없이 buffer_cnt가 소진될때까지 삽입한다.
        while(buffer_cnt>0){
          insert_pos--;
          //if biggest key of key_buffer is bigger
          int rv = key_buffer[buffer_cnt-1].Compare(pred_kvpairs->key[tmp_pred_kvpairs_cnt-1]);
          if(rv > 0){
            pred_kvpairs->key[insert_pos] = key_buffer[buffer_cnt-1];
            pred_kvpairs->value[insert_pos] = value_buffer[buffer_cnt-1];
            buffer_cnt--;              
          }
          //if biggest key of pred is bigger
          //이때 insert_pos는 절대 pred_kvpairs->cnt-1을 따라잡을수 없다.
          else if(rv<0){
            pred_kvpairs->key[insert_pos] = pred_kvpairs->key[tmp_pred_kvpairs_cnt-1];
            pred_kvpairs->value[insert_pos] = pred_kvpairs->value[tmp_pred_kvpairs_cnt-1];
            tmp_pred_kvpairs_cnt--;                 
          }
          //if duplicate key detected
          else{
            pred_kvpairs->key[insert_pos] = pred_kvpairs->key[tmp_pred_kvpairs_cnt-1];
            pred_kvpairs->value[insert_pos] = pred_kvpairs->value[tmp_pred_kvpairs_cnt-1];
            tmp_pred_kvpairs_cnt--;
            leftmost_duplicate_pos = insert_pos;  
          }
        }

        //evict duplicate keys if exist
        if(leftmost_duplicate_pos!=0){
          uint64_t src = leftmost_duplicate_pos + 1;
          uint64_t dest = leftmost_duplicate_pos;
          while(src<pred_kvpairs->cnt){
            //if key of src is duplicate key, skip copy it
            if(pred_kvpairs->key[src].Compare(pred_kvpairs->key[src-1])!=0){
              pred_kvpairs->key[dest] = pred_kvpairs->key[src];
              pred_kvpairs->value[dest] = pred_kvpairs->value[src];
              dest++;
            }
            src++;
          }
          pred_kvpairs->cnt = dest;
        }

        //삽입 후 pred_kvpairs를 clwb 해준다.
        clwb(pred_kvpairs, sizeof(KVpairs));
        _mm_sfence();

        //insert into preds node is done.

        //report number of inserted keys for monitoring
        REPORT_L0_COMPACTION_OPS(tmp_buffer_cnt-buffer_cnt);

        #ifdef L0_COMPACTION_LATENCY_BREAKDOWN
          curr_tp = std::chrono::high_resolution_clock::now();
          auto merge_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(curr_tp - prev_tp);
          merge_latency_nanoseconds += merge_duration.count();
          prev_tp = curr_tp;
        #endif

        REPORT_DONE;

    }//end of loop

    uint64_t total_cnt[kNumRegions] = {0};
    //set skiplist counter for each numa nodes
    for(int i=0; i<kNumRegions; i++){
      for(int j=0; j<kMaxHeight; j++){
        l1_manifest->cnt[i][j] = cnts[i][j];
        total_cnt[i] += cnts[i][j];
      }
    }
    
    //remove l0 table at l0 table list(because l0 compaction is done.). r means remove.
    auto rtable = task->memtable_list->GetFront();
    while (true) {
      auto next_table = rtable->Next();
      if (next_table) {
        if (next_table == (Table*) task->l0) {
          rtable->SetNext(nullptr);
          break;
        }
        rtable = next_table;
      } else {
        break;
      }
    }

    l0_manifest->status = Level0or1Status::kMergeDone;
  
  //if (task->shard == 0 ) fprintf(stdout, "number of compacted l0 nodes : %lu\n", node_cnt);
  //if (task->shard == 0 ) fprintf(stdout, "number of generated l1 nodes : %lu\n", new_node_cnt);

  #ifdef LISTDB_SKIPLIST_CACHE
   for(int i=0; i<kNumRegions; i++){
    //update only when number of nodes grow to update trigger ratio
    if(l1_manifest->recent_cnt_before_update[i]*kUpdateTriggerRatio <= total_cnt[i]){
#ifdef L0_COMPACTION_YIELD
    std::this_thread::yield();
#endif
      if (task->shard == 0 && i==0) fprintf(stdout, "updating lookup cache\n"); // test juwon
      l1_manifest->recent_cnt_before_update[i] = total_cnt[i];
      cache_[task->shard][i]->UpdateCache((PmemTable2List*)l1_tl);
    }
   }
  #endif
  
  #ifdef L0_COMPACTION_LATENCY_BREAKDOWN
    auto curr_tp = std::chrono::high_resolution_clock::now();
    auto scan_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(curr_tp - prev_tp);
    scan_latency_nanoseconds += scan_duration.count();

    auto compaction_end_tp = std::chrono::steady_clock::now();
    auto compaction_duration = std::chrono::duration_cast<std::chrono::milliseconds>(compaction_end_tp - compaction_begin_tp);
    auto compaction_latency = compaction_duration.count();
    printf("%lu\t%lu\t%lu\n",compaction_latency,scan_latency_nanoseconds/1000/1000,merge_latency_nanoseconds/1000/1000);
    //printf("%f\t%f\n",(double)scan_latency_nanoseconds/(scan_latency_nanoseconds+merge_latency_nanoseconds),(double)merge_latency_nanoseconds/(scan_latency_nanoseconds+merge_latency_nanoseconds));
  #endif
}//end of L0 Compaction

void ListDB::PrintDebugLsmState(int shard) {
  auto tl = GetTableList(0, shard);
  auto table = tl->GetFront();
  int mem_cnt = 0;
  int l0_cnt = 0;
  while (table) {
    if (table->type() == TableType::kMemTable) {
      mem_cnt++;
    } else if (table->type() == TableType::kPmemTable) {
      l0_cnt++;
    }
    table = table->Next();
  }

  fprintf(stdout, "mem: %d, L0: %d\n", mem_cnt, l0_cnt);
}

int ListDB::GetStatString(const std::string& name, std::string* buf) {
  int rv = 0;
  std::stringstream ss;
  if (name == "l1_cache_size") {
  #ifdef LISTDB_SKIPLIST_CACHE
    
  #else
    rv = 1;
  #endif
  } else if (name == "flush_stats") {
    for (int i = 0; i < kNumWorkers; i++) {
      ss << "worker " << i << ": flush_cnt = " << worker_data_[i].flush_cnt << " flush_time_usec = " << worker_data_[i].flush_time_usec << std::endl;
    }
  } else {
    ss << "Unknown name: " << name;
    rv = 1;
  }
  buf->assign(std::move(ss.str()));
  return rv;
}

#endif  // LISTDB_LISTDB_H_
