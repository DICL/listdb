#ifndef LISTDB_CORE_PMEM_DB_H_
#define LISTDB_CORE_PMEM_DB_H_

#include <libpmemobj++/persistent_ptr.hpp>

#include "listdb/common.h"
#include "listdb/core/pmem_log.h"

struct pmem_db_shard;
struct pmem_l0_info;
struct pmem_l1_info;

enum class Level0Status {
  kInitialized,
  kFull,
  kPersisted,
  kMergeInitiated,
  kMergeDone,
};

struct pmem_db {
  // TODO: place pointer to shard info here
  pmem::obj::persistent_ptr<pmem_db_shard> shard[kNumShards];
};

struct pmem_db_shard {
  uint64_t l0_cnt;
  //int l0_head;
  //int l0_tail;
  //pmem_l0_info l0_info[100];  // TODO(wkim): Manage this as like a circular-array
  pmem::obj::persistent_ptr<pmem_l0_info> l0_list_head;
  pmem::obj::persistent_ptr<pmem_l1_info> l1_info;
};

struct pmem_l0_info {
  uint64_t id;
  //LogRange log_range[kNumRegions];
  Level0Status status;
  pmem::obj::persistent_ptr<pmem_l0_info> next;
  pmem::obj::persistent_ptr<char[]> head[kNumRegions];
};

struct pmem_l1_info {
  uint64_t id;
  pmem::obj::persistent_ptr<char[]> head[kNumRegions];
};

#endif  // LISTDB_CORE_PMEM_DB_H_
