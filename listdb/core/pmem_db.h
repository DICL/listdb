#ifndef LISTDB_CORE_PMEM_DB_H_
#define LISTDB_CORE_PMEM_DB_H_

#include <libpmemobj++/persistent_ptr.hpp>

#include "listdb/common.h"
#include "listdb/core/pmem_log.h"

enum class Level0Status {
  kInitialized,
  kPersisted,
  kMergeInitiated,
  kMergeDone,
};

struct pmem_db_shard;
struct pmem_l0_info;
struct pmem_l1_info;

struct pmem_db {
  // TODO: place pointer to shard info here
};

struct pmem_db_shard {
  int l0_cnt;
  pmem_l0_info l0_info[100];
  pmem_l1_info l1_info;
};

struct pmem_l0_info {
  int id;
  uint64_t log_begin;
  uint64_t log_end;
  Level0Status status;
  pmem::obj::persistent_ptr<char> head[kNumRegions];
};

struct pmem_l1_info {
  int id;
  pmem::obj::persistent_ptr<char> head[kNumRegions];
};

#endif  // LISTDB_CORE_PMEM_DB_H_
