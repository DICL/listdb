#ifndef LISTDB_COMMON_H_
#define LISTDB_COMMON_H_

#include <sched.h>
#include <unistd.h>
#include <sys/syscall.h>

#include <atomic>
#include <cassert>
#include <cstdio>

//#define GROUP_LOGGING
//#define L1_COW
#define LOOKUP_CACHE
#define L0_STATIC_HASH


#ifndef LISTDB_STRING_KEY
#include "listdb/core/integer_key.h"
#define Key IntegerKey
#else
#include "listdb/core/fixed_length_string_key.h"
constexpr size_t kStringKeyLength = 16;
#define Key FixedLengthStringKey<kStringKeyLength>
#endif
#define Value uint64_t

#define MO_RELAXED std::memory_order_relaxed

constexpr int kNumRegions = 2;
constexpr int kNumShards = 128;
#ifdef LISTDB_RANGE_SHARD
constexpr uint64_t kShardSize = std::numeric_limits<uint64_t>::max() / kNumShards + (kNumShards > 1);
#endif

//constexpr size_t kDramCapacity = 10 * (1ull << 30);
//constexpr size_t kMemTableCapacity = 64 * (1ull << 20);
//constexpr int kMaxNumMemTables = 4;
constexpr int kMaxNumMemTables = 8;
//constexpr size_t kMemTableCapacity = 256 * (1ull << 20);
constexpr size_t kMemTableCapacity = 1 * (1ull << 30) / kMaxNumMemTables;

constexpr int kMaxHeight = 15;

#ifdef LISTDB_L1_LRU
constexpr int kNumCachedLevels = 12;
constexpr int kLruMaxHeight = 20;
#endif

#ifdef LISTDB_SKIPLIST_CACHE
constexpr size_t kSkipListCacheCardinality = 4;
#define SkipListCacheRep SkipListCache<kSkipListCacheCardinality>

constexpr uint16_t kSkipListCacheMaxHeight = 15;
constexpr uint16_t kSkipListCacheBranching = 4;

constexpr int kSkipListCacheMinPmemHeight = 6;
constexpr size_t kSkipListCacheCapacity = (90ull << 20);
#endif

constexpr int kNumDramLevels = 1;
constexpr int kNumPmemLevels = 1;
constexpr int kNumLevels = kNumDramLevels + kNumPmemLevels;

constexpr int kNumWorkers = 40;

constexpr size_t kPmemLogBlockSize = 4 * (1ull<<20) / kNumShards;
constexpr size_t kPmemBlobBlockSize = kPmemLogBlockSize;

//constexpr uint64_t kHTMask = 0x0fffffff;
#ifndef LISTDB_SKIPLIST_CACHE
//constexpr size_t kHTSize = kHTMask + 1;
constexpr size_t kHTSize = 150ull * 1000 * 1000;
#else
#ifdef L0_STATIC_HASH
//constexpr size_t kHTSize = ((1536ull<<20) - kSkipListCacheCapacity) / 8;
constexpr size_t kHTSize = ((1024ull<<20) - kSkipListCacheCapacity) / 8;
#else
constexpr size_t kHTSize = ((1536ull<<20) - kSkipListCacheCapacity) / 24;
#endif
#endif

enum ValueType {
  kTypeAnchor = 0x0,
  kTypeShortcut = 0x1,
  kTypeValue = 0x2,
  kTypeDeletion = 0x3
};

enum class TableType {
  kMemTable,
  kPmemTable
};

enum class TaskType {
  kMemTableFlush,
  kL0Compaction
};

inline void SetAffinity(int coreid) {
  coreid = coreid % sysconf(_SC_NPROCESSORS_ONLN);
  cpu_set_t mask;
  CPU_ZERO(&mask);
  CPU_SET(coreid, &mask);
#ifndef NDEBUG
  int rc = sched_setaffinity(syscall(__NR_gettid), sizeof(mask), &mask);
  assert(rc == 0);
#else
  sched_setaffinity(syscall(__NR_gettid), sizeof(mask), &mask);
#endif
}

inline int GetChip() {
  unsigned long a,d,c;
  asm volatile("rdtscp" : "=a" (a), "=d" (d), "=c" (c));
  int chip = (c & 0xFFF000)>>12;
  //int core = c & 0xFFF;
  return chip;
}

inline int GetCore() {
  unsigned long a,d,c;
  asm volatile("rdtscp" : "=a" (a), "=d" (d), "=c" (c));
  //int chip = (c & 0xFFF000)>>12;
  int core = c & 0xFFF;
  return core;
}

#endif  // LISTDB_COMMON_H_
