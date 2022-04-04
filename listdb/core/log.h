#ifndef LISTDB_CORE_LOG_H_
#define LISTDB_CORE_LOG_H_

#include <atomic>
#include <mutex>

#include <libpmemobj.h>

#include "common.h"
#include "pmem.h"

class LogBlock {
 public:
  LogBlock(const int r);
  void init(const int s);
  void load(TOID(LogBlockBase) base);
  size_t fetch_add_size(const size_t size);
  TOID(LogBlockBase) base();
  char* data();

 private:
  const int r_;
  TOID(LogBlockBase) base_;
  uint64_t* p_;
  char* data_;
};

class Log {
 public:
  void init(const int s);  // s: shard
  void load(TOID(LogBase) log_base[]);
  uint64_t write_WAL(const int s, const std::string_view& key, const uint64_t value);
  uint64_t write_IUL(const int s, const std::string_view& key, const uint64_t value);

 private:
  LogBlock* blocks_[kMaxNumRegions];
  TOID(LogBase) log_base_[kMaxNumRegions];
  std::mutex mu_;
};

class LogWriter {
 public:
  virtual void Write(const Key& key, const Value& value) = 0;
};

#endif  // LISTDB_CORE_LOG_H_
