#ifndef LISTDB_CORE_LOG_WRITER_H_
#define LISTDB_CORE_LOG_WRITER_H_

#include "listdb/common.h"

class LogWriter {
 public:
  virtual uint64_t Write(const IKey& key, const Value& value) = 0;
};

#endif  // LISTDB_CORE_LOG_WRITER_H_
