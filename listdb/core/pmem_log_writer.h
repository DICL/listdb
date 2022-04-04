#ifndef LISTDB_CORE_PMEM_LOG_WRITER_H_
#define LISTDB_CORE_PMEM_LOG_WRITER_H_

#include "listdb/core/log_writer.h"

#include <libpmemobj.h>

#include "listdb/index/lockfree_pskiplist.h"

class IulWriter : public LogWriter {
 public:
  using _Index = lockfree_pskiplist;

  uint64_t Write(const Key& key, const Value& value) override;

 private:
  PMEMobjpool* pop_;
};


#endif  // LISTDB_CORE_PMEM_LOG_WRITER_H_
