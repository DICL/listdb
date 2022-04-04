#ifndef LISTDB_CORE_PMEM_WAL_WRITER_H_
#define LISTDB_CORE_PMEM_WAL_WRITER_H_

//#include <libpmemobj.h>

#include "listdb/common.h"
#include "listdb/util.h"
#include "listdb/core/log_writer.h"

class PmemWalWriter : public LogWriter {
 public:
  uint64_t Write(const IKey& key, const Value& value) override;
}

uint64_t PmemWalWriter::Write(const IKey& key, const Value& value) {
  const size_t alloc_size = util::AlignedSize(key.size() + 8 + 8);
  PmemPtr entry_ptr = log_.Allocate(alloc_size);
  char* p = (char*) entry_ptr.vaddr();
  uint64_t tag = 0;
  memcpy(p, key.data(), 8);
  memcpy(p + 8, &value, 8);
}

#endif  // LISTDB_CORE_PMEM_WAL_WRITER_H_
