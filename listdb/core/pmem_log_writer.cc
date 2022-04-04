#include "listdb/core/pmem_log_writer.h"

uint64_t IulWriter::Write(const Key& key, const Value& value) {
#ifndef LISTDB_STRING_KEY
  auto node_def = _Index::NewNodeDef(key);
  const size_t alloc_size = _Index::Node::ComputeAllocSize(node_def);
#else
  fprintf(stderr, "notimpl\n");
  exit(1);
#endif
  void* buf = log_.Allocate()
}
