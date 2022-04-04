#ifndef LISTDB_CORE_LOG_BUFFER_H_
#define LISTDB_CORE_LOG_BUFFER_H_

#include "listdb/common.h"

class LogBuffer {
 public:
  void Add(Key& key, uint64_t tag, Value& value, int height);
  void Flush();

 private:
  struct NodeInsertOp {
    Key key;
    uint64_t tag;
    Value value;
    int height;
    size_t log_size;

    NodeInsertOp(Key key_, uint64_t tag_, Value value_, int height_, size_t log_size_)
        : key(key_), tag(tag_), value(value_), height(height_), log_size(log_size_) { }
  };

  size_t buffered_size_;
  std::vector<NodeInsertOp> insert_ops_;
  PmemLog* log_;  
};

#endif  // LISTDB_CORE_LOG_BUFFER_H_
