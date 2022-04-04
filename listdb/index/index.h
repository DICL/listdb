#ifndef LISTDB_INDEX_INDEX_H_
#define LISTDB_INDEX_INDEX_H_

#include "listdb/common.h"

class Index {
 public:
  virtual void insert(const Key& key) = 0;
};

#endif  // LISTDB_INDEX_INDEX_H_
