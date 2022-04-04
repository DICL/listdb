#ifndef LISTDB_LSM_LEVEL_H_
#define LISTDB_LSM_LEVEL_H_

#include "listdb/common.h"
#include "listdb/lsm/table.h"
#include "listdb/lsm/table_list.h"

// For client threads:
//  * Point queries:
//    - Put
//      call Put()
//    - Get
//      call Get()
//  * Range queries:
//    - Scan
//      use Iterator 
// For compaction threads:
//  * get mutable table for write
template <class T>
class Level {
 public:
  virtual void Put(const Key& key, const Value& value) = 0;
  virtual bool Get(const Key& key) = 0;
  //virtual Iterator* NewIterator() = 0;

 protected:
  TableList<T>* tables_;
  Level<T>* next_;
};

class TopLevel : public Level<MemTableStlMap> {
 public:
  // Pick the mutable table from the table list.
  //   call table::Put() if the table has enough space.
  //   Otherwise, add a new table at front and enqeue flush
  //     Stall if DRAM usage hit the limit.
  virtual void Put(const Key& key, const Value& value);
};

#endif  // LISTDB_LSM_LEVEL_H_
