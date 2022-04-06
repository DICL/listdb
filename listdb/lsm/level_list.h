#ifndef LISTDB_LSM_LEVEL_LIST_H_
#define LISTDB_LSM_LEVEL_LIST_H_

#include "listdb/common.h"
//#include "listdb/lsm/level.h"
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
//  * locking to fetch two table info for a compaction
class SimpleLevelList {
 public:
  void Put(const Key& key, const Value& value);
  bool Get(const Key& key, void* const value_handle);
  //Iterator* NewIterator() = 0;
  //
 private:
  //Level* head_;
};

// List of TableList
class LevelList {
 public:
  void SetTableList(const int level, TableList* tl);

  TableList* GetTableList(const int level);

  template <typename T>
  T* GetTableList(const int level);

 private:
  TableList* table_lists_[kNumLevels];
};

inline void LevelList::SetTableList(const int level, TableList* tl) {
  table_lists_[level] = tl;
}

inline TableList* LevelList::GetTableList(const int level) {
  return table_lists_[level];
}

template <typename T>
inline T* LevelList::GetTableList(const int level) {
  return (T*) GetTableList(level);
}

#endif  // LISTDB_LSM_LEVEL_LIST_H_
