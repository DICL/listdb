#ifndef LISTDB_DB_CLIENT_H_
#define LISTDB_DB_CLIENT_H_

#include <vector>

#include "listdb/common.h"
#include "listdb/listdb.h"
#include "listdb/util.h"

class DBClient {
 public:
  using MemNode = ListDB::MemNode;

  void Put(const Key& key, const Value& value);
  void ProcessPutGroup(const int shard);

 private:
  struct PutItem {
    const Key& k;
    const Value& v;
    const int height;
    const size_t log_alloc_size;
    const size_t node_alloc_size;

    PutItem(const Key& k_, const Value& v_, const int height_,const size_t log_alloc_size_,
            const size_t node_alloc_size_)
        : k(k_), v(v_), height(height_), log_alloc_size(log_alloc_size_), node_alloc_size(node_alloc_size_) { }
  };

  struct GroupState {
    size_t kv_size;
    size_t log_alloc_size;
    size_t node_alloc_size;
  };

  int RandomHeight();
  int KeyShard(const Key& key);

  Random rnd_(1);
  ListDB* db_;
  LogWriter* logger_[kNumShards][kNumRegions];
  std::vector<PutItem> put_group_[kNumShards];
  GroupState group_state_[kNumShards];
  //size_t group_kv_size_[kNumShards];
  //size_t group_log_alloc_size_[kNumShards];
  //size_t group_node_alloc_size_[kNumShards];
  std::vector<PmemPtr> log_paddrs_[kNumShards];
  std::vector<MemNode*> nodes_[kNumShards];
  int id_;
};

void DBClient::Put(const Key& key, const Value& value) {
  int s = KeyShard(key);

  int height = RandomHeight();
  size_t log_alloc_size = util::AlignedSize(8, LogWriter::Entry::ComputeAllocSize(key, height));
  size_t node_alloc_size = util::AlignedSize(8, MemNode::ComputeAllocSize(key, height));

  put_group_[s].emplace_back(key, value, height, log_alloc_size, node_alloc_size);
  group_state_[s].kv_size += key.size() + /* value.size() */ 8;
  group_state_[s].log_alloc_size += log_alloc_size;
  group_state_[s].node_alloc_size += node_alloc_size;
  if (group_state_[s].log_alloc_size >= 1024) {
    ProcessPutGroup(s);
  }
}

void DBClient::ProcessPutGroup(const int s) {
  std::sort(put_group_[s].begin(), put_group_[s].end(), PutItemComparator);

  int region = GetChip();
  auto log_group_paddr = logger_[s][region]->Allocate(group_state_[s].log_alloc_size);
  char* log_group_buf = (char*) log_group_paddr.get();
  char* p = log_group_buf;
  for (auto& item : put_group_[s]) {
    LogWriter::WriteOnBuf(p, item.k, item.v, item.height);
    p += item.log_alloc_size;
  }

  char* node_group_buf = aligned_alloc(8, group_state_[s].node_alloc_size);
  p = node_group_buf;
  uint64_t log_paddr = log_group_paddr.dump();
  for (auto& item : put_group_[s]) {
    auto node = MemNode::InitOnBuf(p, item.k, /*tag=*/0, item.v, log_paddr, item.height);
    nodes_[s].push_back(node);
    log_paddr += item.log_alloc_size;
    p += item.node_alloc_size;
  }

  db_->PutMany(group_kv_size_[s], nodes_);

  clwb(log_group_buf, group_state_[s].log_alloc_size);
  _mm_sfence();  // For what? against other logs in next batches

  put_group_[s].clear();
  log_paddrs_[s].clear();
  nodes_[s].clear();
  memset(&(group_state_[s]), 0, sizeof(GroupState));
}

inline int DBClient::RandomHeight() {
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
  return height;
}

inline int DBClient::KeyShard(const Key& key) {
  return key.key_num() % kNumShards;
}

#endif  // LISTDB_DB_CLIENT_H_
