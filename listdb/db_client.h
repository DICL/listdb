#ifndef LISTDB_DB_CLIENT_H_
#define LISTDB_DB_CLIENT_H_

#include <algorithm>
#include <vector>

#include "listdb/common.h"
#include "listdb/listdb.h"
#include "listdb/util.h"
#include "listdb/util/random.h"

#define LEVEL_CHECK_PERIOD_FACTOR 1

//#define LOG_NTSTORE
class DBClient {
 public:
  using MemNode = ListDB::MemNode;
  using PmemNode = ListDB::PmemNode;
  using PmemNode2 = ListDB::PmemNode2;
  using KVpairs = ListDB::KVpairs;
  using HintedPtr = ListDB::HintedPtr;

  DBClient(ListDB* db, int id, int region);

  void SetRegion(int region);

  void Put(const Key& key, const Value& value);

  bool Get(const Key& key, Value* value_out);

  void Put_m(const Key& key, const Value& value);

  bool Get_m(const Key& key, Value* value_out);

  void Put_b(size_t key_num);

  bool Get_b(const Key& key, Value* value_out);

  bool Scan(const Key& key, uint64_t scan_num, std::vector<uint64_t>* values_out);


#if defined(LISTDB_STRING_KEY) && defined(LISTDB_WISCKEY)
  void PutStringKV(const std::string_view& key_sv, const std::string_view& value);
  bool GetStringKV(const std::string_view& key_sv, Value* value_out);
#endif
  
  //void ReserveLatencyHistory(size_t size);
  
  size_t pmem_get_cnt() { return pmem_get_cnt_; }
  size_t search_visit_cnt() { return search_visit_cnt_; }
  size_t height_visit_cnt(int h) { return height_visit_cnt_[h]; }
  

 private:
  int DramRandomHeight();
  int PmemRandomHeight();

  static int KeyShard(const Key& key);

#ifdef LISTDB_EXPERIMENTAL_SEARCH_LEVEL_CHECK
  PmemPtr LevelLookup(const Key& key, const int pool_id, const int level, BraidedPmemSkipList* skiplist);
#endif
  PmemPtr Lookup(const Key& key, const int pool_id, BraidedPmemSkipList* skiplist);
  bool Lookupl1(const Key& key, const int pool_id, PackedPmemSkipList* skiplist, const int shard, Value* value_out);
  PmemPtr LookupRangel1(const Key& key, const int pool_id, PackedPmemSkipList* skiplist, const int shard, uint64_t scan_num, std::vector<uint64_t>* values_out);

  ListDB* db_;
  int id_;
  int region_;
  int l0_pool_id_;
  int l1_pool_id_;
  Random rnd_;
  PmemLog* log_[kNumShards];
#ifdef LISTDB_WISCKEY
  PmemBlob* value_blob_[kNumShards];
#endif
  //BraidedPmemSkipList* bsl_[kNumShards];
  size_t pmem_get_cnt_ = 0;
  size_t search_visit_cnt_ = 0;
  size_t height_visit_cnt_[kMaxHeight] = {};

#ifdef GROUP_LOGGING
  struct LogItem {
    Key key;
    uint64_t tag;
    Value value;
    MemNode* mem_node;
    //uint64_t offset;
  };
  std::vector<LogItem> log_group_[kNumShards];
  size_t log_group_alloc_size_[kNumShards];
#endif

  //std::vector<std::chrono::duration<double>> latencies_;
};

DBClient::DBClient(ListDB* db, int id, int region) : db_(db), id_(id), region_(region % kNumRegions), rnd_(id) {
  for (int i = 0; i < kNumShards; i++) {
    log_[i] = db_->log(region_, i);
#ifdef LISTDB_WISCKEY
    value_blob_[i] = db_->value_blob(region_, i);
#endif
  }
  l0_pool_id_ = db_->l0_pool_id(region_);
  l1_pool_id_ = db_->l1_pool_id(region_);
}

void DBClient::SetRegion(int region) {
  region_ = region;
  for (int i = 0; i < kNumShards; i++) {
    log_[i] = db_->log(region_, i);
#ifdef LISTDB_WISCKEY
    value_blob_[i] = db_->value_blob(region_, i);
#endif
  }
}

void DBClient::Put(const Key& key, const Value& value) {
#ifndef GROUP_LOGGING
  int s = KeyShard(key);

  uint64_t pmem_height = PmemRandomHeight();
  size_t iul_entry_size = sizeof(PmemNode) + (pmem_height - 1) * sizeof(uint64_t);
  size_t kv_size = key.size() + sizeof(Value);

  // Determine L0 id
  auto mem = db_->GetWritableMemTable(kv_size, s);
  uint64_t l0_id = mem->l0_id();

  // Write log
  auto log_paddr = log_[s]->Allocate(iul_entry_size);
  PmemNode* iul_entry = (PmemNode*) log_paddr.get();
#ifdef LOG_NTSTORE
  _mm_stream_pi((__m64*) &iul_entry->tag, (__m64) pmem_height);
  _mm_stream_pi((__m64*) &iul_entry->value, (__m64) value);
  //_mm_sfence();
  _mm_stream_pi((__m64*) &iul_entry->key, (__m64) (uint64_t) key);
#else
  iul_entry->tag = (l0_id << 32) | pmem_height;
  iul_entry->value = value;
  clwb(&iul_entry->tag, 16);
  _mm_sfence();
  iul_entry->key = key;
  clwb(iul_entry, 8);
  //clwb(iul_entry, sizeof(PmemNode) - sizeof(uint64_t));
#endif

  // Create skiplist node
  uint64_t dram_height = DramRandomHeight();
  MemNode* node = (MemNode*) malloc(sizeof(MemNode) + (dram_height - 1) * sizeof(uint64_t));
  node->key = key;
  node->tag = (l0_id << 32) | dram_height;
  node->value = log_paddr.dump();
  memset((void*) &node->next[0], 0, dram_height * sizeof(uint64_t));

  auto skiplist = mem->skiplist();
  skiplist->Insert(node);

  mem->w_UnRef();
#else
  int s = KeyShard(key);

  uint64_t height = RandomHeight();

  size_t kv_size = key.size() + sizeof(Value);


  // Create skiplist node
  MemNode* node = (MemNode*) malloc(sizeof(MemNode) + (height - 1) * sizeof(uint64_t));
  node->key = key;
  node->tag = height;
  //node->value = value;
  node->value = 0;
  memset(&node->next[0], 0, height * sizeof(uint64_t));

  auto mem = db_->GetWritableMemTable(kv_size, s);
  auto skiplist = mem->skiplist();

  size_t iul_entry_size = sizeof(PmemNode) + (height - 1) * sizeof(uint64_t);
  log_group_[s].emplace_back(LogItem{ key, height, value, node });
  log_group_alloc_size_[s] += iul_entry_size;
  if (log_group_[s].size() > 7) {
    int group_size = log_group_[s].size();
    //size_t log_space = 0;
    //std::vector<size_t> offset;
    //for (int i = 0; i < group_size; i++) {
    //  size_t iul_entry_size = sizeof(PmemNode) + (log_group_[s][i].tag - 1) * sizeof(uint64_t);
    //  offset.push_back(log_space);
    //  log_space += iul_entry_size;
    //}
    // Write log
    auto log_paddr = log_[s]->Allocate(log_group_alloc_size_[s]);
    char* p = (char*) log_paddr.get();
    auto pool_id = log_paddr.pool_id();
    auto pool_offset = log_paddr.offset();

    for (int i = 0; i < group_size; i++) {
      PmemNode* iul_entry = (PmemNode*) (p);
      iul_entry->key = log_group_[s][i].key;
      iul_entry->tag = log_group_[s][i].tag;
      iul_entry->value = log_group_[s][i].value;
      log_group_[s][i].mem_node->value = PmemPtr(pool_id, p).dump();
      p += sizeof(PmemNode) + (log_group_[s][i].tag - 1) * 8;
    }

    clwb(log_paddr.get(), log_group_alloc_size_[s]);
    log_group_[s].clear();
    log_group_alloc_size_[s] = 0;
  }

  skiplist->Insert(node);
  mem->w_UnRef();
#endif

  //size_t log_alloc_size = util::AlignedSize(8, LogWriter::Entry::ComputeAllocSize(key, height));
  //size_t node_alloc_size = util::AlignedSize(8, MemNode::ComputeAllocSize(key, height));

  //put_group_[s].emplace_back(key, value, height, log_alloc_size, node_alloc_size);
  //group_state_[s].kv_size += key.size() + /* value.size() */ 8;
  //group_state_[s].log_alloc_size += log_alloc_size;
  //group_state_[s].node_alloc_size += node_alloc_size;
  //if (group_state_[s].log_alloc_size >= 1024) {
  //  ProcessPutGroup(s);
  //}
}

bool DBClient::Get(const Key& key, Value* value_out) {
  int s = KeyShard(key);
  
  {
    MemTableList* tl = (MemTableList*) db_->GetTableList(0, s);

    auto table = tl->GetFront();
    while (table) {
      if (table->type() == TableType::kMemTable) {
        auto mem = (MemTable*) table;
        auto skiplist = mem->skiplist();
        auto found = skiplist->Lookup(key);
        if (found && found->key == key) {
          *value_out = found->value;
          return true;
        }
      } else if (table->type() == TableType::kPmemTable) {
        break;
      }
      table = table->Next();
    }

    //pmem_get_cnt_++;
    
    while (table) {
      auto pmem = (PmemTable*) table;
#ifdef LISTDB_BLOOM_FILTER
        if(!pmem->bloom_filter()->KeyMayMatch(key)){
          table = table->Next();
          continue;
        }
#endif
      auto skiplist = pmem->skiplist();
      //auto found_paddr = skiplist->Lookup(key, region_);
      auto found_paddr = Lookup(key, l0_pool_id_, skiplist);
      ListDB::PmemNode* found = (ListDB::PmemNode*) found_paddr.get();
      if (found && found->key == key) {
        //fprintf(stdout, "found on pmem\n");
        *value_out = found->value;
        return true;
      }
    
      table = table->Next();
    }
  }
  
  {
    // Level 1 Lookup
    auto tl = (PmemTable2List*) db_->GetTableList(1, s);
    auto table = tl->GetFront();
    while (table) {
      auto pmem = (PmemTable2*) table;
      auto skiplist = pmem->skiplist();
      if(Lookupl1(key, l1_pool_id_, skiplist, s, value_out)) return true;
      table = table->Next();
    }
  }
  //printf("fail to search! Key : %lu\n",key.key_num());//juwon test
  return false;
}

void DBClient:: Put_m(const Key& key, const Value& value){
  using Node = PmemNode;

  //if no head, make head
  Node* preds[kMaxHeight];
  preds[0] = db_->head_m_;
  if(!preds[0]){
    printf("no head !\n");
    size_t node_size = sizeof(PmemNode) + (kMaxHeight - 1) * sizeof(uint64_t);
    auto node_paddr = db_->allocate_pmemptr(node_size);
    preds[0] = node_paddr.get<PmemNode>();
    db_->set_head_m(preds[0]);
    preds[0]->tag = kMaxHeight;
    preds[0]->value = 0;
    preds[0]->key = 0;
    clwb(preds[0], node_size);
    _mm_sfence();
  }

  //decide height
  auto rnd = Random::GetTLSInstance();
  static const unsigned int kBranching = 4;
  int height = 1;
  if (rnd->Next() % kBranching == 0) {
    height++;
    while (height < kMaxHeight && ((rnd->Next() % kBranching) == 0)) {
      height++;
    }
  }

  for (int j = 1; j < kMaxHeight; j++) {
    preds[j] = preds[0];
  }

  //find position
  for (int i = kMaxHeight-1; i >= 0; i--) {
    uint64_t curr_paddr_dump = preds[i]->next[i];
    auto curr = (Node*) ((PmemPtr*) &curr_paddr_dump)->get();
    while (curr) {
        if (curr->key.Compare(key) < 0) {
           preds[i] = curr;
          curr_paddr_dump = preds[i]->next[i];
          curr = (Node*) ((PmemPtr*) &curr_paddr_dump)->get();
          continue;
        }
      break;
    }
    if(i>0) preds[i-1] = preds[i];
  }

  //allocate new node
  size_t node_size = sizeof(PmemNode) + (height - 1) * sizeof(uint64_t);
  auto node_paddr = db_->allocate_pmemptr(node_size);
  auto new_node = node_paddr.get<PmemNode>();
  new_node->tag = height;
  new_node->key = key;
  new_node->value = value;

  for(int i = height - 1; i >= 0; i--){
    new_node->next[i] = preds[i]->next[i];
    preds[i]->next[i] = node_paddr.dump();
    clwb(&preds[i]->next[i], 8);
    _mm_sfence();
  }
  clwb(new_node, node_size);
  _mm_sfence();
}

bool DBClient::Get_m(const Key& key, Value* value_out) {
  using Node = PmemNode;

  Node* pred = db_->head_m_;
  uint64_t curr_paddr_dump;
  Node* curr = nullptr;

  for (int i = kMaxHeight-1; i >= 0; i--) {
    while (true) {
      curr_paddr_dump = pred->next[i];
      curr = (Node*) ((PmemPtr*) &curr_paddr_dump)->get();
      if (curr) {
        if (curr->key.Compare(key) < 0) {
          pred = curr;
          continue;
        }
      }
      break;
    }
  }

  if (curr && curr->key.Compare(key)==0) {
    //fprintf(stdout, "found on pmem\n");
    *value_out = curr->value;
    return true;
  }
  //printf("fail to search!\n");
  return false;
}

void DBClient::Put_b(size_t key_num){
  auto key_array_paddr = db_->allocate_pmemptr(key_num*(sizeof(Key)));
  auto key_array = key_array_paddr.get<Key>();
  printf("array getted!\n");
  db_->set_key_array(key_array);
  for(size_t i=0; i<key_num; i++){
    key_array[i] = i+1;
  }
  auto value_array_paddr_dump = key_array_paddr.dump() + key_num*sizeof(Key);
  auto value_array = (Value*) ((PmemPtr*) &value_array_paddr_dump)->get();
  for(size_t i=0; i<key_num; i++){
    value_array[i] = i+1;
  }
  clwb(key_array, key_num*(sizeof(Key)+sizeof(Value)));
  _mm_sfence();
}

bool DBClient::Get_b(const Key& key, Value* value_out) {
  return false;
}


bool DBClient::Scan(const Key& key, uint64_t scan_num, std::vector<uint64_t>* values_out) {
  int s = KeyShard(key);
  {
    // Level 1 LookupRange
    auto tl = (PmemTable2List*) db_->GetTableList(1, s);
    auto table = tl->GetFront();
    while (table) {
      auto pmem = (PmemTable2*) table;
      auto skiplist = pmem->skiplist();
      //auto found_paddr = skiplist->Lookup(key, region_);
      LookupRangel1(key, l1_pool_id_, skiplist, s, scan_num, values_out);
      if (!values_out->empty()) return true;
      table = table->Next();
    }
  }

  return false;
}

#if defined(LISTDB_STRING_KEY) && defined(LISTDB_WISCKEY)
void DBClient::PutStringKV(const std::string_view& key_sv, const std::string_view& value) {
  Key& key = *((Key*) key_sv.data());
  //if (!key.Valid()) {
  //  fprintf(stdout, "key is not valid: %s, %zu, key_num=%zu\n", std::string(key_sv).c_str(), *((uint64_t*) key.data()), key.key_num());
  //}
  int s = KeyShard(key);

  uint64_t pmem_height = PmemRandomHeight();
  size_t iul_entry_size = sizeof(PmemNode) + (pmem_height - 1) * sizeof(uint64_t);
  //size_t kv_size = key.size() + value.size();

  // Write value
  size_t value_alloc_size = util::AlignedSize(8, 8 + value.size());
  auto value_paddr = value_blob_[s]->Allocate(value_alloc_size);
  char* value_p = (char*) value_paddr.get();
  *((size_t*) value_p) = value.size();
  value_p += sizeof(size_t);
  memcpy(value_p, value.data(), value.size());

  uint64_t dram_height = DramRandomHeight();
  size_t mem_node_size = sizeof(MemNode) + (dram_height - 1) * sizeof(uint64_t);
  auto mem = db_->GetWritableMemTable(mem_node_size, s);
  uint64_t l0_id = mem->l0_id();

  // Write log
  auto log_paddr = log_[s]->Allocate(iul_entry_size);
  PmemNode* iul_entry = (PmemNode*) log_paddr.get();
  iul_entry->tag = (l0_id << 32) | pmem_height;
  iul_entry->value = value_paddr.dump();
  clwb(&iul_entry->tag, 16);
  _mm_sfence();
  iul_entry->key = key;
  clwb(iul_entry, key.size());
  //clwb(iul_entry, sizeof(PmemNode) - sizeof(uint64_t));

  // Create skiplist node
  MemNode* node = (MemNode*) malloc(mem_node_size);
  node->key = key;
  node->tag = (l0_id << 32) | dram_height;
  //node->value = value;
  node->value = log_paddr.dump();
  memset((void*) &node->next[0], 0, dram_height * sizeof(uint64_t));

  auto skiplist = mem->skiplist();
  skiplist->Insert(node);
  mem->w_UnRef();
}

bool DBClient::GetStringKV(const std::string_view& key_sv, Value* value_out) {
  Key& key = *((Key*) key_sv.data());
  int s = KeyShard(key);
  {
    MemTableList* tl = (MemTableList*) db_->GetTableList(0, s);

    auto table = tl->GetFront();
    while (table) {
      if (table->type() == TableType::kMemTable) {
        auto mem = (MemTable*) table;
        auto skiplist = mem->skiplist();
        auto found = skiplist->Lookup(key);
        if (found && found->key == key) {
          PmemNode* p_node = PmemPtr::Decode<PmemNode>(found->value);
          *value_out = (uint64_t) PmemPtr::Decode<char>(p_node->value);
          return true;
        }
      } else if (table->type() == TableType::kPmemTable) {
        break;
      }
      table = table->Next();
    }
    //pmem_get_cnt_++;
    while (table) {
      auto pmem = (PmemTable*) table;
#ifdef LISTDB_BLOOM_FILTER
        if(!pmem->bloom_filter()->KeyMayMatch(key)){
          table = table->Next();
          continue;
        }
#endif
        auto skiplist = pmem->skiplist();
        //auto found_paddr = skiplist->Lookup(key, region_);
        auto found_paddr = Lookup(key, l0_pool_id_, skiplist);
        ListDB::PmemNode* found = (ListDB::PmemNode*) found_paddr.get();
        if (found && found->key == key) {
          //fprintf(stdout, "found on pmem\n");
          //PmemPtr value_paddr(found->value);
          //char* value_buf = (char*) value_paddr.get();
          //std::string_view value_sv(value_buf + 8, *((size_t*) value_buf));
          //fprintf(stdout, "key: %s, value: %s\n", found->key.data(), value_sv.data());
          //*value_out = found->value;
          *value_out = (uint64_t) PmemPtr::Decode<char>(found->value);
          return true;
        }
      
      table = table->Next();
    }
  }
  {
    // Level 1 Lookup
    auto tl = (PmemTable2List*) db_->GetTableList(1, s);
    auto table = tl->GetFront();
    while (table) {
      auto pmem = (PmemTable2*) table;
      auto skiplist = pmem->skiplist();
      if(Lookupl1(key, l1_pool_id_, skiplist, s, value_out)){
        *value_out = (uint64_t) PmemPtr::Decode<char>(*value_out);
        return true;
      }
      table = table->Next();
    }
  }
  return false;
}
#endif

inline int DBClient::PmemRandomHeight() {
#ifdef LISTDB_SKIPLIST_CACHE
  static const unsigned int kBranching = 2;
#else
  static const unsigned int kBranching = 4;
#endif
  int height = 1;
#if 1
  if (rnd_.Next() % std::max<int>(1, (kBranching / kNumRegions)) == 0) {
    height++;
    while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
      height++;
    }
  }
#else
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
#endif
  return height;
}

inline int DBClient::DramRandomHeight() {
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
  return height;
}

inline int DBClient::KeyShard(const Key& key) {
#ifndef LISTDB_RANGE_SHARD
  return key.key_num() % kNumShards;
#else
  return key.key_num() / kShardSize;
#endif
}

#ifdef LISTDB_EXPERIMENTAL_SEARCH_LEVEL_CHECK
PmemPtr DBClient::LevelLookup(const Key& key, const int region, const int level, BraidedPmemSkipList* skiplist) {
  using Node = PmemNode;
  Node* pred = skiplist->head(pool_id);
  uint64_t curr_paddr_dump;
  Node* curr;
  int height = pred->height();

  // NUMA-local upper layers
  for (int i = height - 1; i >= 1; i--) {
    while (true) {
      curr_paddr_dump = pred->next[i];
      curr = (Node*) ((PmemPtr*) &curr_paddr_dump)->get();
      if (curr) {
        if (rnd_.Next() % LEVEL_CHECK_PERIOD_FACTOR == 0) {
          int curr_level = (curr->tag & 0xf00) >> 8;
          if (curr_level > level) {
            fprintf(stdout, "Level 1 detected. Skip to L1 Search.");
            return 0;  // PmemPtr(0).get() == nullptr
          }
        }
        if (curr->key.Compare(key) < 0) {
          pred = curr;
          continue;
        }
      }
      break;
    }
  }

  // Braided bottom layer
  if (pred == skiplist->head(pool_id)) {
    pred = skiplist->head(0);
  }
  while (true) {
    curr_paddr_dump = pred->next[0];
    curr = (Node*) ((PmemPtr*) &curr_paddr_dump)->get();
    if (curr) {
      if (rnd_.Next() % LEVEL_CHECK_PERIOD_FACTOR == 0) {
        int curr_level = (curr->tag & 0xf00) >> 8;
        if (curr_level > level) {
          fprintf(stdout, "Level 1 detected. Skip to L1 Search.");
          return 0;  // PmemPtr(0).get() == nullptr
        }
      }
      if (curr->key.Compare(key) < 0) {
        pred = curr;
        continue;
      }
    }
    //fprintf(stdout, "lookupkey=%zu, curr->key=%zu\n", key, curr->key);
    break;
  }
  return curr_paddr_dump;
}
#endif

PmemPtr DBClient::Lookup(const Key& key, const int pool_id, BraidedPmemSkipList* skiplist) {
  using Node = PmemNode;
  Node* pred = skiplist->head(pool_id);
  //search_visit_cnt_++;
  //height_visit_cnt_[kMaxHeight - 1]++;
  uint64_t curr_paddr_dump;
  Node* curr;
  int height = pred->height();

  // NUMA-local upper layers
  for (int i = height - 1; i >= 1; i--) {
    while (true) {
      curr_paddr_dump = pred->next[i];
      curr = (Node*) ((PmemPtr*) &curr_paddr_dump)->get();
      if (curr) {
        //search_visit_cnt_++;
        //height_visit_cnt_[i]++;
        if (curr->key.Compare(key) < 0) {
          pred = curr;
          continue;
        }
      }
      break;
    }
  }

  // Braided bottom layer
  if (pred == skiplist->head(pool_id)) {
    if (pool_id != skiplist->primary_pool_id()) {
      //search_visit_cnt_++;
      //height_visit_cnt_[kMaxHeight - 1]++;
    }
    pred = skiplist->head();
  }
  while (true) {
    curr_paddr_dump = pred->next[0];
    curr = (Node*) ((PmemPtr*) &curr_paddr_dump)->get();
    if (curr) {
      //search_visit_cnt_++;
      //height_visit_cnt_[0]++;
      if (curr->key.Compare(key) < 0) {
        pred = curr;
        continue;
      }
    }
    //fprintf(stdout, "lookupkey=%zu, curr->key=%zu\n", key, curr->key);
    break;
  }
  return curr_paddr_dump;
}

bool DBClient::Lookupl1(const Key& key, const int pool_id, PackedPmemSkipList* skiplist, const int shard, Value* value_out) {
  using Node2 = PmemNode2;
  uint64_t pred_paddr_dump;
  Node2* pred;
  Node2* curr;
  int height;

  
#ifdef LISTDB_SKIPLIST_CACHE
  auto c = db_->skiplist_cache(shard, db_->pool_id_to_region(pool_id));
  height = c->GetCacheHeight();
  uint64_t tmp_paddr;
  int rv = c->LookupLessThanOrEqualsTo(key, &tmp_paddr);

  if(rv>=0){
    pred_paddr_dump = tmp_paddr;
    pred = (Node2*) ((PmemPtr*) &pred_paddr_dump)->get();
  }
  else{
    pred = skiplist->head(pool_id);
    pred_paddr_dump = pred->next[0].next_ptr;
    //pass through dummy node (head node)
    pred = (Node2*) ((PmemPtr*) &pred_paddr_dump)->get();
  }
#else
    pred = skiplist->head(pool_id);
    pred_paddr_dump = pred->next[0].next_ptr;
    //pass through dummy node (head node)
    pred = (Node2*) ((PmemPtr*) &pred_paddr_dump)->get();
    height = kMaxHeight;

#endif

  //search_visit_cnt_++;
  //height_visit_cnt_[height - 1]++;

  // NUMA-local upper layers
  for (int i = height - 1; i >= 1; i--) {
    while (true) {
      //if(key==8674865052476271390 || key==4497455375325908531 || key==3772020186569430559 || key==7571122414589934652 || key==4495033681488005219) printf("finding %lu, nextkey %lu, region %d, height %d\n",key,pred->next[i].next_key,db_->pool_id_to_region(pool_id),i);
      if (pred->next[i].next_key.Compare(key) <= 0){
        uint64_t curr_paddr_dump = pred->next[i].next_ptr;
        curr = (Node2*) ((PmemPtr*) &curr_paddr_dump)->get();
        if (curr) {
          //search_visit_cnt_++;
          //height_visit_cnt_[i]++;
          pred = curr;
          pred_paddr_dump = curr_paddr_dump;
          continue;
        }
      }
      break;
    }
  }

  //uint64_t pool_id_tmp = ((PmemPtr)curr_paddr_dump).pool_id();
  //if(shard==0) printf("region is %llu\n",pool_id_tmp);

  // Braided bottom layer

    while (true) {
      //if(key==8674865052476271390 || key==4497455375325908531 || key==3772020186569430559 || key==7571122414589934652 || key==4495033681488005219) printf("finding %lu, nextkey %lu, region %d height 0\n",key,pred->next[0].next_key,db_->pool_id_to_region(pool_id));
      if (pred->next[0].next_key.Compare(key) <= 0){
        uint64_t curr_paddr_dump = pred->next[0].next_ptr;
        curr = (Node2*) ((PmemPtr*) &curr_paddr_dump)->get();
        if (curr) {
          //search_visit_cnt_++;
          //height_visit_cnt_[0]++;
          pred = curr;
          pred_paddr_dump = curr_paddr_dump;
          continue;
        }
      }
      break;
    }

    auto pred_kvpairs_paddr = pred_paddr_dump + sizeof(PmemNode2) + (pred->height-1)*sizeof(HintedPtr);
    auto pred_kvpairs = (KVpairs*) ((PmemPtr*) &pred_kvpairs_paddr)->get();

    uint64_t pred_cnt = pred_kvpairs->cnt;
    for(uint64_t k=0;k<pred_cnt;k++){
      if(pred_kvpairs->KeyByIndex(k).Compare(key) == 0){
        *value_out = pred_kvpairs->ValueByIndex(k);
        //double check for conflict cases
        if(pred_kvpairs->KeyByIndex(k).Compare(key) == 0) return true;
      }
    }

    //if(key==8674865052476271390 || key==4497455375325908531 || key==3772020186569430559 || key==7571122414589934652 || key==4495033681488005219) printf("finding %lu, minkey %lu, region %d, fail\n",key,pred_kvpairs->KeyByIndex(0),db_->pool_id_to_region(pool_id));
    //if(key==8674865052476271390 || key==4497455375325908531 || key==3772020186569430559 || key==7571122414589934652 || key==4495033681488005219){
    //  printf("which in %lu: \n",pred_kvpairs->KeyByIndex(0));
    //  for(uint64_t k=0;k<pred_cnt;k++) printf("%lu ",pred_kvpairs->KeyByIndex(k));
    //  printf("\n");
    //}

    /*
    //check case of conflict scenario
    while(true){
      if(pred->next[0].next_key.Compare(key) <= 0){
        uint64_t curr_paddr_dump = pred->next[0].next_ptr;
        curr = (Node2*) ((PmemPtr*) &curr_paddr_dump)->get();
        if (curr) {
          //search_visit_cnt_++;
          //height_visit_cnt_[0]++;
          pred = curr;
          pred_paddr_dump = curr_paddr_dump;
          continue;
        }
        else return false;
      }
      else return false;

      while (true) {
        if(pred->next[0].next_key.Compare(key) <= 0){
          uint64_t curr_paddr_dump = pred->next[0].next_ptr;
          curr = (Node2*) ((PmemPtr*) &curr_paddr_dump)->get();
          if (curr) {
            //search_visit_cnt_++;
            //height_visit_cnt_[0]++;
            pred = curr;
            pred_paddr_dump = curr_paddr_dump;
            continue;
          }
          break;
        }
      }

      auto pred_kvpairs_paddr = pred_paddr_dump + sizeof(PmemNode2) + (pred->height-1)*sizeof(HintedPtr);
      auto pred_kvpairs = (KVpairs*) ((PmemPtr*) &pred_kvpairs_paddr)->get();

      uint64_t pred_cnt = pred_kvpairs->cnt;
      for(uint64_t k=0;k<pred_cnt;k++){
        if(pred_kvpairs->KeyByIndex(k).Compare(key) == 0){
          *value_out = pred_kvpairs->ValueByIndex(k);
          //double check for conflict cases
          if(pred_kvpairs->KeyByIndex(k).Compare(key) == 0) return true;
        }
      }
      
    }
    */
    return false;
  
}

PmemPtr DBClient::LookupRangel1(const Key& key, const int pool_id, PackedPmemSkipList* skiplist, const int shard, uint64_t scan_num, std::vector<uint64_t>* values_out) {
  using Node2 = PmemNode2;
  uint64_t pred_paddr_dump;
  Node2* pred;
  Node2* curr;
  int height;

  
#ifdef LISTDB_SKIPLIST_CACHE
  auto c = db_->skiplist_cache(shard, db_->pool_id_to_region(pool_id));
  height = c->GetCacheHeight();
  uint64_t tmp_paddr;
  int rv = c->LookupLessThanOrEqualsTo(key, &tmp_paddr);

  if(rv>=0){
    pred_paddr_dump = tmp_paddr;
    pred = (Node2*) ((PmemPtr*) &pred_paddr_dump)->get();
  }
  else{
    pred = skiplist->head(pool_id);
    pred_paddr_dump = pred->next[0].next_ptr;
    //pass through dummy node (head node)
    pred = (Node2*) ((PmemPtr*) &pred_paddr_dump)->get();
  }
#else
    pred = skiplist->head(pool_id);
    pred_paddr_dump = pred->next[0].next_ptr;
    //pass through dummy node (head node)
    pred = (Node2*) ((PmemPtr*) &pred_paddr_dump)->get();
    height = kMaxHeight;

#endif

  //search_visit_cnt_++;
  //height_visit_cnt_[height - 1]++;

  // NUMA-local upper layers
  for (int i = height - 1; i >= 1; i--) {
    while (true) {
      if (pred->next[i].next_key.Compare(key) <= 0){
        uint64_t curr_paddr_dump = pred->next[i].next_ptr;
        curr = (Node2*) ((PmemPtr*) &curr_paddr_dump)->get();
        if (curr) {
          //search_visit_cnt_++;
          //height_visit_cnt_[i]++;
          pred = curr;
          pred_paddr_dump = curr_paddr_dump;
          continue;
        }
      }
      break;
    }
  }

  //uint64_t pool_id_tmp = ((PmemPtr)curr_paddr_dump).pool_id();
  //if(shard==0) printf("region is %llu\n",pool_id_tmp);

  // Braided bottom layer
  while (true) {
    if (pred->next[0].next_key.Compare(key) <= 0){
      uint64_t curr_paddr_dump = pred->next[0].next_ptr;
      curr = (Node2*) ((PmemPtr*) &curr_paddr_dump)->get();
      if (curr) {
        //search_visit_cnt_++;
        //height_visit_cnt_[0]++;
        pred = curr;
        pred_paddr_dump = curr_paddr_dump;
        continue;
      }
    }
    break;
  }

  uint64_t scan_cnt = scan_num;

  auto pred_kvpairs_paddr = pred_paddr_dump + sizeof(PmemNode2) + (pred->height-1)*sizeof(HintedPtr);
  auto pred_kvpairs = (KVpairs*) ((PmemPtr*) &pred_kvpairs_paddr)->get();
  uint64_t result_paddr = pred_kvpairs_paddr;
  if(scan_cnt==0) return result_paddr;

  //start scan from given range
  uint64_t pred_cnt = pred_kvpairs->cnt;
  std::vector<std::pair<Key, Value>> sorted_kvpairs;
    sorted_kvpairs.reserve(pred_kvpairs->cnt);
    for (size_t i = 0; i < pred_kvpairs->cnt; i++) {
      sorted_kvpairs.emplace_back(pred_kvpairs->KeyByIndex(i), pred_kvpairs->ValueByIndex(i));
    }
    std::sort(sorted_kvpairs.begin(), sorted_kvpairs.end(), [](const std::pair<Key, Value>& a, const std::pair<Key, Value>& b) {
      return a.first.Compare(b.first) < 0;
    });

  for(uint64_t k=0;k<pred_cnt;k++){

    if(sorted_kvpairs[k].first.Compare(key) < 0) continue;
    values_out->push_back(sorted_kvpairs[k].second);
    scan_cnt--;
    if(scan_cnt==0) return result_paddr;
  }

  //if scan counter remains, scan next nodes 
  while (scan_cnt>0) {
    //move on to next node
    uint64_t curr_paddr_dump = pred->next[0].next_ptr;
    curr = (Node2*) ((PmemPtr*) &curr_paddr_dump)->get();

    if (curr) {
      auto curr_kvpairs_paddr = curr_paddr_dump + sizeof(PmemNode2) + (curr->height-1)*sizeof(HintedPtr);
      auto curr_kvpairs = (KVpairs*) ((PmemPtr*) &curr_kvpairs_paddr)->get();
      //search_visit_cnt_++;
      //height_visit_cnt_[0]++;

      uint64_t curr_cnt = curr_kvpairs->cnt;
      std::vector<std::pair<Key, Value>> sorted_kvpairs;
        sorted_kvpairs.reserve(curr_kvpairs->cnt);
        for (size_t i = 0; i < curr_kvpairs->cnt; i++) {
          sorted_kvpairs.emplace_back(curr_kvpairs->KeyByIndex(i), curr_kvpairs->ValueByIndex(i));
        }
        std::sort(sorted_kvpairs.begin(), sorted_kvpairs.end(), [](const std::pair<Key, Value>& a, const std::pair<Key, Value>& b) {
          return a.first.Compare(b.first) < 0;
        });
      for(uint64_t k=0;k<curr_cnt;k++){

        if(sorted_kvpairs[k].first.Compare(key) < 0) continue;
        values_out->push_back(sorted_kvpairs[k].second);
        scan_cnt--;
        if(scan_cnt==0) return result_paddr;
      }      
      
      pred = curr;
      pred_paddr_dump = curr_paddr_dump;
      continue;
    }
    break;
  }
  
  return result_paddr;
}

#endif  // LISTDB_DB_CLIENT_H_
