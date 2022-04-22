#ifndef LISTDB_SKIPLIST_CACHE
#include <iostream>
int main() {
  std::cerr << "cmake .. -DSKIPLIST_CACHE=ON" << std::endl;
  return 1;
}
#else
#include <algorithm>
#include <fstream>
#include <memory>
#include <sstream>
#include <string_view>
#include <vector>
#include <iostream>

#include <experimental/filesystem>
namespace fs = std::experimental::filesystem::v1;

#include "listdb/pmem/pmem.h"
#include "listdb/core/pmem_log.h"
#include "listdb/core/skiplist_cache.h"
#include "listdb/port/port_posix.h"
#include "listdb/index/braided_pmem_skiplist.h"
#include "listdb/util/random.h"

static int pool_id;

void InitPoolSet() {
  // Create poolset file
  for (int i = 0; i < 1/* kNumRegions */; i++) {
    std::stringstream pss;
    pss << "/pmem" << i << "/wkim/cache_test";
    std::string path = pss.str();
    fs::remove_all(path);
    fs::create_directories(path);

    std::string poolset = path + ".set";
    std::fstream strm(poolset, strm.out);
    strm << "PMEMPOOLSET" << std::endl;
    strm << "OPTION SINGLEHDR" << std::endl;
    strm << "400G " << path << "/" << std::endl;
    strm.close();

    int id = Pmem::BindPoolSet<pmem_log_root>(poolset, "");
    pool_id = id;
    //pool_table[i] = Pmem::pool<pmem_log_root>(id);
    //pool_id_table[i] = id;
  }
}

#ifdef LISTDB_STRING_KEY
static int key_size_ = kStringKeyLength;
#else
static int key_size_ = sizeof(uint64_t);
#endif

std::string_view AllocateKey(std::unique_ptr<const char[]>* key_guard) {
  char* data = new char[key_size_];
  const char* const_data = data;
  key_guard->reset(const_data);
  return std::string_view(key_guard->get(), key_size_);
}

void GenerateKeyFromInt(uint64_t v, std::string_view* key) {
  char* start = const_cast<char*>(key->data());
  char* pos = start;

  int bytes_to_fill = std::min(key_size_ - static_cast<int>(pos - start), 8);
  if (port::kLittleEndian) {
    for (int i = 0; i < bytes_to_fill; ++i) {
      pos[i] = (v >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
    }
  } else {
    memcpy(pos, static_cast<void*>(&v), bytes_to_fill);
  }
  pos += bytes_to_fill;
  if (key_size_ > pos - start) {
    memset(pos, '0', key_size_ - (pos - start));
  }
}

using PmemNode = BraidedPmemSkipList::Node;
static PmemLog* log_;

PmemNode* CreateIULNode(const std::string_view& key_sv, int height) {
  Key& key = *((Key*) key_sv.data());
  size_t iul_entry_size = sizeof(PmemNode) + (height - 1) * sizeof(uint64_t);
  auto log_paddr = log_->Allocate(iul_entry_size);
  PmemNode* iul_entry = (PmemNode*) log_paddr.get();
  iul_entry->tag = height;
  iul_entry->value = 1234;
  iul_entry->key = key;
  return iul_entry;
}

int main() {
  InitPoolSet();
  log_ = new PmemLog(pool_id, 0);

  int n = 40;

  auto c = new SkipListCache<4>(pool_id, 16 * n / 2);

  std::unique_ptr<const char[]> key_guard;
  std::string_view key = AllocateKey(&key_guard);

  Random64 rand(999);
  std::vector<uint64_t> randints;

  for (int i = 0; i < n; i++) {
    randints.push_back(rand.Uniform(n<<3) + 1);
    std::cout << randints.back() << " ";
  }
  std::cout << std::endl;

  for (int i = 0; i < n; i++) {
    GenerateKeyFromInt(randints[i], &key);
    int height = rand.Next() % 12 + 1;
    PmemNode* p = CreateIULNode(key, height);
    std::cout << p << ": " << p->key.key_num() << std::endl;
    c->Insert(p);
  }

  uint64_t lkey_int;
  {
    lkey_int = randints[0];
    std::cout << "lookup key: " << lkey_int << std::endl;
    GenerateKeyFromInt(lkey_int, &key);
    Key& lookup_key = *((Key*) key.data());
    PmemNode* lt = c->LookupLessThan(lookup_key);
    std::cout << lt << std::endl;
  }
  {
    lkey_int = randints[0] + 1;
    std::cout << "lookup key: " << lkey_int << std::endl;
    GenerateKeyFromInt(lkey_int, &key);
    Key& lookup_key = *((Key*) key.data());
    PmemNode* lt = c->LookupLessThan(lookup_key);
    std::cout << lt << std::endl;
  }
  {
    lkey_int = randints[0] - 1;
    std::cout << "lookup key: " << lkey_int << std::endl;
    GenerateKeyFromInt(lkey_int, &key);
    Key& lookup_key = *((Key*) key.data());
    PmemNode* lt = c->LookupLessThan(lookup_key);
    std::cout << lt << std::endl;
  }

  std::string debug_str;
  c->GetDebugString("cursor", &debug_str);
  std::cout << debug_str << std::endl;

  return 0;
}
#endif  // LISTDB_SKIPLIST_CACHE
