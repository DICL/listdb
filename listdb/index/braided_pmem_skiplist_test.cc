#include <sstream>
#include <fstream>

#include <experimental/filesystem>

#include "listdb/common.h"
#include "listdb/core/pmem_log.h"
#include "listdb/index/braided_pmem_skiplist.h"
#include "listdb/util/random.h"

namespace fs = std::experimental::filesystem::v1;

static pmem::obj::pool<pmem_log_root> pool_table[kNumRegions];
static int pool_id_table[kNumRegions];

void InitPoolSet() {
  // Create poolset file
  for (int i = 0; i < kNumRegions; i++) {
    std::stringstream pss;
    pss << "/pmem" << i << "/wkim/pmem_skiplist_test";
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
    pool_table[i] = Pmem::pool<pmem_log_root>(id);
    pool_id_table[i] = id;
  }
}

void InsertAndLookupSingleElement() {
  InitPoolSet();
  auto sl = new BraidedPmemSkipList(pool_id_table[0]);
  PmemLog* arena[kNumRegions];
  for (int i = 0; i < kNumRegions; i++) {
    arena[i] = new PmemLog(pool_id_table[i], 0);
    sl->BindArena(i, arena[i]);
  }
  sl->Init();

  Random rnd(1);
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && ((rnd.Next() % kBranching) == 0)) {
    height++;
  }
  using PNode = BraidedPmemSkipList::Node;
  size_t node_size = sizeof(PNode) + (height - 1) * 8;
  auto node_paddr = arena[0]->Allocate(node_size);
  PNode* node = (PNode*) node_paddr.get();
  node->key = 5;
  node->tag = height;
  node->value = 10;

  printf("Insert KV = { %lu, %lu }\n", 5ul, 10ul);
  sl->Insert(node_paddr);
  printf("Lookup key = %lu\n", 5ul);
  auto found_paddr = sl->Lookup(5, 0);
  PNode* found = (PNode*) found_paddr.get();
  printf("value = %lu\n", found->value);

  delete sl;
  for (int i = 0; i < kNumRegions; i++) {
    delete arena[i];
  }
}

int main() {
  InsertAndLookupSingleElement();
  return 0;
}
