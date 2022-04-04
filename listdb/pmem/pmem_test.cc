#include <iostream>
#include <fstream>
#include <stdexcept>
#include <experimental/filesystem>

#include <libpmemobj++/make_persistent_atomic.hpp>

#include "listdb/pmem/pmem.h"

namespace fs = std::experimental::filesystem::v1;

#define force_inline __attribute__((always_inline)) inline
/*
 * The x86 memory instructions are new enough that the compiler
 * intrinsic functions are not always available.  The intrinsic
 * functions are defined here in terms of asm statements for now.
 */
static force_inline void
clflushopt(const void *addr)
{
  asm volatile(".byte 0x66; clflush %0" : "+m" \
    (*(volatile char *)(addr)));
}
static force_inline void
clwb(const void *addr)
{
  asm volatile(".byte 0x66; xsaveopt %0" : "+m" \
    (*(volatile char *)(addr)));
}

void foo() {
  fs::remove_all("/pmem0/wkim/pmem_test2");
  fs::create_directories("/pmem0/wkim/pmem_test2");

  std::fstream s("/pmem0/wkim/pmem_test2.set", s.out);
  s << "PMEMPOOLSET" << std::endl;
  s << "OPTION SINGLEHDR" << std::endl;
  s << "400G /pmem0/wkim/pmem_test2/" << std::endl;
  s.close();

  struct log_block {
    //pmem::obj::p<char> data[64];
    char data[64];
  };

  struct pmem_log {
    pmem::obj::persistent_ptr<log_block> head;
  };

  int id = Pmem::BindPoolSet<pmem_log>("/pmem0/wkim/pmem_test2.set", "");
  auto pop = Pmem::pool<pmem_log>(id);
  auto root = pop.root();

  pmem::obj::persistent_ptr<log_block> block;
  pmem::obj::make_persistent_atomic<log_block>(pop, root->head);
}

int main() {
  fs::remove_all("/pmem0/wkim/pmem_test");
  fs::create_directories("/pmem0/wkim/pmem_test");

  std::fstream s("/pmem0/wkim/pmem_test.set", s.out);
  s << "PMEMPOOLSET" << std::endl;
  s << "OPTION SINGLEHDR" << std::endl;
  s << "400G /pmem0/wkim/pmem_test/" << std::endl;
  s.close();

  int id = Pmem::BindPoolSet("/pmem0/wkim/pmem_test.set", "");
  auto pop = Pmem::pool(id);

  pmem::obj::persistent_ptr<char[]> pbuf;
  auto proot = pmemobj_root(pop.handle(), 1);
  std::cout << (void*) ((char*) pop.handle() + proot.off) << std::endl;
  pmem::obj::make_persistent_atomic<char[]>(pop, pbuf, 13);
  char* buf = pbuf.get();
  uint64_t offset = buf - (char*) pop.handle();
  std::cout << pop.handle() << " " << (void*) buf << " " << offset << std::endl;
  sprintf(buf, "Hello World!");
  clwb(buf);
  pop.close();

  pop = pmem::obj::pool_base::open("/pmem0/wkim/pmem_test.set", "");
  buf = (char*) pop.handle() + offset;
  std::cout << pop.handle() << " " << (void*) buf << " " << offset << std::endl;
  std::cout << buf << std::endl;

  foo();

  return 0;
}
