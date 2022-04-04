#include <iostream>

#include "listdb/lib/numa.h"

int main() {
  Numa::Init();
  for (int i = 0; i < Numa::num_cpus(); i++) {
    std::cout << "num=" << i << ", cpu=" << Numa::CpuSequenceRR(i) << std::endl;
  }
  return 0;
}
