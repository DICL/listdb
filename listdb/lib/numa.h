#ifndef LISTDB_LIB_NUMA_H_
#define LISTDB_LIB_NUMA_H_

#include <cassert>
#include <map>
#include <string>
#include <vector>

#include <numa.h>

class Numa {
 public:
  static void Init();
  static int num_cpus() { return num_cpus_; }
  inline static int CpuSequenceRR(const int id);

 private:
  inline static int num_cpus_;
  inline static int num_sockets_;
  inline static int num_cpus_per_socket_;
  inline static std::map<int, std::vector<int>> table_;
  inline static bool is_initialized_ = false;
};

void Numa::Init() {
  table_.clear();
  num_cpus_ = numa_num_configured_cpus();
  num_sockets_ = numa_num_configured_nodes();
  for (int i = 0; i < num_cpus_; i++) {
    int socket = numa_node_of_cpu(i);
    table_[socket].push_back(i);
  }
  num_cpus_per_socket_ = table_[0].size();
  is_initialized_ = true;
}

int Numa::CpuSequenceRR(const int num) {
  assert(is_initialized_ == true);
  const int socket = num % num_sockets_;
  const int seq_in_socket = num / num_sockets_;
  return table_[socket][seq_in_socket];
}

#endif  // LISTDB_LIB_NUMA_H_
