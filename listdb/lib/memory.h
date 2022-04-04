#ifndef LISTDB_LIB_MEMORY_H_
#define LISTDB_LIB_MEMORY_H_

inline size_t aligned_size(const size_t align, const size_t size) {
  int mod = size % align;
  return (mod == 0) ? size : size + (align - mod);
}

inline void clwb(const void *addr, const size_t size) {
  char* a = (char*) addr;
  int s = size;
  while (s > 0) {
    asm volatile(".byte 0x66; xsaveopt %0" : "+m" \
      (*(volatile char *)(a)));
    a += 64;
    s -= 64;
  }
}

#endif  // LISTDB_LIB_MEMORY_H_
