#ifndef LISTDB_UTIL_H_
#define LISTDB_UTIL_H_

namespace util {

inline size_t AlignedSize(const size_t align, const size_t size) {
  int mod = size % align;
  return (mod == 0) ? size : size + (align - mod);
}

}  // namespace util

#endif  // LISTDB_UTIL_H_
