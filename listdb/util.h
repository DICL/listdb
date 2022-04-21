#ifndef LISTDB_UTIL_H_
#define LISTDB_UTIL_H_

namespace util {

inline size_t AlignedSize(const size_t align, const size_t size) {
  int mod = size % align;
  return (mod == 0) ? size : size + (align - mod);
}

inline uint64_t KeyNum(const unsigned char* buf) {
  uint64_t number;
  number = static_cast<uint64_t>(buf[0]) << 56
        | static_cast<uint64_t>(buf[1]) << 48
        | static_cast<uint64_t>(buf[2]) << 40
        | static_cast<uint64_t>(buf[3]) << 32
        | static_cast<uint64_t>(buf[4]) << 24
        | static_cast<uint64_t>(buf[5]) << 16
        | static_cast<uint64_t>(buf[6]) << 8
        | static_cast<uint64_t>(buf[7]);
  return number;
}

}  // namespace util

#endif  // LISTDB_UTIL_H_
