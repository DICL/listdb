#ifndef LISTDB_CORE_INTEGER_KEY_H_
#define LISTDB_CORE_INTEGER_KEY_H_

#include <cstdlib>
#include <cstdint>

class IntegerKey {
 public:
  IntegerKey(const uint64_t key);
  operator uint64_t() const { return data_; }
  size_t size() const;
  uint64_t key_num() const { return data_; }
  int Compare(const IntegerKey& other) const;

  const char* data() const { return (char*) &data_; }

  bool Valid() const { return data_ != 0; }

 private: 
  uint64_t data_;
};

inline IntegerKey::IntegerKey(const uint64_t key) : data_(key) { }

inline size_t IntegerKey::size() const {
  return sizeof(data_);
}

inline int IntegerKey::Compare(const IntegerKey& other) const {
  if (data_ < other.data_) {
    return -1;
  } else if (data_ > other.data_) {
    return 1;
  }
  return 0;
  // No performance difference
  //return data_ - other.data_;
}

inline bool operator< (const IntegerKey& lhs, const IntegerKey& rhs) {
  return lhs.Compare(rhs) < 0;
}

#endif  // LISTDB_CORE_INTEGER_KEY_H_
