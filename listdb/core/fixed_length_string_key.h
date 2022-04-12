#ifndef LISTDB_CORE_FIXED_LENGTH_STRING_KEY_H_
#define LISTDB_CORE_FIXED_LENGTH_STRING_KEY_H_

#include <algorithm>
#include <cstring>

template <std::size_t N>
class FixedLengthStringKey {
 public:
  FixedLengthStringKey();
  FixedLengthStringKey(const char* key);
  FixedLengthStringKey(const std::string& key);
  FixedLengthStringKey(const uint64_t key);
  FixedLengthStringKey(const int key);
  size_t size() const { return N; }
  uint64_t key_num() const;
  int Compare(const FixedLengthStringKey<N>& other) const;

  const char* data() const { return data_; }

  bool operator==(const FixedLengthStringKey<N>& other) const;

 private:
  char data_[N];
};

template <std::size_t N>
inline FixedLengthStringKey<N>::FixedLengthStringKey() {
  memset(data_, 0, N);
}

template <std::size_t N>
inline FixedLengthStringKey<N>::FixedLengthStringKey(const char* key) {
  memcpy(data_, key, N);
}

template <std::size_t N>
inline FixedLengthStringKey<N>::FixedLengthStringKey(const std::string& key) {
  assert(key.size() <= N);
  memcpy(data_, key.data(), key.size());
  memset(data_ + key.size(), 0, N - key.size());
}

template <std::size_t N>
inline FixedLengthStringKey<N>::FixedLengthStringKey(const uint64_t key) {
  assert(N >= 8);
  memcpy(data_, &key, 8);
  memset(data_ + 8, 0, N - 8);
}

template <std::size_t N>
inline FixedLengthStringKey<N>::FixedLengthStringKey(const int key) {
  assert(N >= 8);
  memset(data_, 0, 4);
  memcpy(data_ + 4, &key, 4);
  memset(data_ + 8, 0, N - 8);
}

template <std::size_t N>
inline uint64_t FixedLengthStringKey<N>::key_num() const {
  unsigned char buf[sizeof(uint64_t)];
  memset(buf, 0, sizeof(buf));
  memcpy(buf, this->data(), std::min(this->size(), sizeof(uint64_t)));
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

template <std::size_t N>
inline int FixedLengthStringKey<N>::Compare(const FixedLengthStringKey<N>& other) const {
  return strncmp(data_, other.data_, N);
}

template <std::size_t N>
inline bool FixedLengthStringKey<N>::operator==(const FixedLengthStringKey<N>& other) const {
  return this->Compare(other) == 0;
}

template <std::size_t N>
inline bool operator< (const FixedLengthStringKey<N>& lhs, const FixedLengthStringKey<N>& rhs) {
  return lhs.Compare(rhs) < 0;
}

template <std::size_t N>
inline bool operator> (const FixedLengthStringKey<N>& lhs, const FixedLengthStringKey<N>& rhs) {
  return lhs.Compare(rhs) > 0;
}

#endif  // LISTDB_CORE_FIXED_LENGTH_STRING_KEY_H_
