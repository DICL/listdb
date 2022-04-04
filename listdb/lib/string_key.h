#ifndef LISTDB_LIB_STRING_KEY_H_
#define LISTDB_LIB_STRING_KEY_H_

#include <string>
#include <string_view>

class StringKey {
 public:
  StringKey(const std::string& key);
  // The size of allocated memory
  size_t size() const;
  int Compare(const StringKey& other) const;

 private:
  char data_[1];
};

#endif  // LISTDB_LIB_STRING_KEY_H_
