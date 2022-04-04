#ifndef LISTDB_LIB_SINGLETON_H_
#define LISTDB_LIB_SINGLETON_H_

#include <cassert>

template <typename T>
class Singleton {
 public:
  Singleton(const Singleton<T>&) = delete;
  Singleton& operator=(const Singleton<T>&) = delete;       
  Singleton() {
    assert(!msSingleton);
    msSingleton = static_cast<T*>(this);
  }
  ~Singleton(void) {
    assert(msSingleton);
    msSingleton = 0;
  }
  static T& getSingleton(void) {
    assert(msSingleton);
    return (*msSingleton);
  }
 protected:
  static T* msSingleton; 
};

#endif  // LISTDB_LIB_SINGLETON_H_
