#ifndef LISTDB_ENV_H_
#define LISTDB_ENV_H_

class Env {
 public:
  // Priority for requesting bytes in rate limiter scheduler
  enum IOPriority {
    IO_LOW = 0,
    IO_MID = 1,
    IO_HIGH = 2,
    IO_USER = 3,
    IO_TOTAL = 4
  };
};

#endif  // LISTDB_ENV_H_
