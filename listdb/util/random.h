#ifndef LISTDB_UTIL_RANDOM_H_
#define LISTDB_UTIL_RANDOM_H_

#include <climits>
#include <random>
#include <thread>

#include "listdb/port/likely.h"

// A very simple random number generator.  Not especially good at
// generating truly random bits, but good enough for our needs in this
// package.
class Random {
 private:
  enum : uint32_t {
    M = 2147483647L  // 2^31-1
  };
  enum : uint64_t {
    A = 16807  // bits 14, 8, 7, 5, 2, 1, 0
  };

  uint32_t seed_;

  static uint32_t GoodSeed(uint32_t s) { return (s & M) != 0 ? (s & M) : 1; }

 public:
  // This is the largest value that can be returned from Next()
  enum : uint32_t { kMaxNext = M };

  explicit Random(uint32_t s) : seed_(GoodSeed(s)) {}

  void Reset(uint32_t s) { seed_ = GoodSeed(s); }

  uint32_t Next() {
    // We are computing
    //       seed_ = (seed_ * A) % M,    where M = 2^31-1
    //
    // seed_ must not be zero or M, or else all subsequent computed values
    // will be zero or M respectively.  For all other values, seed_ will end
    // up cycling through every number in [1,M-1]
    uint64_t product = seed_ * A;

    // Compute (product % M) using the fact that ((x << 31) % M) == x.
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
    // The first reduction may overflow by 1 bit, so we may need to
    // repeat.  mod == M is not possible; using > allows the faster
    // sign-bit-based test.
    if (seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }

  uint64_t Next64() { return (uint64_t{Next()} << 32) | Next(); }

  // Returns a uniformly distributed value in the range [0..n-1]
  // REQUIRES: n > 0
  uint32_t Uniform(int n) { return Next() % n; }

  // Randomly returns true ~"1/n" of the time, and false otherwise.
  // REQUIRES: n > 0
  bool OneIn(int n) { return Uniform(n) == 0; }

  // "Optional" one-in-n, where 0 or negative always returns false
  // (may or may not consume a random value)
  bool OneInOpt(int n) { return n > 0 && OneIn(n); }

  // Returns random bool that is true for the given percentage of
  // calls on average. Zero or less is always false and 100 or more
  // is always true (may or may not consume a random value)
  bool PercentTrue(int percentage) {
    return static_cast<int>(Uniform(100)) < percentage;
  }

  // Skewed: pick "base" uniformly from range [0,max_log] and then
  // return "base" random bits.  The effect is to pick a number in the
  // range [0,2^max_log-1] with exponential bias towards smaller numbers.
  uint32_t Skewed(int max_log) {
    return Uniform(1 << Uniform(max_log + 1));
  }

  // Returns a random string of length "len"
  std::string RandomString(int len);

  // Generates a random string of len bytes using human-readable characters
  std::string HumanReadableString(int len);

  // Generates a random binary data
  std::string RandomBinaryString(int len);

  // Returns a Random instance for use by the current thread without
  // additional locking
  static Random* GetTLSInstance();
};

Random* Random::GetTLSInstance() {
  thread_local Random* tls_instance;
  thread_local std::aligned_storage<sizeof(Random)>::type tls_instance_bytes;

  auto rv = tls_instance;
  if (UNLIKELY(rv == nullptr)) {
    size_t seed = std::hash<std::thread::id>()(std::this_thread::get_id());
    rv = new (&tls_instance_bytes) Random((uint32_t)seed);
    tls_instance = rv;
  }
  return rv;
}

std::string Random::HumanReadableString(int len) {
  std::string ret;
  ret.resize(len);
  for (int i = 0; i < len; ++i) {
    ret[i] = static_cast<char>('a' + Uniform(26));
  }
  return ret;
}

std::string Random::RandomString(int len) {
  std::string ret;
  ret.resize(len);
  for (int i = 0; i < len; i++) {
    ret[i] = static_cast<char>(' ' + Uniform(95));  // ' ' .. '~'
  }
  return ret;
}

std::string Random::RandomBinaryString(int len) {
  std::string ret;
  ret.resize(len);
  for (int i = 0; i < len; i++) {
    ret[i] = static_cast<char>(Uniform(CHAR_MAX));
  }
  return ret;
}

// A good 64-bit random number generator based on std::mt19937_64
class Random64 {
 private:
  std::mt19937_64 generator_;

 public:
  explicit Random64(uint64_t s) : generator_(s) { }

  void Seed(uint64_t s) { generator_.seed(s); }

  // Generates the next random number
  uint64_t Next() { return generator_(); }

  // Returns a uniformly distributed value in the range [0..n-1]
  // REQUIRES: n > 0
  uint64_t Uniform(uint64_t n) {
    return std::uniform_int_distribution<uint64_t>(0, n - 1)(generator_);
  }

  // Randomly returns true ~"1/n" of the time, and false otherwise.
  // REQUIRES: n > 0
  bool OneIn(uint64_t n) { return Uniform(n) == 0; }

  // Skewed: pick "base" uniformly from range [0,max_log] and then
  // return "base" random bits.  The effect is to pick a number in the
  // range [0,2^max_log-1] with exponential bias towards smaller numbers.
  uint64_t Skewed(int max_log) {
    return Uniform(uint64_t(1) << Uniform(max_log + 1));
  }
};

// A seeded replacement for removed std::random_shuffle
template <class RandomIt>
void RandomShuffle(RandomIt first, RandomIt last, uint32_t seed) {
  std::mt19937 rng(seed);
  std::shuffle(first, last, rng);
}

// A replacement for removed std::random_shuffle
template <class RandomIt>
void RandomShuffle(RandomIt first, RandomIt last) {
  RandomShuffle(first, last, std::random_device{}());
}

#endif  // LISTDB_UTIL_RANDOM_H_
