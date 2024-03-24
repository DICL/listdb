#ifndef LISTDB_BLOOM_FILTER_H_
#define LISTDB_BLOOM_FILTER_H_

#include <string>

// The FALLTHROUGH_INTENDED macro can be used to annotate implicit fall-through
// between switch labels. The real definition should be provided externally.
// This one is a fallback version for unsupported compilers.
#ifndef FALLTHROUGH_INTENDED
#define FALLTHROUGH_INTENDED \
  do {                       \
  } while (0)
#endif

class BloomFilter {
private:
    std::string* bitArray_;
    size_t bits_;
    size_t k_;
    

    uint32_t BloomHash(const Key& key) {
      const char* data = key.data();
      size_t n = key.size();
      uint32_t seed = 0xbc9f1d34;

      // Similar to murmur hash
      const uint32_t m = 0xc6a4a793;
      const uint32_t r = 24;
      const char* limit = data + n;
      uint32_t h = seed ^ (n * m);

      // Pick up four bytes at a time
      while (data + 4 <= limit) {
        uint32_t w = DecodeFixed32(data);
        data += 4;
        h += w;
        h *= m;
        h ^= (h >> 16);
      }

      // Pick up remaining bytes
      switch (limit - data) {
        case 3:
          h += static_cast<uint8_t>(data[2]) << 16;
          FALLTHROUGH_INTENDED;
        case 2:
          h += static_cast<uint8_t>(data[1]) << 8;
          FALLTHROUGH_INTENDED;
        case 1:
          h += static_cast<uint8_t>(data[0]);
          h *= m;
          h ^= (h >> r);
          break;
      }
      return h;
    }

    inline uint32_t DecodeFixed32(const char* ptr) {
      const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);

      // Recent clang and gcc optimize this to a single mov / ldr instruction.
      return (static_cast<uint32_t>(buffer[0])) |
            (static_cast<uint32_t>(buffer[1]) << 8) |
            (static_cast<uint32_t>(buffer[2]) << 16) |
            (static_cast<uint32_t>(buffer[3]) << 24);
    }

public:
    explicit BloomFilter(int bits_per_key, uint64_t max_key_num) : bits_(max_key_num*bits_per_key) {
        // We intentionally round down to reduce probing cost a little bit
        k_ = static_cast<size_t>(bits_per_key * 0.69);  // 0.69 =~ ln(2)
        if (k_ < 1) k_ = 1;
        if (k_ > 30) k_ = 30;

        // For small n, we can see a very high false positive rate.  Fix it
        // by enforcing a minimum bloom filter length.
        if (bits_ < 64) bits_ = 64;

        size_t bytes = (bits_ + 7) / 8;
        bits_ = bytes * 8;

        bitArray_ = new std::string(bytes, 0);
    }

    BloomFilter& operator=(const BloomFilter& other) {
        if (this != &other) {
            delete bitArray_;
            bits_ = other.bits_;
            k_ = other.k_;
            bitArray_ = new std::string(*other.bitArray_);
        }
        return *this;
    }

    ~BloomFilter() {
      delete bitArray_;
    }

    void AddKey(const Key& key) {
        char* array = bitArray_->data();
        uint32_t h = BloomHash(key);
        const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
        for (size_t j = 0; j < k_; j++) {
            const uint32_t bitpos = h % bits_;
            array[bitpos / 8] |= (1 << (bitpos % 8));
            h += delta;
        }
    }

    bool KeyMayMatch(const Key& key) {
        const char* array = bitArray_->data();
        uint32_t h = BloomHash(key);
        const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
        for (size_t j = 0; j < k_; j++) {
          const uint32_t bitpos = h % bits_;
          if ((array[bitpos / 8] & (1 << (bitpos % 8))) == 0) return false;
          h += delta;
        }
        return true;
    }
};

#endif  // LISTDB_BLOOM_FILTER_H_