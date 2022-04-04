#ifndef LISTDB_LIB_HASH_H_
#define LISTDB_LIB_HASH_H_

#include "listdb/common.h"
#include "listdb/lib/murmur3.h"
#include "listdb/lib/sha1.h"

//constexpr uint64_t kHTMask = 0x07ffffff;

inline uint32_t ht_murmur3(const Key& key) {
	uint32_t h;
	static const uint32_t seed = 0xcafeb0ba;
#ifndef LISTDB_STRING_KEY
	MurmurHash3_x86_32(&key, sizeof(uint64_t), seed, (void*) &h);
#else
	MurmurHash3_x86_32(key.data(), kStringKeyLength, seed, (void*) &h);
#endif
	//return h & kHTMask;
	return h % kHTSize;
}

inline uint32_t ht_sha1(const Key& key) {
	char result[21];  // 5 * 32bit
#ifndef LISTDB_STRING_KEY
	SHA1(result, (char*) &key, 8);
#else
	SHA1(result, key.data(), kStringKeyLength);
#endif
	//return *reinterpret_cast<uint32_t*>(result) & kHTMask;
	return *reinterpret_cast<uint32_t*>(result) % kHTSize;
}

#endif  // LISTDB_LIB_HASH_H_
