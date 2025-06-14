#include "bloom.hpp"
#include <cmath>

namespace DB {

BloomFilter::BloomFilter(size_t size, size_t hashes)
    : bitSize(size), numHashes(hashes), bits(size, false) {}

uint64_t BloomFilter::hash1(const std::string &key) const {
    return std::hash<std::string>{}(key);
}

uint64_t BloomFilter::hash2(const std::string &key) const {
    return std::hash<std::string>{}(key + "#bloom");
}

uint64_t BloomFilter::nthHash(uint64_t h1, uint64_t h2, size_t i) const {
    return h1 + i * h2 + i * i;
}

void BloomFilter::add(const std::string &key) {
    auto h1 = hash1(key);
    auto h2 = hash2(key);
    for (size_t i = 0; i < numHashes; ++i) {
        auto combined = nthHash(h1, h2, i);
        bits[combined % bitSize] = true;
    }
}

bool BloomFilter::possiblyContains(const std::string &key) const {
    auto h1 = hash1(key);
    auto h2 = hash2(key);
    for (size_t i = 0; i < numHashes; ++i) {
        auto combined = nthHash(h1, h2, i);
        if (!bits[combined % bitSize]) {
            return false;
        }
    }
    return true;
}

void BloomFilter::serialize(std::ostream &os) const {
    os << bitSize << ' ' << numHashes << ' ';
    for (bool b : bits) {
        os << (b ? '1' : '0');
    }
    os << '\n';
}

void BloomFilter::deserialize(std::istream &is) {
    size_t size, hashes;
    is >> size >> hashes;
    bitSize = size;
    numHashes = hashes;
    bits.assign(bitSize, false);
    if (is.peek() == ' ') {
        is.get();
    }

    for (size_t i = 0; i < bitSize; ++i) {
        char c = '\0';
        is.get(c);
        bits[i] = (c == '1');
    }

    if (is.peek() == '\n') {
        is.get();
    }
}

} // namespace DB