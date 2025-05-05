#ifndef BLOOM_FILTER_HPP_
#define BLOOM_FILTER_HPP_

#include <cstdint>
#include <functional>
#include <istream>
#include <ostream>
#include <string>
#include <vector>

namespace DB {

class BloomFilter {
public:
    BloomFilter(size_t size = 1 << 20, size_t hashes = 7);

    void add(const std::string &key);
    bool possiblyContains(const std::string &key) const;

    void serialize(std::ostream &os) const;
    void deserialize(std::istream &is);

private:
    size_t bitSize;
    size_t numHashes;
    std::vector<bool> bits;

    uint64_t hash1(const std::string &key) const;
    uint64_t hash2(const std::string &key) const;
    uint64_t nthHash(uint64_t h1, uint64_t h2, size_t i) const;
};

}  // namespace DB

#endif  // BLOOM_FILTER_HPP_