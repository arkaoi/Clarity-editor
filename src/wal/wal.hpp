#ifndef WAL_HPP_
#define WAL_HPP_

#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/stream.hpp>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>
#include <userver/engine/mutex.hpp>
#include <vector>

namespace DB {
using boost_file_sink =
    boost::iostreams::stream<boost::iostreams::file_descriptor_sink>;

class WAL {
public:
    WAL(const std::string &filename);
    ~WAL();

    void
    logInsert(const std::string &key, const std::vector<uint8_t> &valueBlob);
    void logRemove(const std::string &key);

    void recover(std::function<
                 void(const std::string &, const std::vector<uint8_t> &, bool)>
                     applyOperation);

    void clear();

private:
    std::string filename_;
    int fd_out_;
    userver::engine::Mutex walMutex_;
    std::unique_ptr<boost_file_sink> out_;
};

}  // namespace DB

#endif  // WAL_HPP_
