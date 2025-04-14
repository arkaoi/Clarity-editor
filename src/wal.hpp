#ifndef WAL_HPP_
#define WAL_HPP_

#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/stream.hpp>
#include <functional>
#include <string>
#include <userver/engine/mutex.hpp>

namespace DB {

class WAL {
public:
    WAL(const std::string &filename);
    ~WAL();

    void logInsert(const std::string &key, const std::string &value);
    void logRemove(const std::string &key);

    void recover(
        std::function<void(const std::string &, const std::string &, bool)>
            applyOperation
    );

    void clear();

private:
    std::string filename_;
    int fd_out_;
    userver::engine::Mutex walMutex_;
};

}  // namespace DB

#endif  // WAL_HPP_
