#include "wal.hpp"

#include <fcntl.h>
#include <unistd.h>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/stream.hpp>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace DB {

WAL::WAL(const std::string &filename)
    : filename_(filename), fd_out_(-1), out_(nullptr) {
    std::filesystem::create_directories(
        std::filesystem::path(filename_).parent_path()
    );

    fd_out_ = ::open(filename_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_out_ >= 0) {
        out_ = std::make_unique<boost_file_sink>(
            fd_out_, boost::iostreams::never_close_handle
        );
    }
}

WAL::~WAL() {
    if (fd_out_ >= 0) {
        ::close(fd_out_);
    }
}

void WAL::logInsert(const std::string &key, const std::string &value) {
    std::lock_guard<userver::engine::Mutex> lock(walMutex_);

    if (out_) {
        *out_ << "INSERT " << std::quoted(key) << ' ' << std::quoted(value)
              << '\n';
        out_->flush();
    }
}

void WAL::logRemove(const std::string &key) {
    std::lock_guard<userver::engine::Mutex> lock(walMutex_);

    if (out_) {
        *out_ << "REMOVE " << std::quoted(key) << '\n';
        out_->flush();
    }
}

void WAL::recover(
    std::function<void(const std::string &, const std::string &, bool)>
        applyOperation
) {
    int fd = ::open(filename_.c_str(), O_RDONLY);
    if (fd < 0) {
        return;
    }

    boost::iostreams::stream<boost::iostreams::file_descriptor_source> in(
        fd, boost::iostreams::close_handle
    );

    std::string line;
    while (std::getline(in, line)) {
        std::istringstream iss(line);
        std::string op, key, value;
        iss >> op;
        if (op == "INSERT") {
            iss >> std::quoted(key) >> std::quoted(value);
            applyOperation(key, value, false);
        } else if (op == "REMOVE") {
            iss >> std::quoted(key);
            applyOperation(key, "", true);
        }
    }
}

void WAL::clear() {
    std::lock_guard<userver::engine::Mutex> lock(walMutex_);

    if (fd_out_ >= 0) {
        ::close(fd_out_);
    }

    int fd_clear =
        ::open(filename_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd_clear >= 0) {
        ::close(fd_clear);
    }

    fd_out_ = ::open(filename_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_out_ >= 0) {
        out_.reset(
            new boost_file_sink(fd_out_, boost::iostreams::never_close_handle)
        );
    }
}

}  // namespace DB