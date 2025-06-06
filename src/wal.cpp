#include "wal.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/stream.hpp>
#include <filesystem>
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

void WAL::logInsert(
    const std::string &key,
    const std::vector<uint8_t> &valueBlob
) {
    std::lock_guard<userver::engine::Mutex> lock(walMutex_);

    if (out_) {
        uint8_t op = 1;
        uint32_t keySize = static_cast<uint32_t>(key.size());
        uint32_t valueSize = static_cast<uint32_t>(valueBlob.size());

        out_->write(reinterpret_cast<const char *>(&op), sizeof(op));

        out_->write(reinterpret_cast<const char *>(&keySize), sizeof(keySize));
        out_->write(key.data(), keySize);

        out_->write(
            reinterpret_cast<const char *>(&valueSize), sizeof(valueSize)
        );
        if (valueSize > 0) {
            out_->write(
                reinterpret_cast<const char *>(valueBlob.data()), valueSize
            );
        }
        out_->flush();
    }
}

void WAL::logRemove(const std::string &key) {
    std::lock_guard<userver::engine::Mutex> lock(walMutex_);

    if (out_) {
        uint8_t op = 2;
        uint32_t keySize = static_cast<uint32_t>(key.size());
        out_->write(reinterpret_cast<const char *>(&op), sizeof(op));
        out_->write(reinterpret_cast<const char *>(&keySize), sizeof(keySize));
        out_->write(key.data(), keySize);
        out_->flush();
    }
}

void WAL::recover(
    std::function<void(const std::string &, const std::vector<uint8_t> &, bool)>
        applyOperation
) {
    int fd = ::open(filename_.c_str(), O_RDONLY);
    if (fd < 0) {
        return;
    }

    boost::iostreams::stream<boost::iostreams::file_descriptor_source> in(
        fd, boost::iostreams::close_handle
    );

    while (!in.eof()) {
        uint8_t opType = 0;
        in.read(reinterpret_cast<char *>(&opType), sizeof(opType));
        if (!in || in.gcount() != sizeof(opType)) {
            break;
        }

        uint32_t keySize = 0;
        in.read(reinterpret_cast<char *>(&keySize), sizeof(keySize));
        if (!in || in.gcount() != sizeof(keySize)) {
            break;
        }
        std::string key(keySize, '\0');
        in.read(&key[0], keySize);
        if (!in || in.gcount() != static_cast<std::streamsize>(keySize)) {
            break;
        }

        if (opType == 1) {
            uint32_t valueSize = 0;
            in.read(reinterpret_cast<char *>(&valueSize), sizeof(valueSize));
            if (!in || in.gcount() != sizeof(valueSize)) {
                break;
            }
            std::vector<uint8_t> blob(valueSize);
            if (valueSize > 0) {
                in.read(reinterpret_cast<char *>(blob.data()), valueSize);
                if (!in ||
                    in.gcount() != static_cast<std::streamsize>(valueSize)) {
                    break;
                }
            }
            applyOperation(key, blob, false);
        } else if (opType == 2) {
            std::vector<uint8_t> emptyBlob;
            applyOperation(key, emptyBlob, true);
        } else {
            break;
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
