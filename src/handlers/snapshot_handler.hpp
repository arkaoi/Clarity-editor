#pragma once
#include "database.hpp"
#include "db_config.hpp"
#include <fstream>
#include <iterator>
#include <string>
#include <userver/server/handlers/http_handler_json_base.hpp>

namespace userver_db {

class SnapshotHandler final
    : public userver::server::handlers::HttpHandlerJsonBase {
public:
    static constexpr std::string_view kName = "handler-snapshot";
    using HttpHandlerJsonBase::HttpHandlerJsonBase;

    userver::formats::json::Value HandleRequestJsonThrow(
        const userver::server::http::HttpRequest &request,
        const userver::formats::json::Value &request_json,
        userver::server::request::RequestContext &) const override {
        db_.recoverFromWAL();
        const std::string csv_path =
            std::string(DBConfig::kDirectory) + "/snapshot.csv";
        db_.SnapshotCsv(csv_path);
        std::ifstream in(csv_path);
        if (!in.is_open())
            throw std::runtime_error("Cannot open snapshot file");
        std::string csv{std::istreambuf_iterator<char>(in), {}};
        userver::formats::json::ValueBuilder resp;
        resp["snapshot_csv"] = csv;
        return resp.ExtractValue();
    }

private:
    mutable DB::Database db_{DBConfig::kDirectory, DBConfig::kMemtableLimit,
                             DBConfig::kSstableLimit};
};

} // namespace userver_db