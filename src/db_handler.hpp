#pragma once

#include <string_view>
#include <userver/formats/json/value.hpp>
#include <userver/formats/json/value_builder.hpp>
#include <userver/server/handlers/http_handler_json_base.hpp>
#include <userver/server/http/http_request.hpp>
#include <userver/server/request/request_context.hpp>

#include "database.hpp"
#include "db_config.hpp"

namespace userver_db {

class DatabaseHandler final
    : public userver::server::handlers::HttpHandlerJsonBase {
public:
  static constexpr std::string_view kName = "handler-database";

  using HttpHandlerJsonBase::HttpHandlerJsonBase;

  userver::formats::json::Value HandleRequestJsonThrow(
      const userver::server::http::HttpRequest &request,
      const userver::formats::json::Value &request_json,
      userver::server::request::RequestContext &request_context) const override;

private:
  mutable DB::Database db_{DBConfig::kDirectory, DBConfig::kMemtableLimit,
                           DBConfig::kSstableLimit};
};

} // namespace userver_db
