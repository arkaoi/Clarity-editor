#pragma once

#include <string_view>
#include <userver/server/handlers/http_handler_json_base.hpp>
#include <userver/formats/json/value.hpp>
#include <userver/formats/json/value_builder.hpp>
#include <userver/server/http/http_request.hpp>
#include <userver/server/request/request_context.hpp>

#include "db_base.hpp"

namespace userver_demo {


class DatabaseHandler final : public userver::server::handlers::HttpHandlerJsonBase {
 public:
  static constexpr std::string_view kName = "handler-database";

  using HttpHandlerJsonBase::HttpHandlerJsonBase;

  userver::formats::json::Value HandleRequestJsonThrow(
      const userver::server::http::HttpRequest& request,
      const userver::formats::json::Value& request_json,
      userver::server::request::RequestContext& request_context) const override;

 private:
  mutable DB::Database db_{"database.txt"};
};

}  // namespace userver_demo
