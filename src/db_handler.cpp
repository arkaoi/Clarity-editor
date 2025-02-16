#include "db_handler.hpp"
#include <stdexcept>

namespace userver_demo {

userver::formats::json::Value DatabaseHandler::HandleRequestJsonThrow(
    const userver::server::http::HttpRequest& request,
    const userver::formats::json::Value& request_json,
    userver::server::request::RequestContext& request_context) const {
  
  std::string key = request_json["key"].As<std::string>("default_key");
  std::string value = request_json["value"].As<std::string>("default_value");

  
  db_.insert(key, value);
  db_.saveToFile();
  
  userver::formats::json::ValueBuilder response;
  response["status"] = "OK";
  response["inserted_key"] = key;
  response["inserted_value"] = value;

  return response.ExtractValue();
}

}  // namespace userver_demo
