#include "db_handler.hpp"
#include <userver/formats/json/serialize.hpp>
#include <userver/server/handlers/exceptions.hpp>

using userver::formats::json::FromString;
using userver::formats::json::ToString;

namespace userver_db {

namespace {
struct error_builder {
    static constexpr bool kIsExternalBodyFormatted = true;
    std::string message;

    std::string GetExternalBody() const {
        return message;
    }
};
}  // namespace

userver::formats::json::Value DatabaseHandler::
    HandleRequestJsonThrow(const userver::server::http::HttpRequest &request, const userver::formats::json::Value &request_json, userver::server::request::RequestContext &)
        const {
    const auto method = request.GetMethod();

    std::string url = request.GetUrl();
    const std::string prefix = "/database/";
    std::string key;
    if (url.rfind(prefix, 0) == 0) {
        key = url.substr(prefix.size());
    }

    if (key.empty()) {
        throw userver::server::handlers::ClientError(error_builder{
            "Key not provided"});
    }

    userver::formats::json::ValueBuilder response;
    if (method == userver::server::http::HttpMethod::kGet) {
        const auto opt_blob = db_.select(key);
        if (!opt_blob) {
            throw userver::server::handlers::ResourceNotFound(error_builder{
                "Key not found"});
        }

        std::string serialized(opt_blob->begin(), opt_blob->end());
        auto json_value = FromString(serialized);

        response["key"] = key;
        response["value"] = json_value;
        return response.ExtractValue();
    }

    else if (method == userver::server::http::HttpMethod::kPut) {
        auto json_value = request_json["value"];
        std::string serialized = ToString(json_value);

        std::vector<uint8_t> blob(serialized.begin(), serialized.end());
        db_.insert(key, blob);

        response["updated_key"] = key;
        response["updated_value"] = json_value;
        return response.ExtractValue();
    }

    else if (method == userver::server::http::HttpMethod::kDelete) {
        bool deleted = db_.remove(key);
        if (!deleted) {
            throw userver::server::handlers::ResourceNotFound(error_builder{
                "Key not found"});
        }
        userver::formats::json::ValueBuilder response;
        response["deleted_key"] = key;
        return response.ExtractValue();
    } else {
        throw userver::server::handlers::ClientError(error_builder{
            "Unsupported HTTP method"});
    }
}

}  // namespace userver_db
