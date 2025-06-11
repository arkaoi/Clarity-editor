#include <userver/clients/dns/component.hpp>
#include <userver/clients/http/component.hpp>
#include <userver/components/minimal_server_component_list.hpp>
#include <userver/dynamic_config/client/component.hpp>
#include <userver/dynamic_config/updater/component.hpp>
#include <userver/server/handlers/ping.hpp>
#include <userver/server/handlers/tests_control.hpp>
#include <userver/testsuite/testsuite_support.hpp>
#include <userver/utils/daemon_run.hpp>
#include "db_handler.hpp"

int main(int argc, char *argv[]) {
    auto component_list =
        userver::components::MinimalServerComponentList()
            .Append<userver::components::DynamicConfigClient>()
            .Append<userver::components::DynamicConfigClientUpdater>()
            .Append<userver::server::handlers::Ping>()
            .Append<userver::components::HttpClient>()
            .Append<userver::clients::dns::Component>()
            .Append<userver::components::TestsuiteSupport>()
            .Append<userver::server::handlers::TestsControl>();

    component_list.Append<userver_db::DatabaseHandler>();

    return userver::utils::DaemonMain(argc, argv, component_list);
}
