/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <seastarx.hh>
#include "cql3/statements/list_service_level_statement.hh"
#include "service/qos/service_level_controller.hh"

namespace cql3 {

namespace statements {

list_service_level_statement::list_service_level_statement(sstring service_level, bool describe_all) :
    _service_level(service_level), _describe_all(describe_all) {
}

void list_service_level_statement::validate(service::storage_proxy &, const service::client_state &) {
}

future<> list_service_level_statement::check_access(const service::client_state &state) {
    return state.ensure_has_permission(auth::permission::DESCRIBE, auth::root_service_level_resource());
}

future<::shared_ptr<cql_transport::messages::result_message>>
list_service_level_statement::execute(service::storage_proxy &sp,
        service::query_state &state,
        const query_options &) {

    static auto make_column = [] (sstring name, const shared_ptr<const abstract_type> type) {
        return ::make_shared<column_specification>(
                "QOS",
                "service_levels",
                ::make_shared<column_identifier>(std::move(name), true),
                type);
    };

    static thread_local const std::vector<::shared_ptr<column_specification>> metadata({make_column("service_level", utf8_type), make_column("shares", int32_type)});

    return make_ready_future().then([this, &state] () {
                                  if (_describe_all) {
                                      return state.get_service_level_controller().get_distributed_service_levels();
                                  } else {
                                      return state.get_service_level_controller().get_distributed_service_level(_service_level);
                                  }
                              })
            .then([this] (qos::service_levels_info sl_info) {
                auto rs = std::make_unique<result_set>(metadata);
                for (auto &&sl : sl_info) {
                    rs->add_row(std::vector<bytes_opt>{
                            utf8_type->decompose(sl.first),
                            int32_type->decompose(sl.second.shares)});
                }

                auto rows = ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(std::move(rs))));
                return ::static_pointer_cast<cql_transport::messages::result_message>(rows);
            });
}
}
}
