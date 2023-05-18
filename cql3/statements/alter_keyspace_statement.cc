/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <seastar/core/coroutine.hh>
#include "alter_keyspace_statement.hh"
#include "prepared_statement.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "service/client_state.hh"
#include "db/system_keyspace.hh"
#include "data_dictionary/data_dictionary.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "cql3/query_backend.hh"
#include "cql3/statements/ks_prop_defs.hh"
#include "create_keyspace_statement.hh"
#include "gms/feature_service.hh"
#include "transport/messages/result_message.hh"

bool is_system_keyspace(std::string_view keyspace);

cql3::statements::alter_keyspace_statement::alter_keyspace_statement(sstring name, ::shared_ptr<ks_prop_defs> attrs)
    : _name(name)
    , _attrs(std::move(attrs))
{}

const sstring& cql3::statements::alter_keyspace_statement::keyspace() const {
    return _name;
}

future<> cql3::statements::alter_keyspace_statement::check_access(query_backend& qb, const service::client_state& state) const {
    return state.has_keyspace_access(qb.db(), _name, auth::permission::ALTER);
}

void cql3::statements::alter_keyspace_statement::validate(query_backend& qb, const service::client_state& state) const {
        auto tmp = _name;
        std::transform(tmp.begin(), tmp.end(), tmp.begin(), ::tolower);
        if (is_system_keyspace(tmp)) {
            throw exceptions::invalid_request_exception("Cannot alter system keyspace");
        }

        _attrs->validate();

        if (!bool(_attrs->get_replication_strategy_class()) && !_attrs->get_replication_options().empty()) {
            throw exceptions::configuration_exception("Missing replication strategy class");
        }
        try {
            data_dictionary::storage_options current_options = qb.db().find_keyspace(_name).metadata()->get_storage_options();
            data_dictionary::storage_options new_options = _attrs->get_storage_options();
            if (!current_options.can_update_to(new_options)) {
                throw exceptions::invalid_request_exception(format("Cannot alter storage options: {} to {} is not supported",
                        current_options.type_string(), new_options.type_string()));
            }
        } catch (const std::runtime_error& e) {
            throw exceptions::invalid_request_exception(e.what());
        }
        if (!qb.proxy().features().keyspace_storage_options
            && _attrs->get_storage_options().type_string() != "LOCAL") {
        throw exceptions::invalid_request_exception("Keyspace storage options not supported in the cluster");
    }
#if 0
        // The strategy is validated through KSMetaData.validate() in announceKeyspaceUpdate below.
        // However, for backward compatibility with thrift, this doesn't validate unexpected options yet,
        // so doing proper validation here.
        AbstractReplicationStrategy.validateReplicationStrategy(name,
                                                                AbstractReplicationStrategy.getClass(attrs.getReplicationStrategyClass()),
                                                                StorageService.instance.getTokenMetadata(),
                                                                DatabaseDescriptor.getEndpointSnitch(),
                                                                attrs.getReplicationOptions());
#endif
}

future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>>
cql3::statements::alter_keyspace_statement::prepare_schema_mutations(query_backend& qb, api::timestamp_type ts) const {
    try {
        auto old_ksm = qb.db().find_keyspace(_name).metadata();
        const auto& tm = *qb.proxy().get_token_metadata_ptr();

        auto m = qb.get_migration_manager().prepare_keyspace_update_announcement(_attrs->as_ks_metadata_update(old_ksm, tm), ts);

        using namespace cql_transport;
        auto ret = ::make_shared<event::schema_change>(
                event::schema_change::change_type::UPDATED,
                event::schema_change::target_type::KEYSPACE,
                keyspace());

        return make_ready_future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>>(std::make_pair(std::move(ret), std::move(m)));
    } catch (data_dictionary::no_such_keyspace& e) {
        return make_exception_future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>>(exceptions::invalid_request_exception("Unknown keyspace " + _name));
    }
}

std::unique_ptr<cql3::statements::prepared_statement>
cql3::statements::alter_keyspace_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<alter_keyspace_statement>(*this));
}

static logging::logger mylogger("alter_keyspace");

future<::shared_ptr<cql_transport::messages::result_message>>
cql3::statements::alter_keyspace_statement::execute(query_backend& qb, service::query_state& state, const query_options& options) const {
    std::optional<sstring> warning = check_restricted_replication_strategy(qb, keyspace(), *_attrs);
    return schema_altering_statement::execute(qb, state, options).then([warning = std::move(warning)] (::shared_ptr<messages::result_message> msg) {
        if (warning) {
            msg->add_warning(*warning);
            mylogger.warn("{}", *warning);
        }
        return msg;
    });
}
