/*
 * Copyright (C) 2017 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "table_helper.hh"
#include "cql3/statements/create_table_statement.hh"
#include "cql3/query_processor.hh"

future<> table_helper::setup_table() const {
    auto& qp = cql3::get_local_query_processor();
    auto& db = qp.db();

    if (db.has_schema(_keyspace, _name)) {
        return make_ready_future<>();
    }

    ::shared_ptr<cql3::statements::raw::cf_statement> parsed = static_pointer_cast<
                    cql3::statements::raw::cf_statement>(cql3::query_processor::parse_statement(_create_cql));
    parsed->prepare_keyspace(_keyspace);
    ::shared_ptr<cql3::statements::create_table_statement> statement =
                    static_pointer_cast<cql3::statements::create_table_statement>(
                                    parsed->prepare(db, qp.get_cql_stats())->statement);
    auto schema = statement->get_cf_meta_data(db);

    // Generate the CF UUID based on its KF names. This is needed to ensure that
    // all Nodes that create it would create it with the same UUID and we don't
    // hit the #420 issue.
    auto uuid = generate_legacy_id(schema->ks_name(), schema->cf_name());

    schema_builder b(schema);
    b.set_uuid(uuid);

    // We don't care it it fails really - this may happen due to concurrent
    // "CREATE TABLE" invocation on different Nodes.
    // The important thing is that it will converge eventually (some traces may
    // be lost in a process but that's ok).
    return service::get_local_migration_manager().announce_new_column_family(b.build(), false).discard_result().handle_exception([this] (auto ep) {});;
}

future<> table_helper::cache_table_info(service::query_state& qs) {
    if (_prepared_stmt) {
        return now();
    } else {
        // if prepared statement has been invalidated - drop cached pointers
        _insert_stmt = nullptr;
    }

    return cql3::get_local_query_processor().prepare(_insert_cql, qs.get_client_state(), false).then([this] (shared_ptr<cql_transport::messages::result_message::prepared> msg_ptr) {
        _prepared_stmt = std::move(msg_ptr->get_prepared());
        shared_ptr<cql3::cql_statement> cql_stmt = _prepared_stmt->statement;
        _insert_stmt = dynamic_pointer_cast<cql3::statements::modification_statement>(cql_stmt);
    }).handle_exception([this] (auto eptr) {
        // One of the possible causes for an error here could be the table that doesn't exist.
        this->setup_table().discard_result();

        // We throw the bad_column_family exception because the caller
        // expects and accounts this type of errors.
        try {
            std::rethrow_exception(eptr);
        } catch (std::exception& e) {
            throw bad_column_family(_keyspace, _name, e);
        } catch (...) {
            throw bad_column_family(_keyspace, _name);
        }
    });
}
