/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "connection_notifier.hh"
#include "db/query_context.hh"
#include "cql3/constants.hh"
#include "database.hh"
#include "service/storage_proxy.hh"

#include <stdexcept>

namespace db::system_keyspace {
extern const char *const CLIENTS;
}

static sstring to_string(client_type ct) {
    switch (ct) {
        case client_type::cql: return "cql";
        case client_type::thrift: return "thrift";
        case client_type::alternator: return "alternator";
        default: throw std::runtime_error("Invalid client_type");
    }
}

future<> notify_new_client(client_data cd) {
    // FIXME: consider prepared statement
    const static sstring req
            = format("INSERT INTO system.{} (address, port, client_type, shard_id, protocol_version, username) "
                     "VALUES (?, ?, ?, ?, ?, ?);", db::system_keyspace::CLIENTS);
    
    return db::execute_cql(req,
            std::move(cd.ip), cd.port, to_string(cd.ct), cd.shard_id,
            cd.protocol_version.has_value() ? data_value(*cd.protocol_version) : data_value::make_null(int32_type),
            cd.username.value_or("anonymous")).discard_result();
}

future<> notify_disconnected_client(gms::inet_address addr, client_type ct, int port) {
    // FIXME: consider prepared statement
    const static sstring req
            = format("DELETE FROM system.{} where address=? AND port=? AND client_type=?;",
                     db::system_keyspace::CLIENTS);
    return db::execute_cql(req, addr.addr(), port, to_string(ct)).discard_result();
}

future<> clear_clientlist() {
    auto& db_local = service::get_storage_proxy().local().get_db().local();
    return db_local.truncate(
            db_local.find_keyspace(db::system_keyspace_name()),
            db_local.find_column_family(db::system_keyspace_name(),
                    db::system_keyspace::CLIENTS),
            [] { return make_ready_future<db_clock::time_point>(db_clock::now()); },
            false /* with_snapshot */);
}
