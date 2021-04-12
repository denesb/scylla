/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/semaphore.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>

using namespace seastar;

namespace cql_transport { class cql_server; }
class database;
namespace auth { class service; }
namespace service { class migration_notifier; }
namespace gms { class gossiper; }
namespace qos { class service_level_controller; }

namespace cql_transport {

class controller {
    std::unique_ptr<distributed<cql_transport::cql_server>> _server;
    semaphore _ops_sem; /* protects start/stop operations on _server */
    bool _stopped = false;

    distributed<database>& _db;
    sharded<auth::service>& _auth_service;
    sharded<service::migration_notifier>& _mnotifier;
    gms::gossiper& _gossiper;
    sharded<qos::service_level_controller>& _sl_controller;

    future<> set_cql_ready(bool ready);
    future<> do_start_server();
    future<> do_stop_server();

public:
    controller(distributed<database>&, sharded<auth::service>&, sharded<service::migration_notifier>&, gms::gossiper&, sharded<qos::service_level_controller>&);
    future<> start_server();
    future<> stop_server();
    future<> stop();
    future<bool> is_server_running();
};

} // namespace cql_transport
