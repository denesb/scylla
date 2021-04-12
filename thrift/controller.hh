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

class thrift_server;
class database;
namespace auth { class service; }

class thrift_controller {
    std::unique_ptr<distributed<thrift_server>> _server;
    semaphore _ops_sem; /* protects start/stop operations on _server */
    bool _stopped = false;

    distributed<database>& _db;
    sharded<auth::service>& _auth_service;

    future<> do_start_server();
    future<> do_stop_server();

public:
    thrift_controller(distributed<database>&, sharded<auth::service>&);
    future<> start_server();
    future<> stop_server();
    future<> stop();
    future<bool> is_server_running();
};
