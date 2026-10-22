/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "db/cluster_config_manager.hh"
#include "seastarx.hh"

namespace replica {

class database;

// Applies the problem_reads_* cluster config options to the local shard: the table-oriented
// ones to each table's problem_read_thresholds, the node-oriented ones to the shard's
// problem_reads. Start it on every shard, after the cluster config manager, and stop it before
// the manager.
class problem_reads_config {
    std::vector<db::cluster_config_manager::config_callback_registration> _registrations;

public:
    problem_reads_config(sharded<database>& db, sharded<db::cluster_config_manager>& mgr);

    future<> stop();
};

}
