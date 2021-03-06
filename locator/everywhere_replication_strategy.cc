/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */


#include "locator/everywhere_replication_strategy.hh"
#include "utils/class_registrator.hh"
#include "utils/fb_utilities.hh"
#include "locator/token_metadata.hh"

namespace locator {

everywhere_replication_strategy::everywhere_replication_strategy(const sstring& keyspace_name, const token_metadata& token_metadata, snitch_ptr& snitch, const std::map<sstring, sstring>& config_options) :
        abstract_replication_strategy(keyspace_name, token_metadata, snitch, config_options, replication_strategy_type::everywhere_topology) {}

std::vector<inet_address> everywhere_replication_strategy::calculate_natural_endpoints(const token& search_token, const token_metadata& tm) const {
    return tm.get_all_endpoints();
}

std::vector<inet_address> everywhere_replication_strategy::get_natural_endpoints(const token& search_token) {
    if (_token_metadata.sorted_tokens().empty()) {
        return std::vector<inet_address>({utils::fb_utilities::get_broadcast_address()});
    }
    return calculate_natural_endpoints(search_token, _token_metadata);
}

size_t everywhere_replication_strategy::get_replication_factor() const {
    return _token_metadata.get_all_endpoints_count();
}

using registry = class_registrator<abstract_replication_strategy, everywhere_replication_strategy, const sstring&, const token_metadata&, snitch_ptr&, const std::map<sstring, sstring>&>;
static registry registrator("org.apache.cassandra.locator.EverywhereStrategy");
static registry registrator_short_name("EverywhereStrategy");
}
