/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "timestamp.hh"
#include "locator/tablets.hh"
#include "schema/schema_fwd.hh"
#include "mutation/mutation.hh"
#include "mutation/canonical_mutation.hh"
#include "replica/database_fwd.hh"

#include <seastar/core/future.hh>

#include <vector>


namespace cql3 {

class query_processor;

}

namespace replica {

schema_ptr make_tablets_schema();

/// Converts information in tablet_map to mutations of system.tablets.
///
/// The mutations will delete any older tablet information for the same table.
/// The provided timestamp should be strictly monotonically increasing
/// between calls for the overriding to work correctly.
future<mutation> tablet_map_to_mutation(const locator::tablet_map&,
                                        table_id,
                                        const sstring& keyspace_name,
                                        const sstring& table_name,
                                        api::timestamp_type);

mutation make_drop_tablet_map_mutation(const sstring& keyspace_name, table_id, api::timestamp_type);

/// Stores a given tablet_metadata in system.tablets.
///
/// Overrides tablet maps for tables present in the given tablet metadata.
/// Does not delete tablet maps for tables which are absent in the given tablet metadata.
/// The provided timestamp should be strictly monotonically increasing
/// between calls for tablet map overriding to work correctly.
/// The timestamp must be greater than api::min_timestamp.
future<> save_tablet_metadata(replica::database&, const locator::tablet_metadata&, api::timestamp_type);

/// Extract a tablet metadata change hint from the tablet mutations.
///
/// Mutations which don't mutate the tablet table are ignored.
locator::tablet_metadata_change_hint get_tablet_metadata_change_hint(const std::vector<canonical_mutation>&);

/// Update the tablet metadata change hint, with the changes represented by the tablet mutation.
///
/// If the mutation belongs to another table, no updates are done.
void update_tablet_metadata_change_hint(locator::tablet_metadata_change_hint&, const mutation&);

/// Reads tablet metadata from system.tablets.
future<locator::tablet_metadata> read_tablet_metadata(cql3::query_processor&);

/// Update tablet metadata from system.tablets, based on the provided hint.
///
/// The hint is used to determine what has changed and only reload the changed
/// parts from disk, updating the passed-in metadata in-place accordingly.
future<> update_tablet_metadata(cql3::query_processor&, locator::tablet_metadata&, const locator::tablet_metadata_change_hint&);

/// Reads tablet metadata from system.tablets in the form of mutations.
future<std::vector<canonical_mutation>> read_tablet_mutations(seastar::sharded<database>&);

} // namespace replica
