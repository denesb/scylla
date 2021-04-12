/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <seastar/core/future.hh>

namespace cql3 {

namespace statements {

	future<> set_internal_paging_size(int internal_paging_size);
	future<> reset_internal_paging_size();

}

}
