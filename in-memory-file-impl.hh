/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/file.hh>
#include "seastarx.hh"

// Create new or open existing in-memory file.
// Returns a file handler and a bool value that will be true
// if a file was created
std::pair<file, bool> get_in_memory_file(sstring name);
future<> init_in_memory_file_store(size_t memory_reserve_in_mb);
future<> deinit_in_memory_file_store();
future<> remove_memory_file(sstring path);
future<> rename_memory_file(sstring oldpath, sstring newpath);
future<> link_memory_file(sstring oldpath, sstring newpath);
