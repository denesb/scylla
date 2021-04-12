/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "test/lib/tmpdir.hh"

#include <seastar/util/alloc_failure_injector.hh>

// This is not really noexcept. But it is used only from the
// destructor and move assignment operators which have to be
// noexcept. This is only for testing, so a std::unexpected call if
// remove fails is fine.
void tmpdir::remove() noexcept {
    memory::disable_failure_guard dfg;
    if (!_path.empty()) {
        fs::remove_all(_path);
    }
}

tmpdir::tmpdir()
    : _path(fs::temp_directory_path() / fs::path(fmt::format(FMT_STRING("scylla-{}"), utils::make_random_uuid()))) {
    fs::create_directories(_path);
}

tmpdir::tmpdir(tmpdir&& other) noexcept : _path(std::exchange(other._path, {})) {}

void tmpdir::operator=(tmpdir&& other) noexcept {
    remove();
    _path = std::exchange(other._path, {});
}

tmpdir::~tmpdir() {
    remove();
}
