/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "readers/mutation_reader.hh"

using underlying_reader_factory = noncopyable_function<mutation_reader(schema_ptr schema, reader_permit permit)>;

/// Pass the semaphore's disk admission (reader_permit::wait_disk_admission())
/// before createing and using the underlying reader.
mutation_reader make_restricted_reader(schema_ptr schema, reader_permit permit, underlying_reader_factory reader_factory);
