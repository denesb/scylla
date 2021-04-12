/*
 * Copyright (C) 2021 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "feed_writers.hh"

namespace mutation_writer {

bucket_writer::bucket_writer(schema_ptr schema, std::pair<flat_mutation_reader, queue_reader_handle> queue_reader, reader_consumer& consumer)
    : _schema(schema)
    , _handle(std::move(queue_reader.second))
    , _consume_fut(consumer(std::move(queue_reader.first)))
{ }

bucket_writer::bucket_writer(schema_ptr schema, reader_permit permit, reader_consumer& consumer)
    : bucket_writer(schema, make_queue_reader(schema, std::move(permit)), consumer)
{ }

future<> bucket_writer::consume(mutation_fragment mf) {
    return _handle.push(std::move(mf));
}

void bucket_writer::consume_end_of_stream() {
    _handle.push_end_of_stream();
}

void bucket_writer::abort(std::exception_ptr ep) noexcept {
    _handle.abort(std::move(ep));
}

future<> bucket_writer::close() noexcept {
    return std::move(_consume_fut);
}

} // mutation_writer
