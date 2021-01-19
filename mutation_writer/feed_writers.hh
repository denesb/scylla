/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "flat_mutation_reader.hh"

namespace mutation_writer {
using reader_consumer = noncopyable_function<future<> (flat_mutation_reader)>;

template <typename Writer>
requires MutationFragmentConsumer<Writer, future<>>
future<> feed_writer(flat_mutation_reader&& rd, Writer&& wr) {
    return do_with(std::move(rd), std::move(wr), [] (flat_mutation_reader& rd, Writer& wr) {
        return rd.fill_buffer(db::no_timeout).then([&rd, &wr] {
            return do_until([&rd] { return rd.is_buffer_empty() && rd.is_end_of_stream(); }, [&rd, &wr] {
                auto f1 = rd.pop_mutation_fragment().consume(wr);
                auto f2 = rd.is_buffer_empty() ? rd.fill_buffer(db::no_timeout) : make_ready_future<>();
                return when_all_succeed(std::move(f1), std::move(f2)).discard_result();
            });
        }).then_wrapped([&wr] (future<> f) {
            if (f.failed()) {
                auto ex = f.get_exception();
                wr.abort(ex);
                return make_exception_future<>(ex);
            } else {
                return wr.consume_end_of_stream();
            }
        });
    });
}

}
