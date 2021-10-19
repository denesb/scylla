/*
 * Copyright (C) 2021 ScyllaDB
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

#include "mutation_writer/partition_based_splitting_writer.hh"

namespace mutation_writer {

class partition_based_splitting_mutation_writer {
    struct bucket {
        bucket_writer writer;
        dht::decorated_key last_key;
        size_t data_size = 0;
    };

private:
    schema_ptr _schema;
    reader_permit _permit;
    reader_consumer _consumer;
    unsigned _max_buckets;
    std::vector<bucket> _buckets;
    bucket* _current_bucket = nullptr;

    future<> write_to_bucket(mutation_fragment&& mf) {
        _current_bucket->data_size += mf.memory_usage();
        return _current_bucket->writer.consume(std::move(mf));
    }

    future<bucket*> create_bucket_for(const dht::decorated_key& key) {
        if (_buckets.size() < _max_buckets) {
            return make_ready_future<bucket*>(&_buckets.emplace_back(bucket{bucket_writer(_schema, _permit, _consumer), key}));
        }
        auto it = std::max_element(_buckets.begin(), _buckets.end(), [this] (const bucket& a, const bucket& b) {
            return dht::ring_position_tri_compare(*_schema, a.last_key, b.last_key) < 0;
        });
        it->writer.consume_end_of_stream();
        auto close_writer = it->writer.close();
        return close_writer.then([this, it, &key] () mutable {
            try {
                *it = bucket{bucket_writer(_schema, _permit, _consumer), key};
            } catch (...) {
                _buckets.erase(it);
                throw;
            }
            return &*it;
        });
    }

    std::vector<bucket>::iterator find_bucket_with_smallest_gap(const dht::decorated_key& key) {
        // Find the  with the largest key, where this partition doesn't cause
        // monotonicity violations, maximising the chance that a next partition
        // will find an existing bucket to fit into.
        auto min_it = _buckets.end();
        uint64_t min_diff = std::numeric_limits<uint64_t>::max();
        for (auto it = _buckets.begin(); it != _buckets.end(); ++it) {
            const auto& bucket = *it;
            if (dht::ring_position_tri_compare(*_schema, bucket.last_key, key) >= 0) {
                continue;
            }
            // We convert tokens to uint64_t to avoid overflowing int64_t with the diff
            // We know the subtraction is safe because we just checked that key > bucket.last_key.
            const auto diff = static_cast<uint64_t>(key.token().raw()) - static_cast<uint64_t>(bucket.last_key.token().raw());
            if (diff < min_diff) {
                min_diff = diff;
                min_it = it;
            }
        }
        return min_it;
    }
public:
    partition_based_splitting_mutation_writer(schema_ptr schema, reader_permit permit, reader_consumer consumer, unsigned max_buckets)
        : _schema(std::move(schema))
        , _permit(std::move(permit))
        , _consumer(std::move(consumer))
        , _max_buckets(max_buckets)
    {}

    future<> consume(partition_start ps_orig) {
        auto ps = std::make_unique<partition_start>(std::move(ps_orig));
        auto fut = make_ready_future<>();
        auto set_current_bucket_to_ps = [this, &ps = *ps] {
            return create_bucket_for(ps.key()).then([this] (bucket *bp) {
                _current_bucket = bp;
            });
        };
        if (_buckets.empty()) {
            fut = set_current_bucket_to_ps();
        } else if (dht::ring_position_tri_compare(*_schema, _current_bucket->last_key, ps->key()) < 0) {
            // No need to change bucket, just update the last key.
            _current_bucket->last_key = ps->key();
        } else {
            auto it = find_bucket_with_smallest_gap(ps->key());
            if (it == _buckets.end()) {
                fut = set_current_bucket_to_ps();
            } else {
                _current_bucket = &*it;
                _current_bucket->last_key = ps->key();
            }
        }
        return fut.then([this, ps = std::move(ps)] () mutable {
            return write_to_bucket(mutation_fragment(*_schema, _permit, std::move(*ps)));
        });
    }

    future<> consume(static_row&& sr) {
        return write_to_bucket(mutation_fragment(*_schema, _permit, std::move(sr)));
    }

    future<> consume(clustering_row&& cr) {
        return write_to_bucket(mutation_fragment(*_schema, _permit, std::move(cr)));
    }

    future<> consume(range_tombstone&& rt) {
        return write_to_bucket(mutation_fragment(*_schema, _permit, std::move(rt)));
    }

    future<> consume(partition_end&& pe) {
        return write_to_bucket(mutation_fragment(*_schema, _permit, std::move(pe)));
    }

    void consume_end_of_stream() {
        for (auto& bucket : _buckets) {
            bucket.writer.consume_end_of_stream();
        }
    }
    void abort(std::exception_ptr ep) {
        for (auto& bucket : _buckets) {
            bucket.writer.abort(ep);
        }
    }
    future<> close() noexcept {
        return parallel_for_each(_buckets, [] (bucket& bucket) {
            return bucket.writer.close();
        });
    }
};

future<> segregate_by_partition(flat_mutation_reader producer, unsigned max_buckets, reader_consumer consumer) {
    auto schema = producer.schema();
    auto permit = producer.permit();
    return feed_writer(std::move(producer),
            partition_based_splitting_mutation_writer(std::move(schema), std::move(permit), std::move(consumer), max_buckets));
}

} // namespace mutation_writer
