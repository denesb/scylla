/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "sstables.hh"
#include "sstable_writer.hh"
#include "writer.hh"
#include "mx/writer.hh"

namespace sstables {

sstable_writer::sstable_writer(sstable& sst, const schema& s, uint64_t estimated_partitions,
        const sstable_writer_config& cfg, encoding_stats enc_stats, const io_priority_class& pc, shard_id shard) {
    if (sst.get_version() < oldest_writable_sstable_format) {
        on_internal_error(sstlog, format("writing sstables with too old format: {}", sst.get_version()));
    }
    _impl = downgrade_to_v1(mc::make_writer(sst, s, estimated_partitions, cfg, enc_stats, pc, shard));
    if (cfg.replay_position) {
        _impl->_collector.set_replay_position(cfg.replay_position.value());
    }
    if (cfg.sstable_level) {
        _impl->_collector.set_sstable_level(cfg.sstable_level.value());
    }
    sst.get_stats().on_open_for_writing();
}

void sstable_writer::consume_new_partition(const dht::decorated_key& dk) {
    _impl->_validator(dk);
    _impl->_validator(mutation_fragment::kind::partition_start, position_in_partition_view(position_in_partition_view::partition_start_tag_t{}));
    _impl->_sst.get_stats().on_partition_write();
    return _impl->consume_new_partition(dk);
}

void sstable_writer::consume(tombstone t) {
    _impl->_sst.get_stats().on_tombstone_write();
    return _impl->consume(t);
}

stop_iteration sstable_writer::consume(static_row&& sr) {
    _impl->_validator(mutation_fragment::kind::static_row, sr.position());
    if (!sr.empty()) {
        _impl->_sst.get_stats().on_static_row_write();
    }
    return _impl->consume(std::move(sr));
}

stop_iteration sstable_writer::consume(clustering_row&& cr) {
    _impl->_validator(mutation_fragment::kind::clustering_row, cr.position());
    _impl->_sst.get_stats().on_row_write();
    return _impl->consume(std::move(cr));
}

stop_iteration sstable_writer::consume(range_tombstone&& rt) {
    _impl->_validator(mutation_fragment::kind::range_tombstone, rt.position());
    _impl->_sst.get_stats().on_range_tombstone_write();
    return _impl->consume(std::move(rt));
}

stop_iteration sstable_writer::consume_end_of_partition() {
    _impl->_validator.on_end_of_partition();
    return _impl->consume_end_of_partition();
}

void sstable_writer::consume_end_of_stream() {
    _impl->_validator.on_end_of_stream();
    if (_impl->_c_stats.capped_local_deletion_time) {
        _impl->_sst.get_stats().on_capped_local_deletion_time();
    }
    return _impl->consume_end_of_stream();
}

sstable_writer::sstable_writer(sstable_writer&& o) = default;
sstable_writer& sstable_writer::operator=(sstable_writer&& o) = default;
sstable_writer::~sstable_writer() {
    if (_impl) {
        _impl->_sst.get_stats().on_close_for_writing();
    }
}

sstable_writer_v2::sstable_writer_v2(sstable& sst, const schema& s, uint64_t estimated_partitions,
        const sstable_writer_config& cfg, encoding_stats enc_stats, const io_priority_class& pc, shard_id shard) {
    if (sst.get_version() < oldest_writable_sstable_format) {
        on_internal_error(sstlog, format("writing sstables with too old format: {}", sst.get_version()));
    }
    _impl = mc::make_writer(sst, s, estimated_partitions, cfg, enc_stats, pc, shard);
    if (cfg.replay_position) {
        _impl->_collector.set_replay_position(cfg.replay_position.value());
    }
    if (cfg.sstable_level) {
        _impl->_collector.set_sstable_level(cfg.sstable_level.value());
    }
    sst.get_stats().on_open_for_writing();
}

void sstable_writer_v2::consume_new_partition(const dht::decorated_key& dk) {
    _impl->_validator(dk);
    _impl->_validator(mutation_fragment_v2::kind::partition_start, position_in_partition_view(position_in_partition_view::partition_start_tag_t{}));
    _impl->_sst.get_stats().on_partition_write();
    return _impl->consume_new_partition(dk);
}

void sstable_writer_v2::consume(tombstone t) {
    _impl->_sst.get_stats().on_tombstone_write();
    return _impl->consume(t);
}

stop_iteration sstable_writer_v2::consume(static_row&& sr) {
    _impl->_validator(mutation_fragment_v2::kind::static_row, sr.position());
    if (!sr.empty()) {
        _impl->_sst.get_stats().on_static_row_write();
    }
    return _impl->consume(std::move(sr));
}

stop_iteration sstable_writer_v2::consume(clustering_row&& cr) {
    _impl->_validator(mutation_fragment_v2::kind::clustering_row, cr.position());
    _impl->_sst.get_stats().on_row_write();
    return _impl->consume(std::move(cr));
}

stop_iteration sstable_writer_v2::consume(range_tombstone_change&& rtc) {
    _impl->_validator(mutation_fragment_v2::kind::range_tombstone_change, rtc.position());
    _impl->_sst.get_stats().on_range_tombstone_write();
    return _impl->consume(std::move(rtc));
}

stop_iteration sstable_writer_v2::consume_end_of_partition() {
    _impl->_validator.on_end_of_partition();
    return _impl->consume_end_of_partition();
}

void sstable_writer_v2::consume_end_of_stream() {
    _impl->_validator.on_end_of_stream();
    if (_impl->_c_stats.capped_local_deletion_time) {
        _impl->_sst.get_stats().on_capped_local_deletion_time();
    }
    return _impl->consume_end_of_stream();
}

sstable_writer_v2::sstable_writer_v2(sstable_writer_v2&& o) = default;
sstable_writer_v2& sstable_writer_v2::operator=(sstable_writer_v2&& o) = default;
sstable_writer_v2::~sstable_writer_v2() {
    if (_impl) {
        _impl->_sst.get_stats().on_close_for_writing();
    }
}

std::unique_ptr<sstable_writer::writer_impl> downgrade_to_v1(std::unique_ptr<sstable_writer_v2::writer_impl> impl) {
    class downgrade_impl : public sstable_writer::writer_impl {
        std::unique_ptr<sstable_writer_v2::writer_impl> _impl;
        range_tombstone_change_generator _rtc_gen;
        tombstone _current_rt;
    private:
        void flush_tombstones(position_in_partition_view pos) {
            _rtc_gen.flush(pos, [&] (range_tombstone_change rtc) {
                _current_rt = rtc.tombstone();
                _impl->consume(std::move(rtc));
            });
        }
    public:
        downgrade_impl(std::unique_ptr<sstable_writer_v2::writer_impl> impl)
            : writer_impl(impl->_sst, impl->_schema, impl->_pc, impl->_cfg)
            , _impl(std::move(impl))
            , _rtc_gen(_impl->_schema)
        { }
        virtual void consume_new_partition(const dht::decorated_key& dk) override {
            _current_rt = {};
            return _impl->consume_new_partition(dk);
        }
        virtual void consume(tombstone t) override {
            return _impl->consume(t);
        }
        virtual stop_iteration consume(static_row&& sr) override {
            return _impl->consume(std::move(sr));
        }
        virtual stop_iteration consume(clustering_row&& cr) override {
            flush_tombstones(cr.position());
            return _impl->consume(std::move(cr));
        }
        virtual stop_iteration consume(range_tombstone&& rt) override {
            flush_tombstones(rt.position());
            _rtc_gen.consume(std::move(rt));
            return stop_iteration::no;
        }
        virtual stop_iteration consume_end_of_partition() override {
            flush_tombstones(position_in_partition::after_all_clustered_rows());
            if (_current_rt) {
                _impl->consume(range_tombstone_change(position_in_partition::after_all_clustered_rows(), {}));
            }
            return _impl->consume_end_of_partition();
        }
        virtual void consume_end_of_stream() override {
            return _impl->consume_end_of_stream();
        }
    };
    return std::make_unique<downgrade_impl>(std::move(impl));
}

} // namespace sstables
