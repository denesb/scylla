/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <boost/intrusive/slist.hpp>
#include "in-memory-file-impl.hh"
#include <seastar/core/file.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/metrics.hh>
#include "utils/chunked_vector.hh"
#include "init.hh"
#include "db/extensions.hh"
#include "sstables/sstables.hh"
#include "utils/memory.hh"

static logging::logger mlogger("in_memory_store");

static uint8_t* alloc_file_block();
static void free_file_block(uint8_t*);

class in_memory_data_store final {
public:
    static constexpr size_t chunk_size = 8 * 1024;

    class in_memory_store_buffer {
        struct deleter {
            void operator()(uint8_t* p) {
                free_file_block(p);
            }
        };
        std::unique_ptr<uint8_t, deleter> _buf;
        static_assert(sizeof(_buf) == sizeof(void*));
        in_memory_store_buffer(uint8_t* ptr) : _buf(ptr) {}
    public:
        in_memory_store_buffer() = default;
        static in_memory_store_buffer alloc() {
            return in_memory_store_buffer(alloc_file_block());
        }
        uint8_t* get() const {
            return _buf.get();
        }
        uint8_t* get_write() {
            return get();
        }
        uint8_t* begin() const {
            return get();
        }
        constexpr size_t size() {
            return chunk_size;
        }
        explicit operator bool() {
            return bool(_buf);
        }
    };

private:
    utils::chunked_vector<in_memory_store_buffer> _data;
    size_t _size = 0;
public:
    size_t size() const {
        return _size;
    }
    void resize(size_t newsize);
    // return memory_output_stream rewound to write position
    auto to_output_memory_stream(size_t off);
    // return memory_input_stream rewound to read position
    auto to_input_memory_stream(size_t off);
};

class in_memory_file_handle_impl : public file_handle_impl {
    std::shared_ptr<foreign_ptr<std::unique_ptr<in_memory_data_store>>> _data;
public:
    explicit in_memory_file_handle_impl(std::shared_ptr<foreign_ptr<std::unique_ptr<in_memory_data_store>>> data) : _data(std::move(data)) {}
    std::unique_ptr<file_handle_impl> clone() const override {
        return std::make_unique<in_memory_file_handle_impl>(_data);
    }
    shared_ptr<file_impl> to_file() && override;
};

class in_memory_file_impl : public file_impl {
    // foreign/shared needed for dup() support
    std::shared_ptr<foreign_ptr<std::unique_ptr<in_memory_data_store>>> _data;
    size_t write_dma_internal(uint64_t pos, const void* buffer, size_t len);
    size_t read_dma_internal(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc);
    in_memory_data_store& store() {
        return *_data->get();
    }

public:
    in_memory_file_impl() : in_memory_file_impl(std::make_shared<foreign_ptr<std::unique_ptr<in_memory_data_store>>>(make_foreign(std::make_unique<in_memory_data_store>()))) {}
    explicit in_memory_file_impl(std::shared_ptr<foreign_ptr<std::unique_ptr<in_memory_data_store>>> data);
    future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override {
        return make_ready_future<size_t>(write_dma_internal(pos, buffer, len));
    }
    future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override;
    future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        return make_ready_future<size_t>(read_dma_internal(pos, buffer, len, pc));
    }
    future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override;
    future<> flush() override {
        return make_ready_future<>();
    }
    future<struct stat> stat() override {
        throw std::logic_error("in_memory_file::stat() is not supported");
    }
    future<> truncate(uint64_t length) override {
        store().resize(length);
        return make_ready_future<>();
    }
    future<> discard(uint64_t offset, uint64_t length) override;
    future<> allocate(uint64_t position, uint64_t length) override {
        return make_ready_future<>();
    }
    future<uint64_t> size() override {
        return make_ready_future<uint64_t>(store().size());
    }
    future<> close() override {
        return make_ready_future<>();
    }
    std::unique_ptr<file_handle_impl> dup() override {
        return std::make_unique<in_memory_file_handle_impl>(_data);
    }
    subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
        throw std::logic_error("in_memory_file::list_directory() is not supported");
    }
    future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) override;
};

in_memory_file_impl::in_memory_file_impl(std::shared_ptr<foreign_ptr<std::unique_ptr<in_memory_data_store>>> data)
        : _data(std::move(data))
{
    // We're not doing any actual dma transfers and we do not require
    // any specific alignment.
    // The following alignment parameters are solely an optimization.
    _memory_dma_alignment = seastar::cache_line_size;
    _disk_read_dma_alignment = in_memory_data_store::chunk_size;
    _disk_write_dma_alignment = in_memory_data_store::chunk_size;
}

void in_memory_data_store::resize(size_t newsize) {
    auto oldchunknr = _data.size();
    auto chunks = align_up(newsize, chunk_size) / chunk_size;

    if (oldchunknr == chunks) {
        return;
    }
    _data.resize(chunks);
    for (auto i = oldchunknr; i < chunks; i++) {
        _data[i] = in_memory_store_buffer::alloc();
        if (!_data[i]) {
            _data.resize(oldchunknr);
            throw std::system_error(std::make_error_code(std::errc::no_space_on_device), "In-Memory disk is out of space");
        }
        std::memset(_data[i].get_write(), 0, chunk_size);
    }
    _size = newsize;
}

auto in_memory_data_store::to_output_memory_stream(size_t off) {
    using mos = memory_output_stream<decltype(_data)::iterator>;
    size_t skip_chunks = off / chunk_size;
    mos os(mos::fragmented(_data.begin() + skip_chunks, _size - skip_chunks * chunk_size));
    os.skip(off % chunk_size);
    return os;
}

auto in_memory_data_store::to_input_memory_stream(size_t off) {
    using mis = memory_input_stream<decltype(_data)::iterator>;
    size_t skip_chunks = off / chunk_size;
    mis is(mis::fragmented(_data.begin() + skip_chunks, _size - skip_chunks * chunk_size));
    is.skip(off % chunk_size);
    return is;
}

size_t in_memory_file_impl::write_dma_internal(uint64_t pos, const void* buffer, size_t len) {
    if (pos + len > store().size()) {
        store().resize(pos + len);
    }
    auto os = store().to_output_memory_stream(pos);
    os.write(reinterpret_cast<const char*>(buffer), len);
    return len;
}

size_t in_memory_file_impl::read_dma_internal(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) {
    if (pos >= store().size()) {
        return 0;
    }
    size_t to_read = std::min(len, store().size() - pos);
    auto is = store().to_input_memory_stream(pos);
    is.read(reinterpret_cast<char*>(buffer), to_read);
    return to_read;
}

future<size_t> in_memory_file_impl::write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) {
    size_t len = 0;
    for (auto&& v : iov) {
        write_dma_internal(pos + len, v.iov_base, v.iov_len);
        len += v.iov_len;
    }
    return make_ready_future<size_t>(len);
}

future<size_t> in_memory_file_impl::read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) {
    size_t len = 0;
    for (auto&& v : iov) {
        auto l = read_dma_internal(pos + len, v.iov_base, v.iov_len, pc);
        len += l;
        if (l < v.iov_len) {
            break;
        }
    }
    return make_ready_future<size_t>(len);
}

future<> in_memory_file_impl::discard(uint64_t offset, uint64_t length) {
    temporary_buffer<uint8_t> tmp(length);
    std::memset(tmp.get_write(), 0, length);
    write_dma_internal(offset, tmp.get(), length);
    return make_ready_future<>();
}

future<temporary_buffer<uint8_t>> in_memory_file_impl::dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) {
    temporary_buffer<uint8_t> tmp(range_size);
    auto l = read_dma_internal(offset, tmp.get_write(), range_size, pc);
    if (l < range_size) {
        tmp.trim(l);
    }
    return make_ready_future<temporary_buffer<uint8_t>>(std::move(tmp));
}

shared_ptr<file_impl> in_memory_file_handle_impl::to_file() && {
    return ::make_shared<in_memory_file_impl>(std::move(_data));
}

thread_local std::unordered_map<sstring, file> in_memory_fs;

std::pair<file, bool> get_in_memory_file(sstring name) {
    bool new_file = false;
    auto fit = in_memory_fs.find(name);
    if (fit == in_memory_fs.end()) {
        mlogger.debug("Create new in memory file {}", name);
        new_file = true;
        in_memory_fs[name] = file(make_shared<in_memory_file_impl>());
    } else {
        mlogger.debug("Get existing in memory file {}", name);
    }
    return std::make_pair(in_memory_fs[name], new_file);
}

// remove/rename/link
future<> remove_memory_file(sstring path) {
    mlogger.trace("Removing file {}", path);
    return smp::invoke_on_all([=] {
        if (in_memory_fs.erase(path)) {
            mlogger.trace("Removed file {}", path);
        }
    });
}

future<> rename_memory_file(sstring oldpath, sstring newpath) {
    mlogger.trace("Renaming file {} to {}", oldpath, newpath);
    return smp::invoke_on_all([=] {
        auto fit = in_memory_fs.find(oldpath);
        if (fit != in_memory_fs.end()) {
            mlogger.trace("Renamed file {} to {}", oldpath, newpath);
            // operator[] may invalidate the iterator, so do with it before calling operator[]
            file f = std::move(fit->second);
            in_memory_fs.erase(fit);
            in_memory_fs[newpath] = std::move(f);
        }
    });
}

future<> link_memory_file(sstring oldpath, sstring newpath) {
    mlogger.trace("Linking file {} to {}", oldpath, newpath);
    return smp::invoke_on_all([=] {
        auto fit = in_memory_fs.find(oldpath);
        if (fit != in_memory_fs.end()) {
            mlogger.trace("Linked file {} to {}", oldpath, newpath);
            // see comment in rename
            file f = fit->second;
            in_memory_fs[newpath] = std::move(f);
        }
    });
}

// memory pool management
static size_t per_shard_memory_reserve;

namespace bi = boost::intrusive;

struct file_block :  public bi::slist_base_hook<bi::link_mode<bi::normal_link>> {
    char mem[in_memory_data_store::chunk_size - sizeof(bi::slist_base_hook<>)];
}  __attribute__((packed));

static thread_local bi::slist<file_block> free_file_blocks;
static thread_local seastar::metrics::metric_groups in_memory_store_metrics;
static thread_local size_t used_memory;

static_assert(sizeof(file_block) == in_memory_data_store::chunk_size, "wrong file_block size");

future<> init_in_memory_file_store(size_t memory_reserve_in_mb) {
    per_shard_memory_reserve = (memory_reserve_in_mb << 20) / smp::count;
    return smp::invoke_on_all([] {
        mlogger.debug("Reserving {} MB in {} chunks", per_shard_memory_reserve,
                (per_shard_memory_reserve + in_memory_data_store::chunk_size - 1) / in_memory_data_store::chunk_size);
        reserve_memory(per_shard_memory_reserve);
        size_t allocated = 0;
        while (allocated < per_shard_memory_reserve) {
            free_file_blocks.push_front(*new file_block);
            allocated += in_memory_data_store::chunk_size;
        }
        in_memory_store_metrics.add_group("in_memory_store", {
            seastar::metrics::make_gauge("total_memory", seastar::metrics::description("Memory reserved for in memory file store"), per_shard_memory_reserve),
            seastar::metrics::make_gauge("used_memory", seastar::metrics::description("Memory used by in memory file store"), used_memory)
        });
    });
}

future<> deinit_in_memory_file_store() {
    return smp::invoke_on_all([] {
        mlogger.debug("Deleting {} chunks. Used memory: {}", free_file_blocks.size(), used_memory);
        while (!free_file_blocks.empty()) {
            auto* p = &free_file_blocks.front();
            free_file_blocks.pop_front();
            delete p;
        }
        in_memory_store_metrics.clear();
    });
}

static uint8_t* alloc_file_block() {
    if (free_file_blocks.empty()) {
        return nullptr;
    }
    uint8_t* p = reinterpret_cast<uint8_t*>(&free_file_blocks.front());
    free_file_blocks.pop_front();
    used_memory += in_memory_data_store::chunk_size;
    return p;
}

static void free_file_block(uint8_t* p) {
    if (p) {
        used_memory -= in_memory_data_store::chunk_size;
        free_file_blocks.push_front(*reinterpret_cast<file_block*>(p));
    }
}

