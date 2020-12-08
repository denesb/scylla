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
#include "mirror-file-impl.hh"
#include <seastar/core/file.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/metrics.hh>
#include "utils/chunked_vector.hh"
#include "utils/config_file.hh"
#include "init.hh"
#include "db/extensions.hh"
#include "db/config.hh"
#include "sstables/sstables.hh"

class mirror_file_handle_impl : public file_handle_impl {
    seastar::file_handle _primary;
    seastar::file_handle _secondary;
public:
    mirror_file_handle_impl(seastar::file_handle primary, seastar::file_handle secondary) : _primary(std::move(primary)), _secondary(std::move(secondary)) {}
    std::unique_ptr<file_handle_impl> clone() const override {
        return std::make_unique<mirror_file_handle_impl>(_primary, _secondary);
    }
    shared_ptr<file_impl> to_file() && override;
};

// writes to both reads from secondary
class mirror_file_impl : public file_impl {
    file _primary;
    file _secondary;
    bool _check_integrity;
public:
    mirror_file_impl(file primary, file secondary, bool check_integrity = false);
    future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override;
    future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override;
    future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override;
    future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override;
    future<> flush() override;
    future<struct stat> stat() override;
    future<> truncate(uint64_t length) override;
    future<> discard(uint64_t offset, uint64_t length) override;
    future<> allocate(uint64_t position, uint64_t length) override;
    future<uint64_t> size() override;
    future<> close() override;
    std::unique_ptr<file_handle_impl> dup() override;
    subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override;
    future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) override;
};

mirror_file_impl::mirror_file_impl(file primary, file secondary, bool check_integrity)
        : _primary(std::move(primary)), _secondary(std::move(secondary)), _check_integrity(check_integrity)
{
    _memory_dma_alignment = std::max(_primary.memory_dma_alignment(), _secondary.memory_dma_alignment());
    if (_check_integrity) {
        // we read from the primary file only when check_integrity is enabled
        _disk_read_dma_alignment = std::max(_primary.disk_read_dma_alignment(), _secondary.disk_read_dma_alignment());
    } else {
        _disk_read_dma_alignment = _secondary.disk_read_dma_alignment();
    }
    _disk_write_dma_alignment = std::max(_primary.disk_write_dma_alignment(), _secondary.disk_write_dma_alignment());
}

future<size_t> mirror_file_impl::write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) {
    return get_file_impl(_primary)->write_dma(pos, buffer, len, pc).then([this, pos, buffer, &pc] (size_t len) {
        return get_file_impl(_secondary)->write_dma(pos, buffer, len, pc);
    });
}

future<size_t> mirror_file_impl::write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) {
    return get_file_impl(_primary)->write_dma(pos, iov, pc).then([this, pos, iov, &pc] (size_t len) mutable {
        size_t l = 0;
        auto v = iov.begin();
        while(v != iov.end()) {
            l += v->iov_len;
            if (l > len) {
                break;
            }
            v++;
        }
        if (v != iov.end()) {
            v->iov_len -= (l - len);
            iov.erase(v + 1, iov.end());
        }
        return get_file_impl(_secondary)->write_dma(pos, iov, pc);
    });
}

future<size_t> mirror_file_impl::read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) {
    auto f = get_file_impl(_secondary)->read_dma(pos, buffer, len, pc);
    if (_check_integrity) {
        return f.then([this, pos, len , buffer, &pc] (size_t secondary_size) {
            auto b = allocate_aligned_buffer<uint8_t>(len, _primary.memory_dma_alignment());
            auto p = b.get();
            return get_file_impl(_primary)->read_dma(pos, p, len, pc).then([this, secondary_size, buffer, b = std::move(b)] (size_t primary_size) {
                if (primary_size != secondary_size) {
                    throw std::runtime_error(sprint("inconsistency between on disk and memory file sizes (%d != %d)", primary_size, secondary_size));
                }
                if (std::memcmp(buffer, b.get(), primary_size)) {
                    throw std::runtime_error("inconsistency between on disk and memory file data");
                }
                return primary_size;
            });
        });
    } else {
        return f;
    }
}

future<size_t> mirror_file_impl::read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) {
    auto f = get_file_impl(_secondary)->read_dma(pos, iov, pc);
    if (_check_integrity) {
        return f.then([this, pos, iov = std::move(iov), &pc] (size_t secondary_size) mutable {
            std::vector<std::unique_ptr<uint8_t[], free_deleter>> vb(iov.size());
            auto iov2 = iov;
            for (size_t i = 0; i < iov2.size(); i++) {
                vb[i] = allocate_aligned_buffer<uint8_t>(iov[i].iov_len, _primary.memory_dma_alignment());
                iov2[i].iov_base = vb[i].get();
            }
            return get_file_impl(_primary)->read_dma(pos, std::move(iov2), pc).then([this, secondary_size, iov = std::move(iov), vb = std::move(vb)] (size_t primary_size) {
                if (primary_size != secondary_size) {
                    throw std::runtime_error(sprint("inconsistency between on disk and memory file sizes (%d != %d)", primary_size, secondary_size));
                }
                for (size_t i = 0; i < iov.size(); i++) {
                    if (std::memcmp(iov[i].iov_base, vb[i].get(), iov[i].iov_len)) {
                        throw std::runtime_error("inconsistency between on disk and memory file data");
                    }
                }
                return primary_size;
            });
        });
    } else {
        return f;
    }
}

future<> mirror_file_impl::flush() {
    return get_file_impl(_primary)->flush().then([this] {
        return get_file_impl(_secondary)->flush();
    });
}

future<struct stat> mirror_file_impl::stat() {
    return get_file_impl(_primary)->stat();
}

future<> mirror_file_impl::truncate(uint64_t length) {
    return get_file_impl(_primary)->truncate(length).then([this, length] {
        return get_file_impl(_secondary)->truncate(length);
    });
}

future<> mirror_file_impl::discard(uint64_t offset, uint64_t length) {
    return get_file_impl(_primary)->discard(offset, length).then([this, offset, length] {
        return get_file_impl(_secondary)->discard(offset, length);
    });
}

future<> mirror_file_impl::allocate(uint64_t position, uint64_t length) {
    return get_file_impl(_primary)->allocate(position, length).then([this, position, length] {
        return get_file_impl(_secondary)->allocate(position, length);
    });
}

future<uint64_t> mirror_file_impl::size() {
    return get_file_impl(_primary)->size();
}

future<> mirror_file_impl::close() {
    return get_file_impl(_primary)->close().finally([this] {
        return get_file_impl(_secondary)->close();
    });
}

std::unique_ptr<file_handle_impl> mirror_file_impl::dup() {
    return std::make_unique<mirror_file_handle_impl>(_primary.dup(), _secondary.dup());
}

subscription<directory_entry> mirror_file_impl::list_directory(std::function<future<> (directory_entry de)> next) {
    return get_file_impl(_primary)->list_directory(std::move(next));
}

future<temporary_buffer<uint8_t>> mirror_file_impl::dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) {
    auto f = get_file_impl(_secondary)->dma_read_bulk(offset, range_size, pc);
    if (_check_integrity) {
        return f.then([this, offset, range_size, &pc] (temporary_buffer<uint8_t> sb) {
            return get_file_impl(_primary)->dma_read_bulk(offset, range_size, pc).then([this, sb = std::move(sb), range_size, offset] (temporary_buffer<uint8_t> pb) mutable {
                if (sb.size() > pb.size()) { // bulk interface allows reading more than requested size, but memory file always exact
                    throw std::runtime_error(sprint("inconsistency between on disk and memory file sizes (%d < %d)", pb.size(), sb.size()));
                }
                if (std::memcmp(sb.get(), pb.get(), sb.size())) {
                    throw std::runtime_error("inconsistency between on disk and memory file data");
                }
                return std::move(sb);
            });
        });
    } else {
        return f;
    }
}

shared_ptr<file_impl> mirror_file_handle_impl::to_file() && {
    return ::make_shared<mirror_file_impl>(std::move(_primary).to_file(), std::move(_secondary).to_file());
}

file make_mirror_file(file primary, file secondary, bool check_integrity) {
    return file(make_shared<mirror_file_impl>(std::move(primary), std::move(secondary), check_integrity));
}

future<file> make_in_memory_mirror_file(file primary, sstring name, bool check_integrity, const io_priority_class& pc) {
    auto [mem_file, new_file] = get_in_memory_file(name);

    if (!new_file) {
        return make_ready_future<file>(make_mirror_file(std::move(primary), std::move(mem_file), check_integrity));
    }

    // create new memory file and read it into memory
    static constexpr size_t chunk = 128 * 1024;
    return primary.size().then([&pc, primary = std::move(primary), name = std::move(name), mem_file = std::move(mem_file), check_integrity] (uint64_t size) mutable {
        auto buf = allocate_aligned_buffer<uint8_t>(chunk, primary.memory_dma_alignment());
        return do_with(size, uint64_t(0), std::move(primary), std::move(mem_file), std::move(buf), [&pc, check_integrity] (uint64_t& size, uint64_t& off, file& primary, file& secondary, auto& bufptr) {
            return do_until([&size, &off] { return size == off; }, [&] {
                auto buf = bufptr.get();
                return primary.dma_read(off, buf, chunk, pc).then([&, buf] (size_t len) {
                    return secondary.dma_write(off, buf, len, pc).then([&, len] (size_t mem_len) {
                        assert(len == mem_len);
                        off += len;
                    });
                });
            }).then([&, check_integrity] {
                return make_ready_future<file>(make_mirror_file(std::move(primary), std::move(secondary), check_integrity));
            });
        });
    });
}

// remove/rename/link
future<> remove_mirrored_file(sstring path) {
    return remove_file(path).then([=] {
        return remove_memory_file(path);
    });
}

future<> rename_mirrored_file(sstring oldpath, sstring newpath) {
    return rename_file(oldpath, newpath).then([=] {
        return rename_memory_file(oldpath, newpath);
    });
}

future<> link_mirrored_file(sstring oldpath, sstring newpath) {
    return link_file(oldpath, newpath).then([=] {
        return link_memory_file(oldpath, newpath);
    });
}

// hook into configuration to register memory and extension
class in_memory_config_type: public configurable {
    const sstring _name;
    const sstring _desc;
    utils::config_file::named_value<uint32_t> _size;
public:
    explicit in_memory_config_type(utils::config_file& cfg) : _name("in_memory_storage_size_mb"), _desc("in memory storage size for sstables in MB (0 to disable)"), _size(&cfg, _name, utils::config_file::value_status::Used, 0, _desc)
 {}
    void append_options(db::config& cfg, boost::program_options::options_description_easy_init& init) override {
    }
    virtual future<> initialize(const boost::program_options::variables_map& map, const db::config& cfg, db::extensions& exts) {
        class in_memory_file_ext : public sstables::file_io_extension {
        public:
            future<file> wrap_file(sstables::sstable& sstable, sstables::component_type type, file f, open_flags flags) {
                if ((type == sstables::component_type::Index || type == sstables::component_type::Data) && sstable.get_schema()->is_in_memory()) {
                    return make_in_memory_mirror_file(std::move(f), sstable.filename(type));
                } else {
                    return make_ready_future<file>(std::move(f));
                }
            }
        };
        if (_size()) {
            exts.add_sstable_file_io_extension("in_memory_file_store", std::make_unique<in_memory_file_ext>());
            return init_in_memory_file_store(_size());
        }
        return make_ready_future<>();
    }
};

std::any get_in_memory_config_hook(utils::config_file& cfg) {
    return make_lw_shared<in_memory_config_type>(std::ref(cfg));
}
