/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <memory>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/temporary_buffer.hh>

#include "utils/disk-error-handler.hh"
#include "log.hh"

namespace sstables {

extern logging::logger sstlog;

class random_access_reader {
    std::unique_ptr <input_stream<char>> _in;
    seastar::gate _close_gate;
protected:
    virtual input_stream<char> open_at(uint64_t pos) = 0;

public:
    future <temporary_buffer<char>> read_exactly(size_t n) {
        return _in->read_exactly(n);
    }

    void seek(uint64_t pos) {
        if (_in) {
            // Future is waited on indirectly in `close()` (via `_close_gate`).
            // FIXME: error handling
            (void)seastar::with_gate(_close_gate, [in = std::move(_in)]() mutable {
                auto fut = in->close();
                return fut.then([in = std::move(in)] {});
            });
        }
        _in = std::make_unique < input_stream < char >> (open_at(pos));
    }

    bool eof() const { return _in->eof(); }

    virtual future<> close() {
        return _close_gate.close().then([this] {
            return _in->close();
        });
    }

    virtual ~random_access_reader() {}
};

class file_random_access_reader : public random_access_reader {
    file _file;
    uint64_t _file_size;
    size_t _buffer_size;
    unsigned _read_ahead;
public:
    virtual input_stream<char> open_at(uint64_t pos) override {
        auto len = _file_size - pos;
        file_input_stream_options options;
        options.buffer_size = _buffer_size;
        options.read_ahead = _read_ahead;

        return make_file_input_stream(_file, pos, len, std::move(options));
    }

    explicit file_random_access_reader(file f, uint64_t file_size, size_t buffer_size = 8192, unsigned read_ahead = 4)
        : _file(std::move(f)), _file_size(file_size), _buffer_size(buffer_size), _read_ahead(read_ahead) {
        seek(0);
    }

    virtual future<> close() override {
        return random_access_reader::close().finally([this] {
            return _file.close().handle_exception([save = _file](auto ep) {
                sstlog.warn("sstable close failed: {}", ep);
                general_disk_error();
            });
        });
    }
};

}
