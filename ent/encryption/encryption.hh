/*
 * Copyright (C) 2018 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <map>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/distributed.hh>

#include "../../bytes.hh"
#include "../../compress.hh"

class database;

namespace db {
class config;
class extensions;
}

namespace cql3 {
class query_processor;
}
namespace service {
class storage_service;
class migration_manager;
}

namespace encryption {
inline const sstring KEY_PROVIDER = "key_provider";
inline const sstring SECRET_KEY_PROVIDER_FACTORY_CLASS = "secret_key_provider_factory_class";
inline const sstring SECRET_KEY_FILE = "secret_key_file";
inline const sstring SYSTEM_KEY_FILE = "system_key_file";
inline const sstring CIPHER_ALGORITHM = "cipher_algorithm";
inline const sstring IV_LENGTH = "iv_length";
inline const sstring SECRET_KEY_STRENGTH = "secret_key_strength";

inline const sstring HOST_NAME = "kmip_host";
inline const sstring TEMPLATE_NAME = "template_name";
inline const sstring KEY_NAMESPACE = "key_namespace";

bytes base64_decode(const sstring&, size_t off = 0, size_t n = sstring::npos);
sstring base64_encode(const bytes&, size_t off = 0, size_t n = bytes::npos);
bytes calculate_md5(const bytes&, size_t off = 0, size_t n = bytes::npos);
bytes calculate_sha256(const bytes&, size_t off = 0, size_t n = bytes::npos);

future<temporary_buffer<char>> read_text_file_fully(const sstring&);
future<> write_text_file_fully(const sstring&, temporary_buffer<char>);
future<> write_text_file_fully(const sstring&, const sstring&);

class symmetric_key;
struct key_info;

using options = std::map<sstring, sstring>;
using opt_bytes = std::optional<bytes>;
using key_ptr = shared_ptr<symmetric_key>;

/**
 * wrapper for "options" (map) to provide an
 * interface returning empty optionals for
 * non-available values. Makes query simpler
 * and allows .value_or(...)-statements, which
 * are neat for default values.
 *
 * In the long run one could contemplate
 * using non-std maps with similar built-in
 * functionality for all our various configs
 * in the system, but for now we are firmly
 * entrenched in map<string, string>
 */
template<typename Map>
class map_wrapper {
    const Map& _options;
public:
    using mapped_type = typename Map::mapped_type;
    using key_type = typename Map::key_type;

    map_wrapper(const Map& opts)
        : _options(opts)
    {}

    std::optional<mapped_type> operator()(const key_type& k) const {
        auto i = _options.find(k);
        if (i != _options.end()) {
            return i->second;
        }
        return std::nullopt;
    }
};

using opt_wrapper = map_wrapper<options>;

key_info get_key_info(const options&);

class encryption_context;

class key_provider {
public:
    virtual ~key_provider()
    {}
    virtual future<std::tuple<key_ptr, opt_bytes>> key(const key_info&, opt_bytes = {}) = 0;
    virtual future<> validate() const {
        return make_ready_future<>();
    }
    virtual bool should_delay_read(const opt_bytes&) const {
        return false;
    }
private:
    friend std::ostream& operator<<(std::ostream&, const key_provider&);
    virtual void print(std::ostream&) const = 0;
};

std::ostream& operator<<(std::ostream&, const key_provider&);

class key_provider_factory {
public:
    virtual ~key_provider_factory()
    {}
    virtual shared_ptr<key_provider> get_provider(encryption_context& c, const options&) = 0;
};

class encryption_config;
class system_key;
class kmip_host;

/**
 * Context is a singleton object, shared across shards. I.e. even though there are obvious mutating
 * calls in it, it guarantees thread/shard safety.
 *
 * Why is this not a sharded thingamajing? Because its own instance methods need to send itself
 * as a shard-safe object forwards, and thus need to know that same shard, which breaks the circle of
 * ownership and stuff.
 */
class encryption_context {
public:
    virtual ~encryption_context() = default;
    virtual shared_ptr<key_provider> get_provider(const options&) = 0;
    virtual shared_ptr<system_key> get_system_key(const sstring&) = 0;
    virtual shared_ptr<kmip_host> get_kmip_host(const sstring&) = 0;

    virtual shared_ptr<key_provider> get_cached_provider(const sstring& id) const = 0;
    virtual void cache_provider(const sstring& id, shared_ptr<key_provider>) = 0;

    virtual const encryption_config& config() const = 0;
    virtual shared_ptr<symmetric_key> get_config_encryption_key() const = 0;

    virtual distributed<cql3::query_processor>& get_query_processor() const = 0;
    virtual distributed<service::storage_service>& get_storage_service() const = 0;
    virtual distributed<database>& get_database() const = 0;
    virtual distributed<service::migration_manager>& get_migration_manager() const = 0;

    sstring maybe_decrypt_config_value(const sstring&) const;
};

future<> register_extensions(const db::config&, const encryption_config&, db::extensions&);

}

