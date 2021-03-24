/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "ldap_role_manager.hh"

#include <boost/algorithm/string/replace.hpp>
#include <fmt/format.h>
#include <ldap.h>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/dns.hh>
#include <seastar/util/log.hh>
#include <vector>

#include "common.hh"
#include "cql3/query_processor.hh"
#include "database.hh"
#include "exceptions/exceptions.hh"
#include "seastarx.hh"
#include "utils/class_registrator.hh"
#include "db/config.hh"

namespace {

logger mylog{"ldap_role_manager"}; // `log` is taken by math.

struct url_desc_deleter {
    void operator()(LDAPURLDesc *p) {
        ldap_free_urldesc(p);
    }
};

using url_desc_ptr = std::unique_ptr<LDAPURLDesc, url_desc_deleter>;

url_desc_ptr parse_url(sstring_view url) {
    LDAPURLDesc *desc = nullptr;
    if (ldap_url_parse(url.data(), &desc)) {
        mylog.error("error in ldap_url_parse({})", url);
    }
    return url_desc_ptr(desc);
}

/// Extracts attribute \p attr from all entries in \p res.
std::vector<sstring> get_attr_values(LDAP* ld, LDAPMessage* res, const char* attr) {
    std::vector<sstring> values;
    mylog.debug("Analyzing search results");
    for (auto e = ldap_first_entry(ld, res); e; e = ldap_next_entry(ld, e)) {
        struct deleter {
            void operator()(berval** p) { ldap_value_free_len(p); }
            void operator()(char* p) { ldap_memfree(p); }
        };
        const std::unique_ptr<char, deleter> dname(ldap_get_dn(ld, e));
        mylog.debug("Analyzing entry {}", dname.get());
        const std::unique_ptr<berval*, deleter> vals(ldap_get_values_len(ld, e, attr));
        if (!vals) {
            mylog.warn("LDAP entry {} has no attribute {}", dname.get(), attr);
            continue;
        }
        for (size_t i = 0; vals.get()[i]; ++i) {
            values.emplace_back(vals.get()[i]->bv_val, vals.get()[i]->bv_len);
        }
    }
    mylog.debug("Done analyzing search results; extracted roles {}", values);
    return values;
}

const char* ldap_role_manager_full_name = "com.scylladb.auth.LDAPRoleManager";

} // anonymous namespace

namespace auth {

static const class_registrator<
    role_manager,
    ldap_role_manager,
    cql3::query_processor&,
    ::service::migration_manager&> registration(ldap_role_manager_full_name);

ldap_role_manager::ldap_role_manager(
        sstring_view query_template, sstring_view target_attr, sstring_view bind_name, sstring_view bind_password,
        cql3::query_processor& qp, ::service::migration_manager& mm)
        : _std_mgr(qp, mm), _query_template(query_template), _target_attr(target_attr), _bind_name(bind_name)
      , _bind_password(bind_password), _retries(0) {
}

ldap_role_manager::ldap_role_manager(cql3::query_processor& qp, ::service::migration_manager& mm)
    : ldap_role_manager(
            qp.db().get_config().ldap_url_template(),
            qp.db().get_config().ldap_attr_role(),
            qp.db().get_config().ldap_bind_dn(),
            qp.db().get_config().ldap_bind_passwd(),
            qp,
            mm) {
}

std::string_view ldap_role_manager::qualified_java_name() const noexcept {
    return ldap_role_manager_full_name;
}

const resource_set& ldap_role_manager::protected_resources() const {
    return _std_mgr.protected_resources();
}

future<> ldap_role_manager::start() {
    return when_all(_std_mgr.start(), connect()).discard_result();
}

future<> ldap_role_manager::connect() {
    const auto desc = parse_url(get_url("dummy-user")); // Just need host and port -- any user should do.
    if (!desc) {
        return make_ready_future();
    }
    return net::dns::resolve_name(desc->lud_host).then([this, port = desc->lud_port] (net::inet_address host) {
        const socket_address addr(host, uint16_t(port));
        return seastar::connect(addr).then([this] (connected_socket sock) {
            _conn = std::make_unique<ldap_connection>(std::move(sock));
            return _conn->simple_bind(_bind_name.c_str(), _bind_password.c_str()).then(
                    [this] (ldap_msg_ptr response) {
                        if (!response || ldap_msgtype(response.get()) != LDAP_RES_BIND) {
                            reset_connection();
                            throw std::runtime_error(format("simple_bind error: {}", _conn->get_error()));
                        }
                    }).handle_exception([this] (std::exception_ptr e) {
                        mylog.error("giving up due to LDAP bind error: {}", e);
                        reset_connection();
                    });
        });
    }).handle_exception([this] (std::exception_ptr e) {
        mylog.error("error connecting to the LDAP server: '{}'; attempting to reconnect", e);
        return reconnect();
    });
}

future<> ldap_role_manager::reconnect() {
    mylog.trace("reconnect()");
    reset_connection();
    if (_retries < retry_limit) {
        ++_retries;
        mylog.trace("reconnect() increasing count to {}", _retries);
        using namespace std::literals::chrono_literals;
        return sleep(1s * (1 << _retries)).then([this] {
            mylog.trace("reconnect() invoking connect()");
            return connect().then([this] {
                mylog.trace("reconnect() success, resetting count to 0");
                _retries = 0;
            }).handle_exception([this] (std::exception_ptr e) {
                mylog.error("reconnect failed: {}", e);
                return reconnect();
            });
        });
    } else {
        mylog.error("giving up on reconnect after {} attempts", _retries);
        return make_ready_future();
    }
}

void ldap_role_manager::reset_connection() {
    if (_conn) {
        auto p = _conn.release();
        mylog.trace("reset_connection({})", static_cast<const void*>(p));
        // OK to drop the continuation, which frees all its resources and holds no potentially dangling
        // references.  There is no infinite spawning because this code is gated on retry_limit.
        (void) p->close().then_wrapped([p] (future<>) {
            mylog.trace("reset_connection() deleting {}", static_cast<const void*>(p));
            delete p;
        });
        mylog.trace("reset_connection({}) done", static_cast<const void*>(p));
    }
}

future<> ldap_role_manager::stop() {
    return when_all(
            _std_mgr.stop(),
            _conn ? _conn->close() : now())
            .discard_result();
}

future<> ldap_role_manager::create(std::string_view name, const role_config& config) const {
    return _std_mgr.create(name, config);
}

future<> ldap_role_manager::drop(std::string_view name) const {
    return _std_mgr.drop(name);
}

future<> ldap_role_manager::alter(std::string_view name, const role_config_update& config) const {
    return _std_mgr.alter(name, config);
}

future<> ldap_role_manager::grant(std::string_view, std::string_view) const {
    throw exceptions::invalid_request_exception("Cannot grant roles with LDAPRoleManager.");
}

future<> ldap_role_manager::revoke(std::string_view, std::string_view) const {
    throw exceptions::invalid_request_exception("Cannot revoke roles with LDAPRoleManager.");
}

future<role_set> ldap_role_manager::query_granted(std::string_view grantee_name, recursive_role_query) const {
    try {
        if (!_conn) {
            return make_ready_future<role_set>(role_set{sstring(grantee_name)});
        }
        auto desc = parse_url(get_url(grantee_name.data()));
        if (!desc) {
            return make_ready_future<role_set>(role_set{sstring(grantee_name)});
        }
        return _conn->search(desc->lud_dn, desc->lud_scope, desc->lud_filter, desc->lud_attrs,
                             /*attrsonly=*/0, /*serverctrls=*/nullptr, /*clientctrls=*/nullptr,
                             /*timeout=*/nullptr, /*sizelimit=*/0)
                .then([this, grantee_name = sstring(grantee_name)] (ldap_msg_ptr res) {
                    mylog.trace("query_granted: got search results");
                    const auto mtype = ldap_msgtype(res.get());
                    if (mtype != LDAP_RES_SEARCH_ENTRY && mtype != LDAP_RES_SEARCH_RESULT && mtype != LDAP_RES_SEARCH_REFERENCE) {
                        mylog.error("ldap search yielded result {} of type {}", static_cast<const void*>(res.get()), ldap_msgtype(res.get()));
                        throw std::runtime_error("ldap_role_manager: search result has wrong type");
                    }
                    return do_with(
                            get_attr_values(_conn->get_ldap(), res.get(), _target_attr.c_str()),
                            auth::role_set{grantee_name},
                            [this] (const std::vector<sstring>& values, auth::role_set& valid_roles) {
                                // Each value is a role to be granted.
                                return parallel_for_each(values, [this, &valid_roles] (const sstring& ldap_role) {
                                    return _std_mgr.exists(ldap_role).then([&valid_roles, &ldap_role] (bool exists) {
                                        if (exists) {
                                            valid_roles.insert(ldap_role);
                                        } else {
                                            mylog.error("unrecognized role received from LDAP: {}", ldap_role);
                                        }
                                    });
                                }).then([&valid_roles] {
                                    return make_ready_future<role_set>(valid_roles);
                                });
                            });
                });
    } catch (...) {
        return make_exception_future<role_set>(std::current_exception());
    }
}

future<role_set> ldap_role_manager::query_all() const {
    return _std_mgr.query_all();
}

future<bool> ldap_role_manager::exists(std::string_view role_name) const {
    return _std_mgr.exists(role_name);
}

future<bool> ldap_role_manager::is_superuser(std::string_view role_name) const {
    return _std_mgr.is_superuser(role_name);
}

future<bool> ldap_role_manager::can_login(std::string_view role_name) const {
    return _std_mgr.can_login(role_name);
}

future<std::optional<sstring>> ldap_role_manager::get_attribute(
        std::string_view role_name, std::string_view attribute_name) const {
    return _std_mgr.get_attribute(role_name, attribute_name);
}

future<role_manager::attribute_vals> ldap_role_manager::query_attribute_for_all(std::string_view attribute_name) const {
    return _std_mgr.query_attribute_for_all(attribute_name);
}

future<> ldap_role_manager::set_attribute(
        std::string_view role_name, std::string_view attribute_name, std::string_view attribute_value) const {
    return _std_mgr.set_attribute(role_name, attribute_value, attribute_value);
}

future<> ldap_role_manager::remove_attribute(std::string_view role_name, std::string_view attribute_name) const {
    return _std_mgr.remove_attribute(role_name, attribute_name);
}

sstring ldap_role_manager::get_url(std::string_view user) const {
    return boost::replace_all_copy(_query_template, "{USER}", user);
}

} // namespace auth
