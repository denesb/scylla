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
        , _bind_password(bind_password)
        , _connection_factory(bind(std::mem_fn(&ldap_role_manager::reconnect), std::ref(*this))) {
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
    if (!parse_url(get_url("dummy-user"))) { // Just need host and port -- any user should do.
        return make_exception_future(
                std::runtime_error(fmt::format("error getting LDAP server address from template {}", _query_template)));
    }
    return _std_mgr.start();
}

using conn_ptr = lw_shared_ptr<ldap_connection>;

future<conn_ptr> ldap_role_manager::connect() {
    const auto desc = parse_url(get_url("dummy-user")); // Just need host and port -- any user should do.
    if (!desc) {
        return make_exception_future<conn_ptr>(
                std::runtime_error("connect attempted before a successful start"));
    }
    return net::dns::resolve_name(desc->lud_host).then([this, port = desc->lud_port] (net::inet_address host) {
        const socket_address addr(host, uint16_t(port));
        return seastar::connect(addr).then([this] (connected_socket sock) {
            auto conn = make_lw_shared<ldap_connection>(std::move(sock));
            return conn->simple_bind(_bind_name.c_str(), _bind_password.c_str()).then(
                    [this, conn = std::move(conn)] (ldap_msg_ptr response) {
                        if (!response || ldap_msgtype(response.get()) != LDAP_RES_BIND) {
                            throw std::runtime_error(format("simple_bind error: {}", conn->get_error()));
                        }
                        return make_ready_future<conn_ptr>(std::move(conn));
                    });
        });
    });
}

future<conn_ptr> ldap_role_manager::reconnect() {
    using std::optional;
    return do_with(5u, [this] (unsigned& retries_left) {
        using namespace std::literals::chrono_literals;
        return exponential_backoff_retry::do_until_value(1s, 32s, _reconnect_aborter, [this, &retries_left] {
            if (!retries_left) {
                return make_ready_future<optional<conn_ptr>>(conn_ptr{});
            }
            mylog.trace("reconnect() retrying ({} attempts left)", retries_left);
            --retries_left;
            return connect().then([this] (conn_ptr conn) {
                mylog.trace("reconnect() success");
                return optional<conn_ptr>(std::move(conn));
            }).handle_exception([this] (std::exception_ptr ex) {
                mylog.error("error in reconnect: {}", ex);
                return optional<conn_ptr>{};
            });
        }).then([this] (conn_ptr conn) {
            mylog.trace("reconnect() finished backoff, conn={}", reinterpret_cast<void*>(conn.get()));
            return conn ? make_ready_future<conn_ptr>(std::move(conn)) :
                    make_exception_future<conn_ptr>(
                            std::runtime_error("reconnect failed after 5 attempts"));
        });
    });
}

future<> ldap_role_manager::stop() {
    _reconnect_aborter.request_abort();
    return _std_mgr.stop().then([this] { return _connection_factory.stop(); });
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
    const auto url = get_url(grantee_name.data());
    auto desc = parse_url(url);
    if (!desc) {
        return make_exception_future<role_set>(std::runtime_error(format("Error parsing URL {}", url)));
    }
    return _connection_factory.with_connection([this, desc = move(desc), grantee_name = sstring(grantee_name)]
                                               (ldap_connection& conn) {
        return conn.search(desc->lud_dn, desc->lud_scope, desc->lud_filter, desc->lud_attrs,
                           /*attrsonly=*/0, /*serverctrls=*/nullptr, /*clientctrls=*/nullptr,
                           /*timeout=*/nullptr, /*sizelimit=*/0)
                .then([this, &conn, grantee_name = std::move(grantee_name)] (ldap_msg_ptr res) {
                    mylog.trace("query_granted: got search results");
                    const auto mtype = ldap_msgtype(res.get());
                    if (mtype != LDAP_RES_SEARCH_ENTRY && mtype != LDAP_RES_SEARCH_RESULT && mtype != LDAP_RES_SEARCH_REFERENCE) {
                        mylog.error("ldap search yielded result {} of type {}", static_cast<const void*>(res.get()), mtype);
                        throw std::runtime_error("ldap_role_manager: search result has wrong type");
                    }
                    return do_with(
                            get_attr_values(conn.get_ldap(), res.get(), _target_attr.c_str()),
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
    });
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
