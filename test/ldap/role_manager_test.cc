/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "auth/standard_role_manager.hh"
#include "auth/ldap_role_manager.hh"
#include "auth/password_authenticator.hh"
#include "db/config.hh"

#include <fmt/format.h>
#include <seastar/testing/test_case.hh>

#include "test/lib/exception_utils.hh"
#include "ldap_common.hh"
#include "service/migration_manager.hh"
#include "test/lib/cql_test_env.hh"

auto make_manager(cql_test_env& env) {
    auto stop_role_manager = [] (auth::standard_role_manager* m) {
        m->stop().get();
        std::default_delete<auth::standard_role_manager>()(m);
    };
    return std::unique_ptr<auth::standard_role_manager, decltype(stop_role_manager)>(
            new auth::standard_role_manager(env.local_qp(), service::get_local_migration_manager()),
            std::move(stop_role_manager));
}

SEASTAR_TEST_CASE(create_role) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_manager(env);
        m->start().get0();

        const auto anon = auth::authenticated_user();

        //
        // Create a role, and verify its properties.
        //

        auth::role_config c;
        c.is_superuser = true;

        m->create("admin", c).get();
        BOOST_REQUIRE_EQUAL(m->exists("admin").get0(), true);
        BOOST_REQUIRE_EQUAL(m->can_login("admin").get0(), false);
        BOOST_REQUIRE_EQUAL(m->is_superuser("admin").get0(), true);

        BOOST_REQUIRE_EQUAL(
                m->query_granted("admin", auth::recursive_role_query::yes).get0(),
                std::unordered_set<sstring>{"admin"});

        //
        // Creating a role that already exists is an error.
        //

        BOOST_REQUIRE_THROW(m->create("admin", c).get0(), auth::role_already_exists);
    });
}

SEASTAR_TEST_CASE(drop_role) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_manager(env);
        m->start().get0();

        const auto anon = auth::authenticated_user();

        //
        // Create a role, then drop it, then verify it's gone.
        //

        m->create("lord", auth::role_config()).get();
        m->drop("lord").get();
        BOOST_REQUIRE_EQUAL(m->exists("lord").get0(), false);

        //
        // Dropping a role revokes it from other roles and revokes other roles from it.
        //

        m->create("peasant", auth::role_config()).get0();
        m->create("lord", auth::role_config()).get0();
        m->create("king", auth::role_config()).get0();

        auth::role_config tim_config;
        tim_config.is_superuser = false;
        tim_config.can_login = true;
        m->create("tim", tim_config).get0();

        m->grant("lord", "peasant").get0();
        m->grant("king", "lord").get0();
        m->grant("tim", "lord").get0();

        m->drop("lord").get0();

        BOOST_REQUIRE_EQUAL(
                m->query_granted("tim", auth::recursive_role_query::yes).get0(),
                std::unordered_set<sstring>{"tim"});

        BOOST_REQUIRE_EQUAL(
                m->query_granted("king", auth::recursive_role_query::yes).get0(),
                std::unordered_set<sstring>{"king"});

        //
        // Dropping a role that does not exist is an error.
        //

        BOOST_REQUIRE_THROW(m->drop("emperor").get0(), auth::nonexistant_role);
    });
}

SEASTAR_TEST_CASE(grant_role) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_manager(env);
        m->start().get0();

        const auto anon = auth::authenticated_user();

        auth::role_config jsnow_config;
        jsnow_config.is_superuser = false;
        jsnow_config.can_login = true;
        m->create("jsnow", jsnow_config).get0();

        m->create("lord", auth::role_config()).get0();
        m->create("king", auth::role_config()).get0();

        //
        // All kings have the rights of lords, and 'jsnow' is a king.
        //

        m->grant("king", "lord").get0();
        m->grant("jsnow", "king").get0();

        BOOST_REQUIRE_EQUAL(
                m->query_granted("king", auth::recursive_role_query::yes).get0(),
                (std::unordered_set<sstring>{"king", "lord"}));

        BOOST_REQUIRE_EQUAL(
                m->query_granted("jsnow", auth::recursive_role_query::no).get0(),
               (std::unordered_set<sstring>{"jsnow", "king"}));

        BOOST_REQUIRE_EQUAL(
                m->query_granted("jsnow", auth::recursive_role_query::yes).get0(),
                (std::unordered_set<sstring>{"jsnow", "king", "lord"}));

        // A non-existing role cannot be granted.
        BOOST_REQUIRE_THROW(m->grant("jsnow", "doctor").get0(), auth::nonexistant_role);

        // A role cannot be granted to a non-existing role.
        BOOST_REQUIRE_THROW(m->grant("hpotter", "lord").get0(), auth::nonexistant_role);
    });
}

SEASTAR_TEST_CASE(revoke_role) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_manager(env);
        m->start().get0();

        const auto anon = auth::authenticated_user();

        auth::role_config rrat_config;
        rrat_config.is_superuser = false;
        rrat_config.can_login = true;
        m->create("rrat", rrat_config).get0();

        m->create("chef", auth::role_config()).get0();
        m->create("sous_chef", auth::role_config()).get0();

        m->grant("chef", "sous_chef").get0();
        m->grant("rrat", "chef").get0();

        m->revoke("chef", "sous_chef").get0();
        BOOST_REQUIRE_EQUAL(
                m->query_granted("rrat", auth::recursive_role_query::yes).get0(),
                (std::unordered_set<sstring>{"chef", "rrat"}));

        m->revoke("rrat", "chef").get0();
        BOOST_REQUIRE_EQUAL(
                m->query_granted("rrat", auth::recursive_role_query::yes).get0(),
                std::unordered_set<sstring>{"rrat"});

        // A non-existing role cannot be revoked.
        BOOST_REQUIRE_THROW(m->revoke("rrat", "taster").get0(), auth::nonexistant_role);

        // A role cannot be revoked from a non-existing role.
        BOOST_REQUIRE_THROW(m->revoke("ccasper", "chef").get0(), auth::nonexistant_role);

        // Revoking a role not granted is an error.
        BOOST_REQUIRE_THROW(m->revoke("rrat", "sous_chef").get0(), auth::revoke_ungranted_role);
    });
}

SEASTAR_TEST_CASE(alter_role) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_manager(env);
        m->start().get0();

        const auto anon = auth::authenticated_user();

        auth::role_config tsmith_config;
        tsmith_config.is_superuser = true;
        tsmith_config.can_login = true;
        m->create("tsmith", tsmith_config).get0();

        auth::role_config_update u;
        u.can_login = false;

        m->alter("tsmith", u).get0();

        BOOST_REQUIRE_EQUAL(m->is_superuser("tsmith").get0(), true);
        BOOST_REQUIRE_EQUAL(m->can_login("tsmith").get0(), false);

        // Altering a non-existing role is an error.
        BOOST_REQUIRE_THROW(m->alter("hjones", u).get0(), auth::nonexistant_role);
    });
}

namespace {

const auto default_query_template = fmt::format(
        "ldap://localhost:{}/{}?cn?sub?(uniqueMember=uid={{USER}},ou=People,dc=example,dc=com)",
        ldap_port, base_dn);

const auto flaky_server_query_template = fmt::format(
        "ldap://localhost:{}/{}?cn?sub?(uniqueMember=uid={{USER}},ou=People,dc=example,dc=com)",
        std::stoi(ldap_port) + 2, base_dn);

auto make_ldap_manager(cql_test_env& env, sstring query_template = default_query_template) {
    auto stop_role_manager = [] (auth::ldap_role_manager* m) {
        m->stop().get();
        std::default_delete<auth::ldap_role_manager>()(m);
    };
    return std::unique_ptr<auth::ldap_role_manager, decltype(stop_role_manager)>(
            new auth::ldap_role_manager(query_template, /*target_attr=*/"cn", manager_dn, manager_password,
                                        env.local_qp(), service::get_local_migration_manager()),
            std::move(stop_role_manager));
}

void create_ldap_roles(const auth::role_manager& rmgr) {
    rmgr.create("jsmith", auth::role_config()).get();
    rmgr.create("role1", auth::role_config()).get();
    rmgr.create("role2", auth::role_config()).get();
}

} // anonymous namespace

using auth::role_set;

SEASTAR_TEST_CASE(ldap_single_role) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get0();
        create_ldap_roles(*m);
        const role_set expected{"jsmith", "role1"};
        BOOST_REQUIRE_EQUAL(expected, m->query_granted("jsmith", auth::recursive_role_query::no).get0());
    });
}

SEASTAR_TEST_CASE(ldap_two_roles) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get0();
        create_ldap_roles(*m);
        const role_set expected{"cassandra", "role1","role2"};
        BOOST_REQUIRE_EQUAL(expected, m->query_granted("cassandra", auth::recursive_role_query::no).get0());
    });
}

SEASTAR_TEST_CASE(ldap_no_roles) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get0();
        create_ldap_roles(*m);
        BOOST_REQUIRE_EQUAL(role_set{"dontexist"},
                            m->query_granted("dontexist", auth::recursive_role_query::no).get0());
    });
}

SEASTAR_TEST_CASE(ldap_wrong_role) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get0();
        create_ldap_roles(*m);
        BOOST_REQUIRE_EQUAL(role_set{"jdoe"}, m->query_granted("jdoe", auth::recursive_role_query::no).get0());
    });
}

SEASTAR_TEST_CASE(ldap_reconnect) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env, flaky_server_query_template);
        m->start().get0();
        create_ldap_roles(*m);
        std::vector<future<role_set>> queries;
        constexpr int noq = 1000;
        queries.reserve(noq);
        for (int i = 0; i < noq; ++i) {
            queries.push_back(m->query_granted("jsmith", auth::recursive_role_query::no)
                              .handle_exception([] (std::exception_ptr) { return role_set{}; }));
        }
        when_all(queries.begin(), queries.end()).get();
    });
}

using exception_predicate::message_contains;

SEASTAR_TEST_CASE(ldap_wrong_url) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env, "wrong:/UR?L");
        BOOST_REQUIRE_EXCEPTION(m->start().get0(), std::runtime_error, message_contains("server address"));
    });
}

SEASTAR_TEST_CASE(ldap_wrong_server_name) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env, "ldap://server.that.will.never.exist.scylladb.com");
        m->start().get0();
        BOOST_REQUIRE_EXCEPTION(m->query_granted("jdoe", auth::recursive_role_query::no).get0(),
                                std::runtime_error, message_contains("reconnect fail"));
    });
}

SEASTAR_TEST_CASE(ldap_wrong_port) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env, "ldap://localhost:2");
        m->start().get0();
        BOOST_REQUIRE_EXCEPTION(m->query_granted("jdoe", auth::recursive_role_query::no).get0(),
                                std::runtime_error, message_contains("reconnect fail"));
    });
}

SEASTAR_TEST_CASE(ldap_qualified_name) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        const sstring name(make_ldap_manager(env)->qualified_java_name());
        static const sstring suffix = "LDAPRoleManager";
        BOOST_REQUIRE_EQUAL(name.find(suffix), name.size() - suffix.size());
    });
}

SEASTAR_TEST_CASE(ldap_delegates_drop) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get0();
        create_ldap_roles(*m);
        BOOST_REQUIRE(m->exists("role1").get0());
        m->drop("role1").get();
        BOOST_REQUIRE(!m->exists("role1").get0());
    });
}

SEASTAR_TEST_CASE(ldap_delegates_query_all) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get0();
        create_ldap_roles(*m);
        const auto roles = m->query_all().get0();
        BOOST_REQUIRE_EQUAL(1, roles.count("role1"));
        BOOST_REQUIRE_EQUAL(1, roles.count("role2"));
        BOOST_REQUIRE_EQUAL(1, roles.count("jsmith"));
    });
}

SEASTAR_TEST_CASE(ldap_delegates_config) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get0();
        m->create("super", auth::role_config{/*is_superuser=*/true, /*can_login=*/false}).get();
        BOOST_REQUIRE(m->is_superuser("super").get0());
        BOOST_REQUIRE(!m->can_login("super").get0());
        m->create("user", auth::role_config{/*is_superuser=*/false, /*can_login=*/true}).get();
        BOOST_REQUIRE(!m->is_superuser("user").get0());
        BOOST_REQUIRE(m->can_login("user").get0());
        m->alter("super", auth::role_config_update{/*is_superuser=*/true, /*can_login=*/true}).get();
        BOOST_REQUIRE(m->can_login("super").get0());
    });
}

SEASTAR_TEST_CASE(ldap_delegates_attributes) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get0();
        m->create("r", auth::role_config{}).get();
        BOOST_REQUIRE(!m->get_attribute("r", "a").get0());
        m->set_attribute("r", "a", "3").get();
        // TODO: uncomment when failure is fixed.
        //BOOST_REQUIRE_EQUAL("3", *m->get_attribute("r", "a").get0());
        m->remove_attribute("r", "a").get();
        BOOST_REQUIRE(!m->get_attribute("r", "a").get0());
    });
}

using exceptions::invalid_request_exception;

SEASTAR_TEST_CASE(ldap_forbids_grant) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get0();
        BOOST_REQUIRE_EXCEPTION(m->grant("a", "b").get(), invalid_request_exception,
                                message_contains("with LDAPRoleManager."));
    });
}

SEASTAR_TEST_CASE(ldap_forbids_revoke) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get0();
        BOOST_REQUIRE_EXCEPTION(m->revoke("a", "b").get(), invalid_request_exception,
                                message_contains("with LDAPRoleManager."));
    });
}

namespace {

shared_ptr<db::config> make_ldap_config() {
    auto p = make_shared<db::config>();
    p->role_manager("com.scylladb.auth.LDAPRoleManager");
    p->authenticator(sstring(auth::password_authenticator_name));
    p->ldap_url_template(default_query_template);
    p->ldap_attr_role("cn");
    p->ldap_bind_dn(manager_dn);
    p->ldap_bind_passwd(manager_password);
    return p;
}

} // anonymous namespace

SEASTAR_TEST_CASE(ldap_config) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        const auto& svc = env.local_auth_service();
        BOOST_REQUIRE_EQUAL(role_set{"cassandra"}, svc.get_roles("cassandra").get0());
        auth::create_role(svc, "role1", auth::role_config{}, auth::authentication_options{}).get();
        const role_set expected{"cassandra", "role1"};
        BOOST_REQUIRE_EQUAL(expected, svc.get_roles("cassandra").get0());
    },
        make_ldap_config());
}
