/*
 * Copyright (C) 2025-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "cql3/statements/create_nonmaterialized_view_statement.hh"

#include <boost/regex.hpp>
#include <vector>
#include <unordered_set>

#include "cql3/column_identifier.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "exceptions/exceptions.hh"
#include "utils/assert.hh"

#include "cql3/statements/prepared_statement.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/util.hh"
#include "schema/schema_builder.hh"
#include "service/storage_proxy.hh"
#include "validation.hh"
#include "data_dictionary/data_dictionary.hh"
#include "service/migration_manager.hh"
#include "replica/database.hh"

static logging::logger nmvlogger("non_materialized_view");

namespace cql3 {

namespace statements {

create_nonmaterialized_view_statement::create_nonmaterialized_view_statement(
        cf_name view_name,
        cf_name base_name,
        std::vector<::shared_ptr<selection::raw_selector>> select_clause,
        expr::expression where_clause,
        bool if_not_exists)
    : schema_altering_statement{view_name}
    , _base_name{base_name}
    , _select_clause{select_clause}
    , _where_clause{where_clause}
    , _if_not_exists{if_not_exists}
{
}

future<> create_nonmaterialized_view_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return state.has_column_family_access(keyspace(), _base_name.get_column_family(), auth::permission::ALTER);
}

static const column_definition* get_column_definition(const schema& schema, column_identifier::raw& identifier) {
    auto prepared = identifier.prepare(schema);
    SCYLLA_ASSERT(dynamic_pointer_cast<column_identifier>(prepared));
    auto id = static_pointer_cast<column_identifier>(prepared);
    return schema.get_column_definition(id->name());
}

std::pair<view_ptr, cql3::cql_warnings_vec> create_nonmaterialized_view_statement::prepare_non_materialized_view(data_dictionary::database db) const {
    // We need to make sure that:
    //  - materialized view name is valid
    //  - primary key includes all columns in base table's primary key
    //  - make sure that the select statement does not have anything other than columns
    //    and their names match the base table's names
    //  - make sure that primary key does not include any collections
    //  - make sure there is no where clause in the select statement
    //  - make sure there is not currently a table or view
    //  - make sure base_table gc_grace_seconds > 0

    cql3::cql_warnings_vec warnings;

    if (!_base_name.has_keyspace()) {
        _base_name.set_keyspace(keyspace(), true);
    }

    const sstring& cf_name = column_family();
    boost::regex name_regex("\\w+");
    if (!boost::regex_match(std::string(cf_name), name_regex)) {
        throw exceptions::invalid_request_exception(format("\"{}\" is not a valid view name (must contain alphanumeric character only: [0-9A-Za-z] or the underscore)", cf_name.c_str()));
    }
    if (cf_name.size() > size_t(schema::NAME_LENGTH)) {
        throw exceptions::invalid_request_exception(format("View names shouldn't be more than {:d} characters long (got \"{}\")", schema::NAME_LENGTH, cf_name.c_str()));
    }

    schema_ptr schema = validation::validate_column_family(db, _base_name.get_keyspace(), _base_name.get_column_family());
    if (schema->is_counter()) {
        throw exceptions::invalid_request_exception(format("Views are not supported on counter tables"));
    }

    if (db.get_cdc_base_table(*schema)) {
        throw exceptions::invalid_request_exception(format("Views cannot be created on CDC Log tables"));
    }

    // Gather all included columns, as specified by the select clause
    auto included = _select_clause | std::views::transform([&](auto&& selector) {
        if (selector->alias) {
            throw exceptions::invalid_request_exception(format("Cannot use alias when defining a view"));
        }

        auto& selectable = selector->selectable_;
        shared_ptr<column_identifier::raw> identifier;
        expr::visit(overloaded_functor{
            [&] (const expr::unresolved_identifier& ui) { identifier = ui.ident; },
            [] (const auto& default_case) -> void { throw exceptions::invalid_request_exception(format("Cannot use general expressions when defining a view")); },
        }, selectable);

        auto* def = get_column_definition(*schema, *identifier);
        if (!def) {
            throw exceptions::invalid_request_exception(format("Unknown column name detected in CREATE VIEW statement: {}", identifier));
        }
        return def;
    }) | std::ranges::to<std::unordered_set<const column_definition*>>();

    // TODO: Support ordering/limit
    auto parameters = make_lw_shared<raw::select_statement::parameters>(raw::select_statement::parameters::orderings_type(), false, true);
    // TODO: Support group by
    raw::select_statement raw_select(_base_name, std::move(parameters), _select_clause, _where_clause, std::nullopt, std::nullopt, {}, std::make_unique<cql3::attributes::raw>());
    raw_select.prepare_keyspace(keyspace());
    // TODO: Add where bounds
    raw_select.set_bound_variables({});

    cql_stats ignored;
    auto prepared = raw_select.prepare(db, ignored, true);
    // TODO : Ignore restrictions about primary keys
    auto restrictions = static_pointer_cast<statements::select_statement>(prepared->statement)->get_restrictions();

    // TODO: This is temporary, long term do not restrict anything
    const expr::single_column_restrictions_map& non_pk_restrictions = restrictions->get_non_pk_restriction();
    if (!non_pk_restrictions.empty()) {
        throw exceptions::invalid_request_exception(seastar::format("Non-primary key columns cannot be restricted in the SELECT statement used for view {} creation (got restrictions on: {})",
                column_family(),
                fmt::join(non_pk_restrictions | std::views::keys | std::views::transform(std::mem_fn(&column_definition::name_as_text)), ", ")));
    }

    schema_builder builder{keyspace(), column_family(), std::nullopt};
    if (included.empty()) {
        for (auto& cdef : schema->all_columns()) {
            builder.with_column(cdef.name(), cdef.type, cdef.kind);
        }
    } else {
        for (auto* cdef: included) {
            builder.with_column(cdef->name(), cdef->type, cdef->kind);
        }
    }

    auto where_clause_text = util::relations_to_where_clause(_where_clause);
    builder.with_nonmaterialized_view_info(schema, included.empty());

    return std::make_pair(view_ptr(builder.build()), std::move(warnings));
}

future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>>
create_nonmaterialized_view_statement::prepare_schema_mutations(query_processor& qp, const query_options&, api::timestamp_type ts) const {
    std::vector<mutation> m;
    auto [definition, warnings] = prepare_non_materialized_view(qp.db());
    try {
        m = co_await service::prepare_new_nonmaterialized_view_announcement(qp.proxy(), std::move(definition), ts);
    } catch (const exceptions::already_exists_exception& e) {
        if (!_if_not_exists) {
            co_return coroutine::exception(std::current_exception());
        }
    }

    co_return std::make_tuple(created_event(), std::move(m), std::move(warnings));
}

std::unique_ptr<cql3::statements::prepared_statement>
create_nonmaterialized_view_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    if (!_prepare_ctx.get_variable_specifications().empty()) {
        throw exceptions::invalid_request_exception(format("Cannot use query parameters in CREATE VIEW statements"));
    }
    return std::make_unique<prepared_statement>(audit_info(), make_shared<create_nonmaterialized_view_statement>(*this));
}

::shared_ptr<schema_altering_statement::event_t> create_nonmaterialized_view_statement::created_event() const {
    return make_shared<event_t>(
            event_t::change_type::CREATED,
            event_t::target_type::TABLE,
            keyspace(),
            column_family());
}

}

}
