/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/sstring.hh>

#include "cql3/statements/service_level_statement.hh"
#include "cql3/statements/sl_prop_defs.hh"
#include "service/qos/qos_common.hh"

namespace cql3 {
namespace statements {

class create_service_level_statement final : public service_level_statement {
    sstring _service_level;
    qos::service_level_options _slo;
    bool _if_not_exists;

public:
    create_service_level_statement(sstring service_level, shared_ptr<sl_prop_defs> attrs, bool if_not_exists);
    void validate(service::storage_proxy&, const service::client_state&) override;
    virtual future<> check_access(const service::client_state&) override;
    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(service::storage_proxy&, service::query_state&, const query_options&) override;
};

}

}
