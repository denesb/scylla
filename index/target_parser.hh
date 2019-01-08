/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2017 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "cql3/statements/index_target.hh"

#include <boost/algorithm/string/predicate.hpp>

#include <regex>

namespace secondary_index {

struct target_parser {
    static std::pair<const column_definition*, cql3::statements::index_target::target_type>
    parse(schema_ptr schema, const index_metadata& im)
    {
        sstring target = im.options().at(cql3::statements::index_target::target_option_name);
        auto result = parse(schema, target);
        if (!result) {
            throw exceptions::configuration_exception(format("Unable to parse targets for index {} ({})", im.name(), target));
        }
        return *result;
    }

    static std::optional<std::pair<const column_definition*, cql3::statements::index_target::target_type>>
    parse(schema_ptr schema, const sstring& target)
    {
        using namespace cql3::statements;
        // if the regex matches then the target is in the form "keys(foo)", "entries(bar)" etc
        // if not, then it must be a simple column name and implictly its type is VALUES
        sstring column_name;
        index_target::target_type target_type;
        static const std::regex target_regex("^(keys|entries|values|full)\\((.+)\\)$");
        std::cmatch match;
        if (std::regex_match(target.data(), match, target_regex)) {
            target_type = index_target::from_sstring(match[1].str());
            column_name = match[2].str();
        } else {
            column_name = target;
            target_type = index_target::target_type::values;
        }

        auto column = schema->get_column_definition(utf8_type->decompose(column_name));
        if (!column) {
            return std::nullopt;
        }
        return std::make_pair(column, target_type);
    }
};

}
