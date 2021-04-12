/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <vector>
#include <map>
#include <boost/range/numeric.hpp>
#include "sstables/compaction_strategy_impl.hh"
#include "sstables/sstable_set.hh"
#include "sstables/compaction.hh"
#include "database.hh"

namespace sstables {

// Not suitable for production, its sole purpose is testing.

class sstable_run_based_compaction_strategy_for_tests : public compaction_strategy_impl {
    static constexpr size_t static_fragment_size_for_run = 1024*1024;
public:
    sstable_run_based_compaction_strategy_for_tests();

    virtual compaction_descriptor get_sstables_for_compaction(column_family& cf, std::vector<sstables::shared_sstable> uncompacting_sstables) override;

    virtual int64_t estimated_pending_compactions(column_family& cf) const override;

    virtual compaction_strategy_type type() const;

    virtual compaction_backlog_tracker& get_backlog_tracker() override;
};

}
