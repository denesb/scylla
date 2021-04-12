/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <seastar/testing/thread_test_case.hh>
#include "utils/stall_free.hh"

SEASTAR_THREAD_TEST_CASE(test_merge1) {
    std::list<int> l1{1, 2, 5, 8};
    std::list<int> l2{3};
    std::list<int> expected{1,2,3,5,8};
    utils::merge_to_gently(l1, l2, std::less<int>());
    BOOST_CHECK(l1 == expected);
}

SEASTAR_THREAD_TEST_CASE(test_merge2) {
    std::list<int> l1{1};
    std::list<int> l2{3, 5, 6};
    std::list<int> expected{1,3,5,6};
    utils::merge_to_gently(l1, l2, std::less<int>());
    BOOST_CHECK(l1 == expected);
}

SEASTAR_THREAD_TEST_CASE(test_merge3) {
    std::list<int> l1{};
    std::list<int> l2{3, 5, 6};
    std::list<int> expected{3,5,6};
    utils::merge_to_gently(l1, l2, std::less<int>());
    BOOST_CHECK(l1 == expected);
}

SEASTAR_THREAD_TEST_CASE(test_merge4) {
    std::list<int> l1{1};
    std::list<int> l2{};
    std::list<int> expected{1};
    utils::merge_to_gently(l1, l2, std::less<int>());
    BOOST_CHECK(l1 == expected);
}
