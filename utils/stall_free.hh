/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <list>
#include <algorithm>
#include <seastar/core/thread.hh>
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include "utils/collection-concepts.hh"

namespace utils {


// Similar to std::merge but it does not stall. Must run inside a seastar
// thread. It merges items from list2 into list1. Items from list2 can only be copied.
template<class T, class Compare>
SEASTAR_CONCEPT(requires LessComparable<T, T, Compare>)
void merge_to_gently(std::list<T>& list1, const std::list<T>& list2, Compare comp) {
    auto first1 = list1.begin();
    auto first2 = list2.begin();
    auto last1 = list1.end();
    auto last2 = list2.end();
    while (first2 != last2) {
        seastar::thread::maybe_yield();
        if (first1 == last1) {
            // Copy remaining items of list2 into list1
            std::copy_if(first2, last2, std::back_inserter(list1), [] (const auto&) { return true; });
            return;
        }
        if (comp(*first2, *first1)) {
            first1 = list1.insert(first1, *first2);
            ++first2;
        } else {
            ++first1;
        }
    }
}

template<class T>
seastar::future<> clear_gently(std::list<T>& list) {
    return repeat([&list] () mutable {
        if (list.empty()) {
            return seastar::stop_iteration::yes;
        }
        list.pop_back();
        return seastar::stop_iteration::no;
    });
}

}

