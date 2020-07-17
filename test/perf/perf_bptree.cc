/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <algorithm>
#include <vector>
#include <random>
#include <fmt/core.h>
#include "perf.hh"

using per_key_t = int64_t;

struct key_compare {
    bool operator()(const per_key_t& a, const per_key_t& b) const noexcept { return a < b; }
};

#include "utils/bptree.hh"

using namespace bplus;
using namespace seastar;

constexpr int TEST_NODE_SIZE = 4;

/* On node size 32 (this test) linear search works better */
using test_tree = tree<per_key_t, unsigned long, key_compare, TEST_NODE_SIZE, key_search::linear>;

class collection_tester {
public:
    virtual void insert(per_key_t k) = 0;
    virtual void lower_bound(per_key_t k) = 0;
    virtual void erase(per_key_t k) = 0;
    virtual void drain(int batch) = 0;
    virtual void show_stats() = 0;
    virtual ~collection_tester() {};
};

class bptree_tester : public collection_tester {
    test_tree _t;
public:
    bptree_tester() : _t(key_compare{}) {}
    virtual void insert(per_key_t k) override { _t.emplace(k, 0); }
    virtual void lower_bound(per_key_t k) override {
        auto i = _t.lower_bound(k);
        assert(i != _t.end());
    }
    virtual void erase(per_key_t k) override { _t.erase(k); }
    virtual void drain(int batch) override {
        int x = 0;
        auto i = _t.begin();
        while (i != _t.end()) {
            i = i.erase(key_compare{});
            if (++x % batch == 0) {
                seastar::thread::yield();
            }
        }
    }
    virtual void show_stats() {
        struct bplus::stats st = _t.get_stats();
        fmt::print("nodes:     {}\n", st.nodes);
        for (int i = 0; i < (int)st.nodes_filled.size(); i++) {
            fmt::print("   {}: {} ({}%)\n", i, st.nodes_filled[i], st.nodes_filled[i] * 100 / st.nodes);
        }
        fmt::print("leaves:    {}\n", st.leaves);
        for (int i = 0; i < (int)st.leaves_filled.size(); i++) {
            fmt::print("   {}: {} ({}%)\n", i, st.leaves_filled[i], st.leaves_filled[i] * 100 / st.leaves);
        }
        fmt::print("datas:     {}\n", st.datas);
    }
    virtual ~bptree_tester() {
        _t.clear();
    }
};

class set_tester : public collection_tester {
    std::set<per_key_t> _s;
public:
    virtual void insert(per_key_t k) override { _s.insert(k); }
    virtual void lower_bound(per_key_t k) override {
        auto i = _s.lower_bound(k);
        assert(i != _s.end());
    }
    virtual void erase(per_key_t k) override { _s.erase(k); }
    virtual void drain(int batch) override {
        int x = 0;
        auto i = _s.begin();
        while (i != _s.end()) {
            i = _s.erase(i);
            if (++x % batch == 0) {
                seastar::thread::yield();
            }
        }
    }
    virtual void show_stats() { }
    virtual ~set_tester() = default;
};

class map_tester : public collection_tester {
    std::map<per_key_t, unsigned long> _m;
public:
    virtual void insert(per_key_t k) override { _m[k] = 0; }
    virtual void lower_bound(per_key_t k) override {
        auto i = _m.lower_bound(k);
        assert(i != _m.end());
    }
    virtual void erase(per_key_t k) override { _m.erase(k); }
    virtual void drain(int batch) override {
        int x = 0;
        auto i = _m.begin();
        while (i != _m.end()) {
            i = _m.erase(i);
            if (++x % batch == 0) {
                seastar::thread::yield();
            }
        }
    }
    virtual void show_stats() { }
    virtual ~map_tester() = default;
};

int main(int argc, char **argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("count", bpo::value<int>()->default_value(5000000), "number of keys to fill the tree with")
        ("batch", bpo::value<int>()->default_value(50), "number of operations between deferring points")
        ("iters", bpo::value<int>()->default_value(1), "number of iterations")
        ("col", bpo::value<std::string>()->default_value("bptree"), "collection to test")
        ("test", bpo::value<std::string>()->default_value("erase"), "what to test (erase, drain, find)")
        ("stats", bpo::value<bool>()->default_value(false), "show stats");

    return app.run(argc, argv, [&app] {
        auto count = app.configuration()["count"].as<int>();
        auto iters = app.configuration()["iters"].as<int>();
        auto batch = app.configuration()["batch"].as<int>();
        auto col = app.configuration()["col"].as<std::string>();
        auto tst = app.configuration()["test"].as<std::string>();
        auto stats = app.configuration()["stats"].as<bool>();

        return seastar::async([count, iters, batch, col, tst, stats] {
            std::unique_ptr<collection_tester> c;

            if (col == "bptree") {
                c = std::make_unique<bptree_tester>();
            } else if (col == "set") {
                c = std::make_unique<set_tester>();
            } else if (col == "map") {
                c = std::make_unique<map_tester>();
            } else {
                fmt::print("Unknown collection\n");
                return;
            }

            std::vector<per_key_t> keys;

            for (per_key_t i = 0; i < count; i++) {
                keys.push_back(i + 1);
            }

            std::random_device rd;
            std::mt19937 g(rd());

            fmt::print("Inserting {:d} k:v pairs into {} {:d} times\n", count, col, iters);

            for (auto rep = 0; rep < iters; rep++) {
                std::shuffle(keys.begin(), keys.end(), g);
                seastar::thread::yield();

                auto d = duration_in_seconds([&] {
                    for (int i = 0; i < count; i++) {
                        c->insert(keys[i]);
                        if ((i + 1) % batch == 0) {
                            seastar::thread::yield();
                        }
                    }
                });

                fmt::print("fill: {:.6f} ms\n", d.count() * 1000);

                if (stats) {
                    c->show_stats();
                }

                if (tst == "erase") {
                    std::shuffle(keys.begin(), keys.end(), g);
                    seastar::thread::yield();

                    d = duration_in_seconds([&] {
                        for (int i = 0; i < count; i++) {
                            c->erase(keys[i]);
                            if ((i + 1) % batch == 0) {
                                seastar::thread::yield();
                            }
                        }
                    });

                    fmt::print("erase: {:.6f} ms\n", d.count() * 1000);
                } else if (tst == "drain") {
                    d = duration_in_seconds([&] {
                        c->drain(batch);
                    });

                    fmt::print("drain: {:.6f} ms\n", d.count() * 1000);
                } else if (tst == "find") {
                    std::shuffle(keys.begin(), keys.end(), g);
                    seastar::thread::yield();

                    d = duration_in_seconds([&] {
                        for (int i = 0; i < count; i++) {
                            c->lower_bound(keys[i]);
                            if ((i + 1) % batch == 0) {
                                seastar::thread::yield();
                            }
                        }
                    });

                    fmt::print("find: {:.6f} ms\n", d.count() * 1000);
                }
            }
        });
    });
}
