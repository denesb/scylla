/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

/* Copyright 2020 ScyllaDB */

#include "util.hh"

#ifdef DEBUG

#include <ucontext.h>

extern "C" {
void __sanitizer_start_switch_fiber(void** fake_stack_save, const void* stack_bottom, size_t stack_size);
void __sanitizer_finish_switch_fiber(void* fake_stack_save, const void** stack_bottom_old, size_t* stack_size_old);
}

#endif

namespace cql3::util {

static void do_with_parser_impl_impl(const sstring_view& cql, noncopyable_function<void (cql3_parser::CqlParser& parser)> f) {
    cql3_parser::CqlLexer::collector_type lexer_error_collector(cql);
    cql3_parser::CqlParser::collector_type parser_error_collector(cql);
    cql3_parser::CqlLexer::InputStreamType input{reinterpret_cast<const ANTLR_UINT8*>(cql.begin()), ANTLR_ENC_UTF8, static_cast<ANTLR_UINT32>(cql.size()), nullptr};
    cql3_parser::CqlLexer lexer{&input};
    lexer.set_error_listener(lexer_error_collector);
    cql3_parser::CqlParser::TokenStreamType tstream(ANTLR_SIZE_HINT, lexer.get_tokSource());
    cql3_parser::CqlParser parser{&tstream};
    parser.set_error_listener(parser_error_collector);
    f(parser);
}

#ifndef DEBUG

void do_with_parser_impl(const sstring_view& cql, noncopyable_function<void (cql3_parser::CqlParser& parser)> f) {
    return do_with_parser_impl_impl(cql, std::move(f));
}

#else

// The CQL parser uses huge amounts of stack space in debug mode,
// enough to overflow our 128k stacks. The mechanism below runs
// the parser in a larger stack.

struct thunk_args {
    // arguments to do_with_parser_impl_impl
    const sstring_view& cql;
    noncopyable_function<void (cql3_parser::CqlParser&)>&& func;
    // Exceptions can't be returned from another stack, so store
    // any thrown exception here
    std::exception_ptr ex;
    // Caller's stack
    ucontext_t caller_stack;
    // Address Sanitizer needs some extra storage for stack switches.
    struct {
        void* fake_stack;
        const void* stack_bottom;
        size_t stack_size;
    } sanitizer_state;
};

// Translate from makecontext(3)'s strange calling convention
// to do_with_parser_impl_impl().
static void thunk(int p1, int p2) {
    auto p = uint32_t(p1) | (uint64_t(uint32_t(p2)) << 32);
    auto args = reinterpret_cast<thunk_args*>(p);
    auto& san = args->sanitizer_state;
    // Complete stack switch started in do_with_parser_impl()
    __sanitizer_finish_switch_fiber(nullptr, &san.stack_bottom, &san.stack_size);
    try {
        do_with_parser_impl_impl(args->cql, std::move(args->func));
    } catch (...) {
        args->ex = std::current_exception();
    }
    // Switch back to original stack
    __sanitizer_start_switch_fiber(nullptr, san.stack_bottom, san.stack_size);
    setcontext(&args->caller_stack);
};

void do_with_parser_impl(const sstring_view& cql, noncopyable_function<void (cql3_parser::CqlParser& parser)> f) {
    static constexpr size_t stack_size = 1 << 20;
    static thread_local std::unique_ptr<char[]> stack = std::make_unique<char[]>(stack_size);
    thunk_args args{
        .cql = cql,
        .func = std::move(f),
    };
    ucontext_t uc;
    auto r = getcontext(&uc);
    assert(r == 0);
    uc.uc_stack.ss_sp = stack.get();
    uc.uc_stack.ss_size = stack_size;
    uc.uc_link = nullptr;
    auto q = reinterpret_cast<uint64_t>(reinterpret_cast<uintptr_t>(&args));
    makecontext(&uc, reinterpret_cast<void (*)()>(thunk), 2, int(q), int(q >> 32));
    auto& san = args.sanitizer_state;
    // Tell Address Sanitizer we are switching to another stack
    __sanitizer_start_switch_fiber(&san.fake_stack, stack.get(), stack_size);
    swapcontext(&args.caller_stack, &uc);
    // Completes stack switch started in thunk()
    __sanitizer_finish_switch_fiber(san.fake_stack, nullptr, 0);
    if (args.ex) {
        std::rethrow_exception(std::move(args.ex));
    }
}

#endif


}
