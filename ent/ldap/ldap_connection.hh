/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */


#pragma once

#include <ldap.h>
#include <memory>
#include <unordered_map>

#include <seastar/core/iostream.hh>
#include <seastar/core/posix.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/api.hh>

/// Functor to invoke ldap_msgfree.
struct ldap_msg_deleter {
    void operator()(LDAPMessage* p) {
        ldap_msgfree(p);
    }
};

using ldap_msg_ptr = std::unique_ptr<LDAPMessage, ldap_msg_deleter>;

/// A connection to an LDAP server with custom networking over a Seastar socket.  Constructor takes
/// a connected socket and generates an LDAP structure hooked up to it.  The LDAP object is obtained
/// using get_ldap(); its custom networking is valid as long as its ldap_connection host is alive.
class ldap_connection {
    seastar::file_desc _fd; ///< Libldap polls this to determine if we're ready for reading/writing.
    seastar::connected_socket _socket;
    seastar::input_stream<char> _input_stream; ///< _socket's input.
    seastar::output_stream<char> _output_stream; ///< _socket's output.
    seastar::temporary_buffer<char> _read_buffer; ///< Everything read by Seastar but not yet consumed by Sockbuf_IO.
    static std::mutex _global_init_mutex; ///< Mutex for protecting global ldap data initialization for older ldap libs -
                                         /// ref:https://github.com/openldap/openldap/commit/877faea723ecc5721291b0b2f53e34f7921e0f7c
    enum class status {
        // Lowercase, to avoid inadvertently invoking macros:
        up,             ///< Connected, operating normally.
        down,           ///< Shut down.
        eof,            ///< Read encountered EOF, write should be OK.
        err             ///< IO error encountered.
    };
    status _status; ///< When not OK, all \c read() and \c write() calls will immediately return without action.
    seastar::future<> _read_consumer; ///< Consumes Seastar read data.
    bool _read_in_progress; ///< Is there a Seastar read in progress?
    seastar::future<> _outstanding_write; ///< Captures Seastar write continuation.
    /// When LDAP yields a result for one of these msgids, forward it to the corresponding promise:
    std::unordered_map<int, seastar::promise<ldap_msg_ptr>> _msgid_to_promise;
    bool _currently_polling; ///< True iff poll_results() is in progress.

    /// Deallocates an LDAP structure.
    struct ldap_deleter {
        void operator()(LDAP*);
    };
    std::unique_ptr<LDAP, ldap_deleter> _ldap;

  public:
    /// Creates LDAP with custom Seastar networking.
    ldap_connection(seastar::connected_socket&& socket);

    /// A pointer to the LDAP customized with Seastar IO.  *this keeps ownership.
    ///
    /// \warning Do not call ldap_result() on this object, nor any ldap_* operations that generate
    /// network traffic.  This includes at least:
    /// - ldap_abandon
    /// - ldap_add
    /// - ldap_bind
    /// - ldap_compare
    /// - ldap_delete
    /// - ldap_extended_operation
    /// - ldap_modify
    /// - ldap_rename
    /// - ldap_search
    /// - ldap_unbind()
    LDAP* get_ldap() const { return _ldap.get(); }

    /// Before destroying *this, user must wait on the future returned:
    seastar::future<> close();

    /// Performs LDAP simple bind operation.  See man ldap_bind.
    seastar::future<ldap_msg_ptr> simple_bind(const char *who, const char *passwd);

    /// Performs LDAP search operation.  See man ldap_search.
    seastar::future<ldap_msg_ptr> search(
              char *base,
              int scope,
              char *filter,
              char *attrs[],
              int attrsonly,
              LDAPControl **serverctrls,
              LDAPControl **clientctrls,
              struct timeval *timeout,
              int sizelimit);

    /// The last error reported by an LDAP operation.
    seastar::sstring get_error() const;

    /// Cannot be moved, since it spawns continuations that capture \c this.
    ldap_connection(ldap_connection&&) = delete;

  private:
    // Sockbuf_IO functionality (see Sockbuf_IO manpage):
    static int sbi_ctrl(Sockbuf_IO_Desc* sid, int option, void* value) noexcept;
    static ber_slen_t sbi_read(Sockbuf_IO_Desc* sid, void* buffer, ber_len_t size) noexcept;
    static ber_slen_t sbi_write(Sockbuf_IO_Desc* sid, void* buffer, ber_len_t size) noexcept;
    static int sbi_close(Sockbuf_IO_Desc* sid) noexcept;
    static const Sockbuf_IO seastar_sbio;

    /// Efficiently waits for ldap_result of msgid, returning the result object in the future.  If
    /// an error occurs at any point, the future will be exceptional.
    ///
    /// \warning You must call await_result() immediately after obtaining msgid from an ldap_*
    /// function, without yielding to Seastar in between.  Otherwise, the result may be dropped.
    seastar::future<ldap_msg_ptr> await_result(int msgid);

    /// If status is LDAP_SUCCESS, returns await_result(msgid).  Otherwise, returns an exceptional
    /// future with the error report.
    seastar::future<ldap_msg_ptr> await_result(int status, int msgid);

    /// Schedules a Seastar write of (copied) b[:size].  On success, returns size.  On failure,
    /// returns LDAP's failure code.
    ber_slen_t write(char const* b, ber_len_t size);

    /// Consumes at most \p size bytes from what Seastar has read so far and writes them to b.  On
    /// success, returns the number of bytes so consumed (possibly less than \p size).  On failure,
    /// returns LDAP's failure code.
    ber_slen_t read(char* b, ber_len_t size);

    /// Shuts down all internal state that can be shut down immediately.  See also close().
    void shutdown();

    /// Initiates a Seastar read that will procure data for \c read() to consume.  Data consumption
    /// will happen in a future captured by _read_consumer.  If _read_consumer is currently active,
    /// however (because the previous Seastar read hasn't been consumed yet), this method does
    /// nothing.
    void read_ahead();

    /// Invokes ldap_result for all elements of _msgid_to_promise.  For every ready result, fulfills
    /// its promise and removes it from _msgid_to_promise.
    void poll_results();

    /// Sets _status to \p s.  If s != status::up, sends an exception to each _msgid_to_promise
    /// element, then clears _msgid_to_promise.
    void set_status(ldap_connection::status s);
};
