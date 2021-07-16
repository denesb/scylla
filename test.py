#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 ScyllaDB
#

#
# This file is part of Scylla.
#
# See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
#
from abc import ABC, abstractmethod
import argparse
import asyncio
import colorama
import difflib
import filecmp
import glob
import io
import itertools
import logging
import multiprocessing
import os
import pathlib
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
import yaml
from random import randint

output_is_a_tty = sys.stdout.isatty()

LDAP_SERVER_CONFIGURATION_FILE = os.path.join(os.path.dirname(__file__), 'test', 'resource', 'slapd.conf')

DEFAULT_ENTRIES = [
    """dn: dc=example,dc=com
objectClass: dcObject
objectClass: organization
dc: example
o: Example
description: Example directory.
""",
    """dn: cn=root,dc=example,dc=com
objectClass: organizationalRole
cn: root
description: Directory manager.
""",
    """dn: ou=People,dc=example,dc=com
objectClass: organizationalUnit
ou: People
description: Our people.
""",
    """# Default superuser for Scylla
dn: uid=cassandra,ou=People,dc=example,dc=com
objectClass: organizationalPerson
objectClass: uidObject
cn: cassandra
ou: People
sn: cassandra
userid: cassandra
userPassword: cassandra
""",
    """dn: uid=jsmith,ou=People,dc=example,dc=com
objectClass: organizationalPerson
objectClass: uidObject
cn: Joe Smith
ou: People
sn: Smith
userid: jsmith
userPassword: joeisgreat
""",
    """dn: uid=jdoe,ou=People,dc=example,dc=com
objectClass: organizationalPerson
objectClass: uidObject
cn: John Doe
ou: People
sn: Doe
userid: jdoe
userPassword: pa55w0rd
""",
    """dn: cn=role1,dc=example,dc=com
objectClass: groupOfUniqueNames
cn: role1
uniqueMember: uid=jsmith,ou=People,dc=example,dc=com
uniqueMember: uid=cassandra,ou=People,dc=example,dc=com
""",
    """dn: cn=role2,dc=example,dc=com
objectClass: groupOfUniqueNames
cn: role2
uniqueMember: uid=cassandra,ou=People,dc=example,dc=com
""",
    """dn: cn=role3,dc=example,dc=com
objectClass: groupOfUniqueNames
cn: role3
uniqueMember: uid=jdoe,ou=People,dc=example,dc=com
""",
]

def create_formatter(*decorators):
    """Return a function which decorates its argument with the given
    color/style if stdout is a tty, and leaves intact otherwise."""
    def color(arg):
        return "".join(decorators) + str(arg) + colorama.Style.RESET_ALL

    def nocolor(arg):
        return str(arg)
    return color if output_is_a_tty else nocolor


class palette:
    """Color palette for formatting terminal output"""
    ok = create_formatter(colorama.Fore.GREEN, colorama.Style.BRIGHT)
    fail = create_formatter(colorama.Fore.RED, colorama.Style.BRIGHT)
    new = create_formatter(colorama.Fore.BLUE)
    skip = create_formatter(colorama.Style.DIM)
    path = create_formatter(colorama.Style.BRIGHT)
    diff_in = create_formatter(colorama.Fore.GREEN)
    diff_out = create_formatter(colorama.Fore.RED)
    diff_mark = create_formatter(colorama.Fore.MAGENTA)
    warn = create_formatter(colorama.Fore.YELLOW)
    crit = create_formatter(colorama.Fore.RED, colorama.Style.BRIGHT)


class TestSuite(ABC):
    """A test suite is a folder with tests of the same type.
    E.g. it can be unit tests, boost tests, or CQL tests."""

    # All existing test suites, one suite per path.
    suites = dict()
    _next_id = 0

    def __init__(self, path, cfg):
        self.path = path
        self.name = os.path.basename(self.path)
        self.cfg = cfg
        self.tests = []

    @property
    def next_id(self):
        TestSuite._next_id += 1
        return TestSuite._next_id

    @staticmethod
    def test_count():
        return TestSuite._next_id

    @staticmethod
    def load_cfg(path):
        with open(os.path.join(path, "suite.yaml"), "r") as cfg_file:
            cfg = yaml.safe_load(cfg_file.read())
            if not isinstance(cfg, dict):
                raise RuntimeError("Failed to load tests in {}: suite.yaml is empty".format(path))
            return cfg

    @staticmethod
    def opt_create(path):
        """Return a subclass of TestSuite with name cfg["type"].title + TestSuite.
        Ensures there is only one suite instance per path."""
        suite = TestSuite.suites.get(path)
        if not suite:
            cfg = TestSuite.load_cfg(path)
            kind = cfg.get("type")
            if kind is None:
                raise RuntimeError("Failed to load tests in {}: suite.yaml has no suite type".format(path))
            SpecificTestSuite = globals().get(kind.title() + "TestSuite")
            if not SpecificTestSuite:
                raise RuntimeError("Failed to load tests in {}: suite type '{}' not found".format(path, kind))
            suite = SpecificTestSuite(path, cfg)
            TestSuite.suites[path] = suite
        return suite

    @staticmethod
    def tests():
        return itertools.chain(*[suite.tests for suite in
                                 TestSuite.suites.values()])

    @property
    @abstractmethod
    def pattern(self):
        pass

    @abstractmethod
    def add_test(self, name, args, mode, options):
        pass

    def junit_tests(self):
        """Tests which participate in a consolidated junit report"""
        return self.tests

    def add_test_list(self, mode, options):
        lst = [ os.path.splitext(os.path.basename(t))[0] for t in glob.glob(os.path.join(self.path, self.pattern)) ]
        run_first_tests = set(self.cfg.get("run_first", []))
        if lst:
            # Some tests are long and are better to be started earlier,
            # so pop them up while sorting the list
            lst.sort(key=lambda x: (x not in run_first_tests, x))
        disable_tests = set(self.cfg.get("disable", []))
        skip_tests = set(self.cfg.get("skip_in_debug_mode", []))
        for shortname in lst:
            if shortname in disable_tests or (mode not in ["release", "dev"] and shortname in skip_tests):
                continue
            t = os.path.join(self.name, shortname)
            patterns = options.name if options.name else [t]
            if options.skip_pattern and options.skip_pattern in t:
                continue
            for p in patterns:
                if p in t:
                    for i in range(options.repeat):
                        self.add_test(shortname, mode, options)


class UnitTestSuite(TestSuite):
    """TestSuite instantiation for non-boost unit tests"""

    def __init__(self, path, cfg):
        super().__init__(path, cfg)
        # Map of custom test command line arguments, if configured
        self.custom_args = cfg.get("custom_args", {})

    def create_test(self, *args, **kwargs):
        return UnitTest(*args, **kwargs)

    def add_test(self, shortname, mode, options):
        """Create a UnitTest class with possibly custom command line
        arguments and add it to the list of tests"""

        # Default seastar arguments, if not provided in custom test options,
        # are two cores and 2G of RAM
        args = self.custom_args.get(shortname, ["-c2 -m2G"])
        for a in args:
            test = self.create_test(self.next_id, shortname, a, self, mode, options)
            self.tests.append(test)

    @property
    def pattern(self):
        return "*_test.cc"


class BoostTestSuite(UnitTestSuite):
    """TestSuite for boost unit tests"""

    def create_test(self, *args, **kwargs):
        return BoostTest(*args, **kwargs)

    def junit_tests(self):
        """Boost tests produce an own XML output, so are not included in a junit report"""
        return []

class LdapTestSuite(UnitTestSuite):
    """TestSuite for ldap unit tests"""

    def create_test(self, *args, **kwargs):
        return LdapTest(*args, **kwargs)

    def junit_tests(self):
        """Ldap tests produce an own XML output, so are not included in a junit report"""
        return []


class CqlTestSuite(TestSuite):
    """TestSuite for CQL tests"""

    def add_test(self, shortname, mode, options):
        """Create a CqlTest class and add it to the list"""
        test = CqlTest(self.next_id, shortname, self, mode, options)
        self.tests.append(test)

    @property
    def pattern(self):
        return "*_test.cql"

class RunTestSuite(TestSuite):
    """TestSuite for test directory with a 'run' script """

    def add_test(self, shortname, mode, options):
        test = RunTest(self.next_id, shortname, self, mode, options)
        self.tests.append(test)

    @property
    def pattern(self):
        return "run"


class Test:
    """Base class for CQL, Unit and Boost tests"""
    def __init__(self, test_no, shortname, suite, mode, options):
        self.id = test_no
        # Name with test suite name
        self.name = os.path.join(suite.name, shortname)
        # Name within the suite
        self.shortname = shortname
        self.mode = mode
        self.suite = suite
        # Unique file name, which is also readable by human, as filename prefix
        self.uname = "{}.{}".format(self.shortname, self.id)
        self.log_filename = os.path.join(options.tmpdir, self.mode, self.uname + ".log")
        self.success = None

    @abstractmethod
    async def run(self, options):
        pass

    @abstractmethod
    def print_summary(self):
        pass

    async def setup(self, port, options):
        """Performs any necessary setup steps before running a test.

Returns (fn, txt) where fn is a cleanup function to call unconditionally after the test stops running, and txt is failure-injection description."""
        return (lambda: 0, None)

    def check_log(self, trim):
        """Check and trim logs and xml output for tests which have it"""
        if trim:
            pathlib.Path(self.log_filename).unlink()
        pass


class UnitTest(Test):
    standard_args = shlex.split("--overprovisioned --unsafe-bypass-fsync 1 --blocked-reactor-notify-ms 2000000 --collectd 0"
                                " --max-networking-io-control-blocks=100")

    def __init__(self, test_no, shortname, args, suite, mode, options):
        super().__init__(test_no, shortname, suite, mode, options)
        self.path = os.path.join("build", self.mode, "test", self.name)
        self.args = shlex.split(args) + UnitTest.standard_args

    def print_summary(self):
        print("Output of {} {}:".format(self.path, " ".join(self.args)))
        print(read_log(self.log_filename))

    async def run(self, options):
        self.success = await run_test(self, options)
        logging.info("Test #%d %s", self.id, "succeeded" if self.success else "failed ")
        return self


class BoostTest(UnitTest):
    """A unit test which can produce its own XML output"""

    def __init__(self, test_no, shortname, args, suite, mode, options):
        super().__init__(test_no, shortname, args, suite, mode, options)
        boost_args = []
        self.xmlout = os.path.join(options.tmpdir, self.mode, "xml", self.uname + ".xunit.xml")
        boost_args += ['--report_level=no',
                       '--logger=HRF,test_suite:XML,test_suite,' + self.xmlout]
        boost_args += ['--catch_system_errors=no']  # causes undebuggable cores
        boost_args += ['--color_output={}'.format('true' if output_is_a_tty else 'false')]
        boost_args += ['--']
        self.args = boost_args + self.args

    def check_log(self, trim):
        ET.parse(self.xmlout)
        super().check_log(trim)

def can_connect(address, family=socket.AF_INET):
    s = socket.socket(family)
    try:
        s.connect(address)
        return True
    except:
        return False

def try_something_backoff(something):
    sleep_time = 0.05
    while not something():
        if sleep_time > 30:
            return False
        time.sleep(sleep_time)
        sleep_time *= 2
    return True


def make_saslauthd_conf(port, instance_path):
    """Creates saslauthd.conf with appropriate contents under instance_path.  Returns the path to the new file."""
    saslauthd_conf_path = os.path.join(instance_path, 'saslauthd.conf')
    with open(saslauthd_conf_path, 'w') as f:
        f.write('ldap_servers: ldap://localhost:{}\nldap_search_base: dc=example,dc=com'.format(port))
    return saslauthd_conf_path


class LdapTest(BoostTest):
    """A unit test which can produce its own XML output, and needs an ldap server"""

    def __init__(self, test_no, shortname, args, suite, mode, options):
        super().__init__(test_no, shortname, args, suite, mode, options)

    async def setup(self, port, options):
        instances_root = os.path.join(os.path.split(self.path)[0], 'ldap_instances');
        instance_path = os.path.join(os.path.abspath(instances_root), str(port))
        slapd_pid_file = os.path.join(instance_path, 'slapd.pid')
        data_path = os.path.join(instance_path, 'data')
        os.makedirs(data_path)
        # This will always fail because it lacks the permissions to read the default slapd data
        # folder but it does create the instance folder so we don't want to fail here.
        try:
            subprocess.check_output(['slaptest', '-f', LDAP_SERVER_CONFIGURATION_FILE, '-F', instance_path],
                                    stderr=subprocess.DEVNULL)
        except:
            pass
        # Set up failure injection.
        proxy_name = 'p{}'.format(port)
        subprocess.check_output([
            'toxiproxy-cli', 'c', proxy_name,
            '--listen', 'localhost:{}'.format(port + 2), '--upstream', 'localhost:{}'.format(port)])
        # Sever the connection after byte_limit bytes have passed through:
        byte_limit = options.byte_limit if options.byte_limit else randint(0, 2000)
        subprocess.check_output(['toxiproxy-cli', 't', 'a', proxy_name, '-t', 'limit_data', '-n', 'limiter',
                                 '-a', 'bytes={}'.format(byte_limit)])
        # Change the data folder in the default config.
        replace_expression = 's/olcDbDirectory:.*/olcDbDirectory: {}/g'.format(
            os.path.abspath(data_path).replace('/','\/'))
        subprocess.check_output(
            ['find', instance_path, '-type', 'f', '-exec', 'sed', '-i', replace_expression, '{}', ';'])
        # Change the pid file to be kept with the instance.
        replace_expression = 's/olcPidFile:.*/olcPidFile: {}/g'.format(
            os.path.abspath(slapd_pid_file).replace('/', '\/'))
        subprocess.check_output(
            ['find', instance_path, '-type', 'f', '-exec', 'sed', '-i', replace_expression, '{}', ';'])
        # Put the test data in.
        cmd = ['slapadd', '-F', instance_path]
        subprocess.check_output(
            cmd, input='\n\n'.join(DEFAULT_ENTRIES).encode('ascii'), stderr=subprocess.STDOUT)
        # Set up the server.
        SLAPD_URLS='ldap://:{}/ ldaps://:{}/'.format(port, port + 1)
        def can_connect_to_slapd():
            return can_connect(('127.0.0.1', port)) and can_connect(('127.0.0.1', port + 1)) and can_connect(('127.0.0.1', port + 2))
        def can_connect_to_saslauthd():
            return can_connect(os.path.join(instance_path, 'mux'), socket.AF_UNIX)
        slapd_proc = subprocess.Popen(['slapd', '-F', instance_path, '-h', SLAPD_URLS, '-d', '0'])
        saslauthd_conf_path = make_saslauthd_conf(port, instance_path)
        saslauthd_proc = subprocess.Popen(
            ['saslauthd', '-d', '-n', '1', '-a', 'ldap', '-O', saslauthd_conf_path, '-m', instance_path],
            stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        def finalize():
            slapd_proc.terminate()
            slapd_proc.wait() # Wait for slapd to remove slapd.pid, so it doesn't race with rmtree below.
            saslauthd_proc.kill() # Somehow, invoking terminate() here also terminates toxiproxy-server. o_O
            shutil.rmtree(instance_path)
            subprocess.check_output(['toxiproxy-cli', 'd', proxy_name])
        if not try_something_backoff(can_connect_to_slapd):
            finalize()
            raise Exception('Unable to connect to slapd')
        if not try_something_backoff(can_connect_to_saslauthd):
            finalize()
            raise Exception('Unable to connect to saslauthd')
        return finalize, '--byte-limit={}'.format(byte_limit)

class CqlTest(Test):
    """Run the sequence of CQL commands stored in the file and check
    output"""

    def __init__(self, test_no, shortname, suite, mode, options):
        super().__init__(test_no, shortname, suite, mode, options)
        # Path to cql_repl driver, in the given build mode
        self.path = os.path.join("build", self.mode, "test/tools/cql_repl")
        self.cql = os.path.join(suite.path, self.shortname + ".cql")
        self.result = os.path.join(suite.path, self.shortname + ".result")
        self.tmpfile = os.path.join(options.tmpdir, self.mode, self.uname + ".reject")
        self.reject = os.path.join(suite.path, self.shortname + ".reject")
        self.args = shlex.split("-c1 -m2G --input={} --output={} --log={}".format(
            self.cql, self.tmpfile, self.log_filename))
        self.args += UnitTest.standard_args
        self.is_executed_ok = False
        self.is_new = False
        self.is_equal_result = None
        self.summary = "not run"

    async def run(self, options):
        self.is_executed_ok = await run_test(self, options)
        self.success = False
        self.summary = "failed"

        def set_summary(summary):
            self.summary = summary
            logging.info("Test %d %s", self.id, summary)

        if not os.path.isfile(self.tmpfile):
            set_summary("failed: no output file")
        elif not os.path.isfile(self.result):
            set_summary("failed: no result file")
            self.is_new = True
        else:
            self.is_equal_result = filecmp.cmp(self.result, self.tmpfile)
            if self.is_equal_result is False:
                set_summary("failed: test output does not match expected result")
            elif self.is_executed_ok:
                self.success = True
                set_summary("succeeded")
            else:
                set_summary("failed: correct output but non-zero return status.\nCheck test log.")

        if self.is_new or self.is_equal_result is False:
            # Put a copy of the .reject file close to the .result file
            # so that it's easy to analyze the diff or overwrite .result
            # with .reject. Preserve the original .reject file: in
            # multiple modes the copy .reject file may be overwritten.
            shutil.copyfile(self.tmpfile, self.reject)
        elif os.path.exists(self.tmpfile):
            pathlib.Path(self.tmpfile).unlink()

        return self

    def print_summary(self):
        print("Test {} ({}) {}".format(palette.path(self.name), self.mode,
                                       self.summary))
        if self.is_equal_result is False:
            print_unidiff(self.result, self.reject)

class RunTest(Test):
    """Run tests in a directory started by a run script"""

    def __init__(self, test_no, shortname, suite, mode, options):
        super().__init__(test_no, shortname, suite, mode, options)
        self.path = os.path.join(suite.path, shortname)
        self.xmlout = os.path.join(options.tmpdir, self.mode, "xml", self.uname + ".xunit.xml")
        self.args = ["--junit-xml={}".format(self.xmlout)]
        self.env = { 'SCYLLA': os.path.join("build", self.mode, "scylla") }

    def print_summary(self):
        print("Output of {} {}:".format(self.path, " ".join(self.args)))
        print(read_log(self.log_filename))

    async def run(self, options):
        # This test can and should be killed gently, with SIGTERM, not with SIGKILL
        self.success = await run_test(self, options, gentle_kill=True, env=self.env)
        logging.info("Test #%d %s", self.id, "succeeded" if self.success else "failed ")
        return self

class TabularConsoleOutput:
    """Print test progress to the console"""

    def __init__(self, verbose, test_count):
        self.verbose = verbose
        self.test_count = test_count
        self.print_newline = False
        self.last_test_no = 0
        self.last_line_len = 1

    def print_start_blurb(self):
        print("="*80)
        print("{:7s} {:50s} {:^8s} {:8s}".format("[N/TOTAL]", "TEST", "MODE", "RESULT"))
        print("-"*78)

    def print_end_blurb(self):
        if self.print_newline:
            print("")
        print("-"*78)

    def print_progress(self, test):
        self.last_test_no += 1
        msg = "{:9s} {:50s} {:^8s} {:8s}".format(
            "[{}/{}]".format(self.last_test_no, self.test_count),
            test.name, test.mode[:8],
            palette.ok("[ PASS ]") if test.success else palette.fail("[ FAIL ]")
        )
        if self.verbose is False:
            if test.success:
                print("\r" + " " * self.last_line_len, end="")
                self.last_line_len = len(msg)
                print("\r" + msg, end="")
                self.print_newline = True
            else:
                if self.print_newline:
                    print("")
                print(msg)
                self.print_newline = False
        else:
            if hasattr(test, 'time_end') and test.time_end > 0:
                msg += " {:.2f}s".format(test.time_end - test.time_start)
            print(msg)


async def run_test(test, options, gentle_kill=False, env=dict()):
    """Run test program, return True if success else False"""

    with open(test.log_filename, "wb") as log:
        ldap_port = 5000 + test.id * 3
        cleanup_fn = None
        finject_desc = None
        cleanup_fn, finject_desc = await test.setup(ldap_port, options)

        def report_error(error, failure_injection_desc = None):
            msg = "=== TEST.PY SUMMARY START ===\n"
            msg += "{}\n".format(error)
            msg += "=== TEST.PY SUMMARY END ===\n"
            if failure_injection_desc is not None:
                msg += 'failure injection: {}'.format(failure_injection_desc)
            log.write(msg.encode(encoding="UTF-8"))
        process = None
        stdout = None
        logging.info("Starting test #%d: %s %s", test.id, test.path, " ".join(test.args))
        UBSAN_OPTIONS = [
            "halt_on_error=1",
            "abort_on_error=1",
            f"suppressions={os.getcwd()}/ubsan-suppressions.supp",
            os.getenv("UBSAN_OPTIONS"),
        ]
        ASAN_OPTIONS = [
            "disable_coredump=0",
            "abort_on_error=1",
            "detect_stack_use_after_return=1",
            os.getenv("ASAN_OPTIONS"),
        ]
        ldap_instance_path = os.path.join(
            os.path.abspath(os.path.join(os.path.split(test.path)[0], 'ldap_instances')),
            str(ldap_port))
        saslauthd_mux_path = os.path.join(ldap_instance_path, 'mux')
        if options.manual_execution:
            print('Please run the following shell command, then press <enter>:')
            print('SEASTAR_LDAP_PORT={} SASLAUTHD_MUX_PATH={} {}'.format(
                ldap_port, saslauthd_mux_path, ' '.join([shlex.quote(e) for e in [test.path, *test.args]])))
            input('-- press <enter> to continue --')
            if cleanup_fn is not None:
                cleanup_fn()
            return True
        try:
            log.write("=== TEST.PY STARTING TEST #{} ===\n".format(test.id).encode(encoding="UTF-8"))
            log.write("export UBSAN_OPTIONS='{}'\n".format(":".join(filter(None, UBSAN_OPTIONS))).encode(encoding="UTF-8"))
            log.write("export ASAN_OPTIONS='{}'\n".format(":".join(filter(None, ASAN_OPTIONS))).encode(encoding="UTF-8"))
            log.write("{} {}\n".format(test.path, " ".join(test.args)).encode(encoding="UTF-8"))
            log.write("=== TEST.PY TEST OUTPUT ===\n".format(test.id).encode(encoding="UTF-8"))
            log.flush();
            test.time_start = time.time()
            test.time_end = 0
            process = await asyncio.create_subprocess_exec(
                test.path,
                *test.args,
                stderr=log,
                stdout=log,
                env=dict(os.environ,
                         SEASTAR_LDAP_PORT=str(ldap_port),
                         SASLAUTHD_MUX_PATH=saslauthd_mux_path,
                         UBSAN_OPTIONS=":".join(filter(None, UBSAN_OPTIONS)),
                         ASAN_OPTIONS=":".join(filter(None, ASAN_OPTIONS)),
                         # TMPDIR env variable is used by any seastar/scylla
                         # test for directory to store test temporary data.
                         TMPDIR=os.path.join(options.tmpdir, test.mode),
                         **env,
                         ),
                preexec_fn=os.setsid,
            )
            stdout, _ = await asyncio.wait_for(process.communicate(), options.timeout)
            test.time_end = time.time()
            if process.returncode != 0:
                report_error('Test exited with code {code}\n'.format(code=process.returncode))
                return False
            try:
                test.check_log(not options.save_log_on_success)
            except Exception as e:
                print("")
                print(test.name + ": " + palette.crit("failed to parse XML output: {}".format(e)))
                # return False
            return True
        except (asyncio.TimeoutError, asyncio.CancelledError) as e:
            if process is not None:
                if gentle_kill:
                    process.terminate()
                else:
                    process.kill()
                stdout, _ = await process.communicate()
            if isinstance(e, asyncio.TimeoutError):
                report_error("Test timed out")
            elif isinstance(e, asyncio.CancelledError):
                print(test.name, end=" ")
                report_error("Test was cancelled")
        except Exception as e:
            report_error("Failed to run the test:\n{e}".format(e=e), finject_desc)
        finally:
            if cleanup_fn is not None:
                cleanup_fn()
    return False


def setup_signal_handlers(loop, signaled):

    async def shutdown(loop, signo, signaled):
        print("\nShutdown requested... Aborting tests:"),
        signaled.signo = signo
        signaled.set()

    # Use a lambda to avoid creating a coroutine until
    # the signal is delivered to the loop - otherwise
    # the coroutine will be dangling when the loop is over,
    # since it's never going to be invoked
    for signo in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(signo, lambda: asyncio.create_task(shutdown(loop, signo, signaled)))


def parse_cmd_line():
    """ Print usage and process command line options. """
    all_modes = ['debug', 'release', 'dev', 'sanitize']
    sysmem = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
    testmem = 2e9
    cpus_per_test_job = 1
    default_num_jobs_mem = ((sysmem - 4e9) // testmem)
    default_num_jobs_cpu = multiprocessing.cpu_count() // cpus_per_test_job
    default_num_jobs = min(default_num_jobs_mem, default_num_jobs_cpu)

    parser = argparse.ArgumentParser(description="Scylla test runner")
    parser.add_argument(
        "name",
        nargs="*",
        action="store",
        help="""Can be empty. List of test names, to look for in
                suites. Each name is used as a substring to look for in the
                path to test file, e.g. "mem" will run all tests that have
                "mem" in their name in all suites, "boost/mem" will only enable
                tests starting with "mem" in "boost" suite. Default: run all
                tests in all suites.""",
    )
    parser.add_argument(
        "--tmpdir",
        action="store",
        default="testlog",
        help="""Path to temporary test data and log files. The data is
        further segregated per build mode. Default: ./testlog.""",
    )
    parser.add_argument('--mode', choices=all_modes, action="append", dest="modes",
                        help="Run only tests for given build mode(s)")
    parser.add_argument('--repeat', action="store", default="1", type=int,
                        help="number of times to repeat test execution")
    parser.add_argument('--timeout', action="store", default="12000", type=int,
                        help="timeout value for test execution")
    parser.add_argument('--verbose', '-v', action='store_true', default=False,
                        help='Verbose reporting')
    parser.add_argument('--jobs', '-j', action="store", default=default_num_jobs, type=int,
                        help="Number of jobs to use for running the tests")
    parser.add_argument('--save-log-on-success', "-s", default=False,
                        dest="save_log_on_success", action="store_true",
                        help="Save test log output on success.")
    parser.add_argument('--list', dest="list_tests", action="store_true", default=False,
                        help="Print list of tests instead of executing them")
    parser.add_argument('--skip', default="",
                        dest="skip_pattern", action="store",
                        help="Skip tests which match the provided pattern")
    parser.add_argument('--manual-execution', action='store_true', default=False,
                        help='Let me manually run the test executable at the moment this script would run it')
    parser.add_argument('--byte-limit', action="store", default=None, type=int,
                        help="Specific byte limit for failure injection (random by default)")
    args = parser.parse_args()

    if not output_is_a_tty:
        args.verbose = True

    if not args.modes:
        out = subprocess.Popen(['ninja', 'mode_list'], stdout=subprocess.PIPE).communicate()[0].decode()
        # [1/1] List configured modes
        # debug release dev
        args.modes = out.split('\n')[1].split(' ')

    def prepare_dir(dirname, pattern):
        # Ensure the dir exists
        pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)
        # Remove old artefacts
        for p in glob.glob(os.path.join(dirname, pattern), recursive=True):
            pathlib.Path(p).unlink()

    args.tmpdir = os.path.abspath(args.tmpdir)
    prepare_dir(args.tmpdir, "*.log")

    for mode in args.modes:
        prepare_dir(os.path.join(args.tmpdir, mode), "*.log")
        prepare_dir(os.path.join(args.tmpdir, mode), "*.reject")
        prepare_dir(os.path.join(args.tmpdir, mode, "xml"), "*.xml")

    return args


def find_tests(options):

    for f in glob.glob(os.path.join("test", "*")):
        if os.path.isdir(f) and os.path.isfile(os.path.join(f, "suite.yaml")):
            for mode in options.modes:
                suite = TestSuite.opt_create(f)
                suite.add_test_list(mode, options)

    if not TestSuite.test_count():
        print("Test {} not found".format(palette.path(options.name[0])))
        sys.exit(1)

    logging.info("Found %d tests, repeat count is %d, starting %d concurrent jobs",
                 TestSuite.test_count(), options.repeat, options.jobs)


async def run_all_tests(signaled, options):
    console = TabularConsoleOutput(options.verbose, TestSuite.test_count())
    signaled_task = asyncio.create_task(signaled.wait())
    pending = set([signaled_task])

    async def cancel(pending):
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        print("... done.")
        raise asyncio.CancelledError

    async def reap(done, pending, signaled):
        nonlocal console
        if signaled.is_set():
            await cancel(pending)
        for coro in done:
            result = coro.result()
            if isinstance(result, bool):
                continue    # skip signaled task result
            console.print_progress(result)
    console.print_start_blurb()
    try:
        for test in TestSuite.tests():
            # +1 for 'signaled' event
            if len(pending) > options.jobs:
                # Wait for some task to finish
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                await reap(done, pending, signaled)
            pending.add(asyncio.create_task(test.run(options)))
        # Wait & reap ALL tasks but signaled_task
        # Do not use asyncio.ALL_COMPLETED to print a nice progress report
        while len(pending) > 1:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            await reap(done, pending, signaled)

    except asyncio.CancelledError:
        return

    console.print_end_blurb()


def read_log(log_filename):
    """Intelligently read test log output"""
    try:
        with open(log_filename, "r") as log:
            msg = log.read()
            return msg if len(msg) else "===Empty log output==="
    except FileNotFoundError:
        return "===Log {} not found===".format(log_filename)
    except OSError as e:
        return "===Error reading log {}===".format(e)


def print_summary(failed_tests):
    if failed_tests:
        print("The following test(s) have failed: {}".format(
            palette.path(" ".join([t.name for t in failed_tests]))))
        for test in failed_tests:
            test.print_summary()
            print("-"*78)
        print("Summary: {} of the total {} tests failed".format(
            len(failed_tests), TestSuite.test_count()))


def print_unidiff(fromfile, tofile):
    with open(fromfile, "r") as frm, open(tofile, "r") as to:
        diff = difflib.unified_diff(
            frm.readlines(),
            to.readlines(),
            fromfile=fromfile,
            tofile=tofile,
            fromfiledate=time.ctime(os.stat(fromfile).st_mtime),
            tofiledate=time.ctime(os.stat(tofile).st_mtime),
            n=10)           # Number of context lines

        for i, line in enumerate(diff):
            if i > 60:
                break
            if line.startswith('+'):
                line = palette.diff_in(line)
            elif line.startswith('-'):
                line = palette.diff_out(line)
            elif line.startswith('@'):
                line = palette.diff_mark(line)
            sys.stdout.write(line)


def write_junit_report(tmpdir, mode):
    junit_filename = os.path.join(tmpdir, mode, "xml", "junit.xml")
    total = 0
    failed = 0
    xml_results = ET.Element("testsuite", name="non-boost tests", errors="0")
    for suite in TestSuite.suites.values():
        for test in suite.junit_tests():
            if test.mode != mode:
                continue
            total += 1
            xml_res = ET.SubElement(xml_results, 'testcase',
                                    name="{}.{}.{}".format(test.shortname, mode, test.id))
            if test.success is True:
                continue
            failed += 1
            xml_fail = ET.SubElement(xml_res, 'failure')
            xml_fail.text = "Test {} {} failed, check the log at {}".format(
                test.path,
                " ".join(test.args),
                test.log_filename)
    if total == 0:
        return
    xml_results.set("tests", str(total))
    xml_results.set("failures", str(failed))
    with open(junit_filename, "w") as f:
        ET.ElementTree(xml_results).write(f, encoding="unicode")


def open_log(tmpdir):
    pathlib.Path(tmpdir).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=os.path.join(tmpdir, "test.py.log"),
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s> %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.critical("Started %s", " ".join(sys.argv))


async def main():

    options = parse_cmd_line()

    open_log(options.tmpdir)
    find_tests(options)
    if options.list_tests:
        print('\n'.join([t.name for t in TestSuite.tests()]))
        return 0

    if options.manual_execution and TestSuite.test_count() > 1:
        print('--manual-execution only supports running a single test, but multiple selected: {}'.format(
            [t.path for t in TestSuite.tests()][:3])) # Print whole t.path; same shortname may be in different dirs.
        return 1

    signaled = asyncio.Event()

    setup_signal_handlers(asyncio.get_event_loop(), signaled)

    tp_server = None
    try:
        if [t for t in TestSuite.tests() if isinstance(t, LdapTest)]:
            tp_server = subprocess.Popen('toxiproxy-server', stderr=subprocess.DEVNULL)
            def can_connect_to_toxiproxy():
                return can_connect(('127.0.0.1', 8474))
            if not try_something_backoff(can_connect_to_toxiproxy):
                raise Exception('Could not connect to toxiproxy')

        await run_all_tests(signaled, options)
    finally:
        if tp_server is not None:
            tp_server.terminate()

    if signaled.is_set():
        return -signaled.signo

    failed_tests = [t for t in TestSuite.tests() if t.success is not True]

    print_summary(failed_tests)

    for mode in options.modes:
        write_junit_report(options.tmpdir, mode)

    # Note: failure codes must be in the ranges 0-124, 126-127,
    #       to cooperate with git bisect's expectations
    return 0 if not failed_tests else 1

if __name__ == "__main__":
    colorama.init()

    if sys.version_info < (3, 7):
        print("Python 3.7 or newer is required to run this program")
        sys.exit(-1)
    sys.exit(asyncio.run(main()))
