#  Copyright (C) 2017 ScyllaDB

# This file is part of Scylla.
#
# See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.

import configparser
import io
import logging
import os
import re
import shlex
import shutil
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import yaml
import psutil
import sys
from pathlib import Path

import distro


def scriptsdir_p():
    p = Path(sys.argv[0]).resolve()
    if p.parent.name == 'libexec':
        return p.parents[1]
    return p.parent

def scylladir_p():
    p = scriptsdir_p()
    return p.parent

def is_nonroot():
    return Path(scylladir_p() / 'SCYLLA-NONROOT-FILE').exists()

def is_offline():
    return Path(scylladir_p() / 'SCYLLA-OFFLINE-FILE').exists()

def bindir_p():
    if is_nonroot():
        return scylladir_p() / 'bin'
    else:
        return Path('/usr/bin')

def etcdir_p():
    if is_nonroot():
        return scylladir_p() / 'etc'
    else:
        return Path('/etc')

def datadir_p():
    if is_nonroot():
        return scylladir_p()
    else:
        return Path('/var/lib/scylla')

def scyllabindir_p():
    return scylladir_p() / 'bin'

def scriptsdir():
    return str(scriptsdir_p())

def scylladir():
    return str(scylladir_p())

def bindir():
    return str(bindir_p())

def etcdir():
    return str(etcdir_p())

def datadir():
    return str(datadir_p())

def scyllabindir():
    return str(scyllabindir_p())

# @param headers dict of k:v
def curl(url, byte=False, headers={}):
    max_retries = 5
    retries = 0
    while True:
        try:
            req = urllib.request.Request(url,headers=headers)
            with urllib.request.urlopen(req) as res:
                if byte:
                    return res.read()
                else:
                    return res.read().decode('utf-8')
        except urllib.error.HTTPError:
            logging.warn("Failed to grab %s..." % url)
            time.sleep(5)
            retries += 1
            if (retries >= max_retries):
                raise

class gcp_instance:
    """Describe several aspects of the current GCP instance"""

    EPHEMERAL = "ephemeral"
    ROOT = "root"

    def __init__(self):
        self.__type = None
        self.__cpu = None
        self.__memoryGB = None
        self.__nvmeDiskCount = None
        self.__firstNvmeSize = None
        self.__osDisks = None

    @staticmethod
    def is_gce_instance():
        """Check if it's GCE instance via DNS lookup to metadata server."""
        import socket
        try:
            addrlist = socket.getaddrinfo('metadata.google.internal', 80)
        except socket.gaierror:
            return False
        for res in addrlist:
            af, socktype, proto, canonname, sa = res
            if af == socket.AF_INET:
                addr, port = sa
                if addr == "169.254.169.254":
                    return True
        return False

    def __instance_metadata(self, path):
        """query GCP metadata server, recursively!"""
        return curl("http://169.254.169.254/computeMetadata/v1/instance/" + path+"?recursive=true", headers={"Metadata-Flavor": "Google"})
        #169.254.169.254 is metadata.google.internal

    def _non_root_nvmes(self):
        """get list of nvme disks from os, filter away if one of them is root"""
        nvme_re = re.compile(r"nvme\d+n\d+$")

        root_dev_candidates = [x for x in psutil.disk_partitions() if x.mountpoint == "/"]
        if len(root_dev_candidates) != 1:
            raise Exception("found more than one disk mounted at root ".format(root_dev_candidates))

        root_dev = root_dev_candidates[0].device
        # if root_dev.startswith("/dev/mapper"):
        #     raise Exception("mapper used for root, not checking if nvme is used ".format(root_dev))

        nvmes_present = list(filter(nvme_re.match, os.listdir("/dev")))
        return {self.ROOT: [root_dev], self.EPHEMERAL: [x for x in nvmes_present if not root_dev.startswith(os.path.join("/dev/", x))]}

    @property
    def os_disks(self):
        """populate disks from /dev/ and root mountpoint"""
        if self.__osDisks is None:
            __osDisks = {}
            nvmes_present = self._non_root_nvmes()
            for k, v in nvmes_present.items():
                __osDisks[k] = v
            self.__osDisks = __osDisks
        return self.__osDisks

    def getEphemeralOsDisks(self):
        """return just transient disks"""
        return self.os_disks[self.EPHEMERAL]

    @staticmethod
    def isNVME(gcpdiskobj):
        """check if disk from GCP metadata is a NVME disk"""
        if gcpdiskobj["interface"]=="NVME":
            return True
        return False

    def __get_nvme_disks_from_metadata(self):
        """get list of nvme disks from metadata server"""
        import json
        try:
            disksREST=self.__instance_metadata("disks")
            disksobj=json.loads(disksREST)
            nvmedisks=list(filter(self.isNVME, disksobj))
        except Exception as e:
            print ("Problem when parsing disks from metadata:")
            print (e)
            nvmedisks={}
        return nvmedisks

    @property
    def nvmeDiskCount(self):
        """get # of nvme disks available for scylla raid"""
        if self.__nvmeDiskCount is None:
            try:
                ephemeral_disks = self.getEphemeralOsDisks()
                count_os_disks=len(ephemeral_disks)
            except Exception as e:
                print ("Problem when parsing disks from OS:")
                print (e)
                count_os_disks=0
            nvme_metadata_disks = self.__get_nvme_disks_from_metadata()
            count_metadata_nvme_disks=len(nvme_metadata_disks)
            self.__nvmeDiskCount = count_os_disks if count_os_disks<count_metadata_nvme_disks else count_metadata_nvme_disks
        return self.__nvmeDiskCount

    @property
    def instancetype(self):
        """return the type of this instance, e.g. n2-standard-2"""
        if self.__type is None:
            self.__type = self.__instance_metadata("machine-type").split("/")[-1]
        return self.__type

    @property
    def cpu(self):
        """return the # of cpus of this instance"""
        if self.__cpu is None:
            self.__cpu = psutil.cpu_count()
        return self.__cpu

    @property
    def memoryGB(self):
        """return the size of memory in GB of this instance"""
        if self.__memoryGB is None:
            self.__memoryGB = psutil.virtual_memory().total/1024/1024/1024
        return self.__memoryGB

    def instance_size(self):
        """Returns the size of the instance we are running in. i.e.: 2"""
        return self.instancetype.split("-")[2]

    def instance_class(self):
        """Returns the class of the instance we are running in. i.e.: n2"""
        return self.instancetype.split("-")[0]

    def instance_purpose(self):
        """Returns the purpose of the instance we are running in. i.e.: standard"""
        return self.instancetype.split("-")[1]

    m1supported="m1-megamem-96" #this is the only exception of supported m1 as per https://cloud.google.com/compute/docs/machine-types#m1_machine_types

    def is_unsupported_instance(self):
        """Returns if this instance type belongs to unsupported ones for nvmes"""
        if self.instancetype == self.m1supported:
            return False
        if self.instance_class() in ['e2', 'f1', 'g1', 'm2', 'm1']:
            return True
        return False

    def is_supported_instance(self):
        """Returns if this instance type belongs to supported ones for nvmes"""
        if self.instancetype == self.m1supported:
            return True
        if self.instance_class() in ['n1', 'n2', 'n2d' ,'c2']:
            return True
        return False

    def is_recommended_instance_size(self):
        """if this instance has at least 2 cpus, it has a recommended size"""
        if int(self.instance_size()) > 1:
            return True
        return False

    @staticmethod
    def get_file_size_by_seek(filename):
        "Get the file size by seeking at end"
        fd= os.open(filename, os.O_RDONLY)
        try:
            return os.lseek(fd, 0, os.SEEK_END)
        finally:
            os.close(fd)

    # note that GCP has 3TB physical devices actually, which they break into smaller 375GB disks and share the same mem with multiple machines
    # this is a reference value, disk size shouldn't be lower than that
    GCP_NVME_DISK_SIZE_2020=375

    @property
    def firstNvmeSize(self):
        """return the size of first non root NVME disk in GB"""
        if self.__firstNvmeSize is None:
            ephemeral_disks = self.getEphemeralOsDisks()
            firstDisk = ephemeral_disks[0]
            firstDiskSize = self.get_file_size_by_seek(os.path.join("/dev/", firstDisk))
            firstDiskSizeGB = firstDiskSize/1024/1024/1024
            if firstDiskSizeGB >= self.GCP_NVME_DISK_SIZE_2020:
                self.__firstNvmeSize = firstDiskSizeGB
            else:
                raise Exception("First nvme is smaller than lowest expected size. ".format(firstDisk))
        return self.__firstNvmeSize

    def is_recommended_instance(self):
        if self.is_recommended_instance_size() and not self.is_unsupported_instance() and self.is_supported_instance():
            # at least 1:2GB cpu:ram ratio , GCP is at 1:4, so this should be fine
            if self.cpu/self.memoryGB < 0.5:
              # 30:1 Disk/RAM ratio must be kept at least(AWS), we relax this a little bit
              # on GCP we are OK with 50:1 , n1-standard-2 can cope with 1 disk, not more
              diskCount = self.nvmeDiskCount
              # to reach max performance for > 16 disks we mandate 32 or more vcpus
              # https://cloud.google.com/compute/docs/disks/local-ssd#performance
              if diskCount >= 16 and self.cpu < 32:
                  return False
              diskSize= self.firstNvmeSize
              if diskCount < 1:
                  return False
              disktoramratio = (diskCount*diskSize)/self.memoryGB
              if (disktoramratio <= 50) and (disktoramratio > 0):
                  return True
        return False

class aws_instance:
    """Describe several aspects of the current AWS instance"""

    def __disk_name(self, dev):
        name = re.compile(r"(?:/dev/)?(?P<devname>[a-zA-Z]+)\d*")
        return name.search(dev).group("devname")

    def __instance_metadata(self, path):
        return curl("http://169.254.169.254/latest/meta-data/" + path)

    def __device_exists(self, dev):
        if dev[0:4] != "/dev":
            dev = "/dev/%s" % dev
        return os.path.exists(dev)

    def __xenify(self, devname):
        dev = self.__instance_metadata('block-device-mapping/' + devname)
        return dev.replace("sd", "xvd")

    def _non_root_nvmes(self):
        nvme_re = re.compile(r"nvme\d+n\d+$")

        root_dev_candidates = [ x for x in psutil.disk_partitions() if x.mountpoint == "/" ]
        if len(root_dev_candidates) != 1:
            raise Exception("found more than one disk mounted at root'".format(root_dev_candidates))

        root_dev = root_dev_candidates[0].device
        nvmes_present = list(filter(nvme_re.match, os.listdir("/dev")))
        return {"root": [ root_dev ], "ephemeral": [ x for x in nvmes_present if not root_dev.startswith(os.path.join("/dev/", x)) ] }

    def __populate_disks(self):
        devmap = self.__instance_metadata("block-device-mapping")
        self._disks = {}
        devname = re.compile("^\D+")
        nvmes_present = self._non_root_nvmes()
        for k,v in nvmes_present.items():
            self._disks[k] = v

        for dev in devmap.splitlines():
            t = devname.match(dev).group()
            if t == "ephemeral" and nvmes_present:
                continue
            if t not in self._disks:
                self._disks[t] = []
            if not self.__device_exists(self.__xenify(dev)):
                continue
            self._disks[t] += [self.__xenify(dev)]

    def __mac_address(self, nic='eth0'):
        with open('/sys/class/net/{}/address'.format(nic)) as f:
            return f.read().strip()

    def __init__(self):
        self._type = self.__instance_metadata("instance-type")
        self.__populate_disks()

    def instance(self):
        """Returns which instance we are running in. i.e.: i3.16xlarge"""
        return self._type

    def instance_size(self):
        """Returns the size of the instance we are running in. i.e.: 16xlarge"""
        return self._type.split(".")[1]

    def instance_class(self):
        """Returns the class of the instance we are running in. i.e.: i3"""
        return self._type.split(".")[0]

    def is_supported_instance_class(self):
        if self.instance_class() in ['i2', 'i3', 'i3en', 'c5d', 'm5d', 'm5ad', 'r5d', 'z1d']:
            return True
        return False

    def get_en_interface_type(self):
        instance_class = self.instance_class()
        instance_size = self.instance_size()
        if instance_class in ['c3', 'c4', 'd2', 'i2', 'r3']:
            return 'ixgbevf'
        if instance_class in ['a1', 'c5', 'c5a', 'c5d', 'c5n', 'c6g', 'c6gd', 'f1', 'g3', 'g4', 'h1', 'i3', 'i3en', 'inf1', 'm5', 'm5a', 'm5ad', 'm5d', 'm5dn', 'm5n', 'm6g', 'm6gd', 'p2', 'p3', 'r4', 'r5', 'r5a', 'r5ad', 'r5d', 'r5dn', 'r5n', 't3', 't3a', 'u-6tb1', 'u-9tb1', 'u-12tb1', 'u-18tn1', 'u-24tb1', 'x1', 'x1e', 'z1d']:
            return 'ena'
        if instance_class == 'm4':
            if instance_size == '16xlarge':
                return 'ena'
            else:
                return 'ixgbevf'
        return None

    def disks(self):
        """Returns all disks in the system, as visible from the AWS registry"""
        disks = set()
        for v in list(self._disks.values()):
            disks = disks.union([self.__disk_name(x) for x in v])
        return disks

    def root_device(self):
        """Returns the device being used for root data. Unlike root_disk(),
           which will return a device name (i.e. xvda), this function will return
           the full path to the root partition as returned by the AWS instance
           metadata registry"""
        return set(self._disks["root"])

    def root_disk(self):
        """Returns the disk used for the root partition"""
        return self.__disk_name(self._disks["root"][0])

    def non_root_disks(self):
        """Returns all attached disks but root. Include ephemeral and EBS devices"""
        return set(self._disks["ephemeral"] + self._disks["ebs"])

    def ephemeral_disks(self):
        """Returns all ephemeral disks. Include standard SSDs and NVMe"""
        return set(self._disks["ephemeral"])

    def ebs_disks(self):
        """Returns all EBS disks"""
        return set(self._disks["ephemeral"])

    def public_ipv4(self):
        """Returns the public IPv4 address of this instance"""
        return self.__instance_metadata("public-ipv4")

    def private_ipv4(self):
        """Returns the private IPv4 address of this instance"""
        return self.__instance_metadata("local-ipv4")

    def is_vpc_enabled(self, nic='eth0'):
        mac = self.__mac_address(nic)
        mac_stat = self.__instance_metadata('network/interfaces/macs/{}'.format(mac))
        return True if re.search(r'^vpc-id$', mac_stat, flags=re.MULTILINE) else False


# Regular expression helpers
# non-advancing comment matcher
_nocomment = r"^\s*(?!#)"
# non-capturing grouping
_scyllaeq = r"(?:\s*|=)"
_cpuset = r"(?:\s*--cpuset" + _scyllaeq + r"(?P<cpuset>\d+(?:[-,]\d+)*))"
_smp = r"(?:\s*--smp" + _scyllaeq + r"(?P<smp>\d+))"


def _reopt(s):
    return s + r"?"


def is_developer_mode():
    f = open(etcdir() + "/scylla.d/dev-mode.conf", "r")
    pattern = re.compile(_nocomment + r".*developer-mode" + _scyllaeq + "(1|true)")
    return len([x for x in f if pattern.match(x)]) >= 1


class scylla_cpuinfo:
    """Class containing information about how Scylla sees CPUs in this machine.
    Information that can be probed include in which hyperthreads Scylla is configured
    to run, how many total threads exist in the system, etc"""

    def __parse_cpuset(self):
        f = open(etcdir() + "/scylla.d/cpuset.conf", "r")
        pattern = re.compile(_nocomment + r"CPUSET=\s*\"" + _reopt(_cpuset) + _reopt(_smp) + "\s*\"")
        grp = [pattern.match(x) for x in f.readlines() if pattern.match(x)]
        if not grp:
            d = {"cpuset": None, "smp": None}
        else:
            # if more than one, use last
            d = grp[-1].groupdict()
        actual_set = set()
        if d["cpuset"]:
            groups = d["cpuset"].split(",")
            for g in groups:
                ends = [int(x) for x in g.split("-")]
                actual_set = actual_set.union(set(range(ends[0], ends[-1] + 1)))
            d["cpuset"] = actual_set
        if d["smp"]:
            d["smp"] = int(d["smp"])
        self._cpu_data = d

    def __system_cpus(self):
        cur_proc = -1
        f = open("/proc/cpuinfo", "r")
        results = {}
        for line in f:
            if line == '\n':
                continue
            key, value = [x.strip() for x in line.split(":")]
            if key == "processor":
                cur_proc = int(value)
                results[cur_proc] = {}
            results[cur_proc][key] = value
        return results

    def __init__(self):
        self.__parse_cpuset()
        self._cpu_data["system"] = self.__system_cpus()

    def system_cpuinfo(self):
        """Returns parsed information about CPUs in the system"""
        return self._cpu_data["system"]

    def system_nr_threads(self):
        """Returns the number of threads available in the system"""
        return len(self._cpu_data["system"])

    def system_nr_cores(self):
        """Returns the number of cores available in the system"""
        return len(set([x['core id'] for x in list(self._cpu_data["system"].values())]))

    def cpuset(self):
        """Returns the current cpuset Scylla is configured to use. Returns None if no constraints exist"""
        return self._cpu_data["cpuset"]

    def smp(self):
        """Returns the explicit smp configuration for Scylla, returns None if no constraints exist"""
        return self._cpu_data["smp"]

    def nr_shards(self):
        """How many shards will Scylla use in this machine"""
        if self._cpu_data["smp"]:
            return self._cpu_data["smp"]
        elif self._cpu_data["cpuset"]:
            return len(self._cpu_data["cpuset"])
        else:
            return len(self._cpu_data["system"])


# When a CLI tool is not installed, use relocatable CLI tool provided by Scylla
scylla_env = os.environ.copy()
scylla_env['PATH'] =  '{}:{}'.format(scyllabindir(), scylla_env['PATH'])

def run(cmd, shell=False, silent=False, exception=True):
    stdout = subprocess.DEVNULL if silent else None
    stderr = subprocess.DEVNULL if silent else None
    if not shell:
        cmd = shlex.split(cmd)
    return subprocess.run(cmd, stdout=stdout, stderr=stderr, shell=shell, check=exception, env=scylla_env).returncode


def out(cmd, shell=False, exception=True, timeout=None):
    if not shell:
        cmd = shlex.split(cmd)
    return subprocess.run(cmd, capture_output=True, shell=shell, timeout=timeout, check=exception, encoding='utf-8', env=scylla_env).stdout.strip()


def get_id_like():
    like = distro.like()
    if not like:
        return None
    return like.split(' ')

def is_debian_variant():
    d = get_id_like() if get_id_like() else distro.id()
    return ('debian' in d)

def is_redhat_variant():
    d = get_id_like() if get_id_like() else distro.id()
    return ('rhel' in d) or ('fedora' in d) or ('oracle') in d

def is_amzn2():
    return ('amzn' in distro.id()) and ('2' in distro.version())

def is_gentoo_variant():
    return ('gentoo' in distro.id())

def redhat_version():
    return distro.version()

def is_ec2():
    if os.path.exists('/sys/hypervisor/uuid'):
        with open('/sys/hypervisor/uuid') as f:
            s = f.read()
        return True if re.match(r'^ec2.*', s, flags=re.IGNORECASE) else False
    elif os.path.exists('/sys/class/dmi/id/board_vendor'):
        with open('/sys/class/dmi/id/board_vendor') as f:
            s = f.read()
        return True if re.match(r'^Amazon EC2$', s) else False
    return False


def is_systemd():
    try:
        with open('/proc/1/comm') as f:
            s = f.read()
        return True if re.match(r'^systemd$', s, flags=re.MULTILINE) else False
    except Exception:
        return False


def hex2list(hex_str):
    hex_str2 = hex_str.replace("0x", "").replace(",", "")
    hex_int = int(hex_str2, 16)
    bin_str = "{0:b}".format(hex_int)
    bin_len = len(bin_str)
    cpu_list = []
    i = 0
    while i < bin_len:
        if 1 << i & hex_int:
            j = i
            while j + 1 < bin_len and 1 << j + 1 & hex_int:
                j += 1
            if j == i:
                cpu_list.append(str(i))
            else:
                cpu_list.append("{0}-{1}".format(i, j))
                i = j
        i += 1
    return ",".join(cpu_list)


def makedirs(name):
    if not os.path.isdir(name):
        os.makedirs(name)


def rmtree(path):
    if not os.path.islink(path):
        shutil.rmtree(path)
    else:
        os.remove(path)

def current_umask():
    current = os.umask(0)
    os.umask(current)
    return current

def dist_name():
    return distro.name()


def dist_ver():
    return distro.version()


SYSTEM_PARTITION_UUIDS = [
        '21686148-6449-6e6f-744e-656564454649', # BIOS boot partition
        'c12a7328-f81f-11d2-ba4b-00a0c93ec93b', # EFI system partition
        '024dee41-33e7-11d3-9d69-0008c781f39f'  # MBR partition scheme
]

def get_partition_uuid(dev):
    return out(f'lsblk -n -oPARTTYPE {dev}')

def is_system_partition(dev):
    uuid = get_partition_uuid(dev)
    return (uuid in SYSTEM_PARTITION_UUIDS)

def is_unused_disk(dev):
    # dev is not in /sys/class/block/, like /dev/nvme[0-9]+
    if not os.path.isdir('/sys/class/block/{dev}'.format(dev=dev.replace('/dev/', ''))):
        return False
    try:
        fd = os.open(dev, os.O_EXCL)
        os.close(fd)
        # dev is not reserved for system
        return not is_system_partition(dev)
    except OSError:
        return False


CONCOLORS = {'green': '\033[1;32m', 'red': '\033[1;31m', 'nocolor': '\033[0m'}


def colorprint(msg, **kwargs):
    fmt = dict(CONCOLORS)
    fmt.update(kwargs)
    print(msg.format(**fmt))


def get_mode_cpuset(nic, mode):
    mode_cpu_mask = out('/opt/scylladb/scripts/perftune.py --tune net --nic {} --mode {} --get-cpu-mask-quiet'.format(nic, mode))
    return hex2list(mode_cpu_mask)


def parse_scylla_dirs_with_default(conf='/etc/scylla/scylla.yaml'):
    y = yaml.safe_load(open(conf))
    if 'workdir' not in y or not y['workdir']:
        y['workdir'] = datadir()
    if 'data_file_directories' not in y or \
            not y['data_file_directories'] or \
            not len(y['data_file_directories']) or \
            not " ".join(y['data_file_directories']).strip():
        y['data_file_directories'] = [os.path.join(y['workdir'], 'data')]
    for t in [ "commitlog", "hints", "view_hints", "saved_caches" ]:
        key = "%s_directory" % t
        if key not in y or not y[key]:
            y[key] = os.path.join(y['workdir'], t)
    return y


def get_scylla_dirs():
    """
    Returns a list of scylla directories configured in /etc/scylla/scylla.yaml.
    Verifies that mandatory parameters are set.
    """
    scylla_yaml_name = '/etc/scylla/scylla.yaml'
    y = yaml.safe_load(open(scylla_yaml_name))

    # Check that mandatory fields are set
    if 'workdir' not in y or not y['workdir']:
        y['workdir'] = datadir()
    if 'data_file_directories' not in y or \
            not y['data_file_directories'] or \
            not len(y['data_file_directories']) or \
            not " ".join(y['data_file_directories']).strip():
        y['data_file_directories'] = [os.path.join(y['workdir'], 'data')]
    if 'commitlog_directory' not in y or not y['commitlog_directory']:
        y['commitlog_directory'] = os.path.join(y['workdir'], 'commitlog')

    dirs = []
    dirs.extend(y['data_file_directories'])
    dirs.append(y['commitlog_directory'])

    if 'hints_directory' in y and y['hints_directory']:
        dirs.append(y['hints_directory'])
    if 'view_hints_directory' in y and y['view_hints_directory']:
        dirs.append(y['view_hints_directory'])

    return [d for d in dirs if d is not None]


def perftune_base_command():
    disk_tune_param = "--tune disks " + " ".join("--dir {}".format(d) for d in get_scylla_dirs())
    return '/opt/scylladb/scripts/perftune.py {}'.format(disk_tune_param)


def get_cur_cpuset():
    cfg = sysconfig_parser('/etc/scylla.d/cpuset.conf')
    cpuset = cfg.get('CPUSET')
    return re.sub(r'^--cpuset (.+)$', r'\1', cpuset).strip()


def get_tune_mode(nic):
    if not os.path.exists('/etc/scylla.d/cpuset.conf'):
        raise Exception('/etc/scylla.d/cpuset.conf not found')
    cur_cpuset = get_cur_cpuset()
    mq_cpuset = get_mode_cpuset(nic, 'mq')
    sq_cpuset = get_mode_cpuset(nic, 'sq')
    sq_split_cpuset = get_mode_cpuset(nic, 'sq_split')

    if cur_cpuset == mq_cpuset:
        return 'mq'
    elif cur_cpuset == sq_cpuset:
        return 'sq'
    elif cur_cpuset == sq_split_cpuset:
        return 'sq_split'
    else:
        raise Exception('tune mode not found')


def create_perftune_conf(cfg):
    """
    This function checks if a perftune configuration file should be created and
    creates it if so is the case, returning a boolean accordingly. It returns False
    if none of the perftune options are enabled in scylla_server file. If the perftune
    configuration file already exists, none is created.
    :return boolean indicating if perftune.py should be executed
    """
    params = ''
    if get_set_nic_and_disks_config_value(cfg) == 'yes':
        nic = cfg.get('IFNAME')
        if not nic:
            nic = 'eth0'
        params += '--tune net --nic "{nic}"'.format(nic=nic)

    if cfg.has_option('SET_CLOCKSOURCE') and cfg.get('SET_CLOCKSOURCE') == 'yes':
        params += ' --tune-clock'

    if len(params) > 0:
        if os.path.exists('/etc/scylla.d/perftune.yaml'):
            return True

        mode = get_tune_mode(nic)
        params += ' --mode {mode} --dump-options-file'.format(mode=mode)
        yaml = out('/opt/scylladb/scripts/perftune.py ' + params)
        with open('/etc/scylla.d/perftune.yaml', 'w') as f:
            f.write(yaml)
        return True
    else:
        return False

def is_valid_nic(nic):
    if len(nic) == 0:
        return False
    return os.path.exists('/sys/class/net/{}'.format(nic))

# Remove this when we do not support SET_NIC configuration value anymore


def get_set_nic_and_disks_config_value(cfg):
    """
    Get the SET_NIC_AND_DISKS configuration value.
    Return the SET_NIC configuration value if SET_NIC_AND_DISKS is not found (old releases case).
    :param cfg: sysconfig_parser object
    :return configuration value
    :except If the configuration value is not found
    """

    # Sanity check
    if cfg.has_option('SET_NIC_AND_DISKS') and cfg.has_option('SET_NIC'):
        raise Exception("Only one of 'SET_NIC_AND_DISKS' and 'SET_NIC' is allowed to be present")

    try:
        return cfg.get('SET_NIC_AND_DISKS')
    except Exception:
        # For backwards compatibility
        return cfg.get('SET_NIC')

def swap_exists():
    swaps = out('swapon --noheadings --raw')
    return True if swaps != '' else False

class SystemdException(Exception):
    pass


class systemd_unit:
    def __init__(self, unit):
        if is_nonroot():
            self.ctlparam = '--user'
        else:
            self.ctlparam = ''
        try:
            run('systemctl {} cat {}'.format(self.ctlparam, unit), silent=True)
        except subprocess.CalledProcessError:
            raise SystemdException('unit {} is not found or invalid'.format(unit))
        self._unit = unit

    def __str__(self):
        return self._unit

    def start(self):
        return run('systemctl {} start {}'.format(self.ctlparam, self._unit))

    def stop(self):
        return run('systemctl {} stop {}'.format(self.ctlparam, self._unit))

    def restart(self):
        return run('systemctl {} restart {}'.format(self.ctlparam, self._unit))

    def enable(self):
        return run('systemctl {} enable {}'.format(self.ctlparam, self._unit))

    def disable(self):
        return run('systemctl {} disable {}'.format(self.ctlparam, self._unit))

    def is_active(self):
        return out('systemctl {} is-active {}'.format(self.ctlparam, self._unit), exception=False)

    def mask(self):
        return run('systemctl {} mask {}'.format(self.ctlparam, self._unit))

    def unmask(self):
        return run('systemctl {} unmask {}'.format(self.ctlparam, self._unit))

    @classmethod
    def reload(cls):
        run('systemctl daemon-reload')

class sysconfig_parser:
    def __load(self):
        f = io.StringIO('[global]\n{}'.format(self._data))
        self._cfg = configparser.ConfigParser()
        self._cfg.optionxform = str
        self._cfg.readfp(f)

    def __escape(self, val):
        return re.sub(r'"', r'\"', val)

    def __add(self, key, val):
        self._data += '{}="{}"\n'.format(key, self.__escape(val))
        self.__load()

    def __init__(self, filename):
        self._filename = filename
        if not os.path.exists(filename):
            open(filename, 'a').close()
        with open(filename) as f:
            self._data = f.read()
        self.__load()

    def get(self, key):
        return self._cfg.get('global', key).strip('"')

    def has_option(self, key):
        return self._cfg.has_option('global', key)

    def set(self, key, val):
        if not self.has_option(key):
            return self.__add(key, val)
        self._data = re.sub('^{}=[^\n]*$'.format(key), '{}="{}"'.format(key, self.__escape(val)), self._data, flags=re.MULTILINE)
        self.__load()

    def commit(self):
        with open(self._filename, 'w') as f:
            f.write(self._data)
