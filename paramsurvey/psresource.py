import resource
import os
import platform
import multiprocessing
from collections import defaultdict

import psutil

from . import pslogger


def vmem():
    # replace with pusutil.Process().memory_info().vms ?

    ru = resource.getrusage(resource.RUSAGE_SELF)
    gigs = ru[2]/(1024*1024)  # gigabytes
    if platform.system() == 'Darwin':
        gigs = gigs / 1024.
    return gigs


def resource_stats(worker=True):
    ret = {'hostname': platform.node(), 'pid': os.getpid()}
    vm = psutil.virtual_memory()
    ret['total'] = vm.total
    ret['available'] = vm.available
    ret['load1'] = psutil.getloadavg()[0] / multiprocessing.cpu_count()
    ret['worker'] = worker

    if platform.system() == 'Linux' or platform.system() == 'Darwin':
        p = psutil.Process()
        mfi = p.memory_full_info()  # takes 1 millisecond
        ret['uss'] = mfi.uss
        if hasattr(mfi, 'dirty'):
            ret['dirty'] = mfi.dirty  # Linux only
        if hasattr(mfi, 'swap'):
            ret['swap'] = mfi.swap  # Linux only
    return ret


memory_available_levels = {}


def _memory_complaint(hostname, hostnamep, resource_stats, verbose=1):
    mal = memory_available_levels.setdefault(hostname, [10, 5, 1, 0])
    if not mal:
        return
    av_pct = int(100 * resource_stats['available'] / resource_stats['total'])

    value = mal.pop(0)
    prev = 101
    while av_pct < value:
        prev = value
        value = mal.pop(0)
    mal.insert(0, value)

    if prev < 100:
        pslogger.log('{} memory available has fallen below {}%'.format(hostnamep, prev), stderr=verbose)


high_loadavg = defaultdict(float)


def _loadavg_complaint(hostname, hostnamep, resource_stats, verbose=1):
    load1 = resource_stats['load1']
    if load1 >= 2.:
        # log more visibly if it's greater than before
        if load1 > high_loadavg[hostname]:
            pslogger.log('{} load1-per-core average of {} is high'.format(hostnamep, load1), stderr=verbose)
            high_loadavg[hostname] = load1
        else:
            pslogger.log('{} load1-per-core average of {} is high'.format(hostnamep, load1), stderr=verbose > 2)
    else:
        if hostname in high_loadavg:
            del high_loadavg[hostname]
            pslogger.log('{} load1-per-core has returned to normal'.format(hostnamep), stderr=verbose)


def _other_complaint(hostname, hostnamep, resource_stats, verbose=1):
    if 'uss' not in resource_stats:
        return
    alarming = resource_stats['uss'] * 0.1

    if 'dirty' in resource_stats:
        if resource_stats['dirty'] > alarming:
            pct = int(100 * resource_stats['dirty'] / alarming)
            pslogger.log('{} dirty is an alarming {}%'.format(hostnamep, pct), stderr=verbose)
    if 'swap' in resource_stats:
        if resource_stats['swap'] > alarming:
            pct = int(100 * resource_stats['swap'] / alarming)
            pslogger.log('{} swap is an alarming {}%'.format(hostnamep, pct), stderr=verbose)


def resource_complaint(resource_stats, verbose=1):
    hostname = resource_stats['hostname']

    if resource_stats['worker']:
        hostnamep = hostname + ':' + str(resource_stats['pid'])
    else:
        hostnamep = 'driver'

    _memory_complaint(hostname, hostnamep, resource_stats, verbose=verbose)
    _loadavg_complaint(hostname, hostnamep, resource_stats, verbose=verbose)
    _other_complaint(hostname, hostnamep, resource_stats, verbose=verbose)


def memory_limits(raw=False):
    limits = {}

    # everywhere
    limits['available'] = psutil.virtual_memory().available

    # macos and windows don't have these, even though macos does support getrlimit
    try:
        p = psutil.Process()
        limits['rlimit_as'] = p.rlimit(psutil.RLIMIT_AS)[0]
        limits['rlimit_rss'] = p.rlimit(psutil.RLIMIT_RSS)[0]
    except AttributeError:
        pass

    # macos
    limits['rrlimit_rss'] = resource.getrlimit(resource.RLIMIT_RSS)[0]

    try:
        with open('/sys/fs/cgroup/memory/memory.limit_in_bytes') as f:
            cgroup = f.read()
            if len(cgroup) < 19:
                limits['cgroup'] = int(cgroup)
            else:
                # if improbably big, actually RLIM_INFINITY
                limits['cgroup'] = resource.RLIM_INFINITY
    except FileNotFoundError:
        pass

    raw_limits = limits.copy()

    for k, v in limits.copy().items():
        if v is None or v == resource.RLIM_INFINITY:
            del limits[k]

    lim = min([rl for rl in limits.values() if rl > 0])

    if raw:
        return lim, raw_limits
    return lim


suffix_table = {
    'k': 1024,
    'm': 1024**2,
    'g': 1024**3,
    't': 1024**4,
}


def memory_suffix(s):
    last = s[-1]
    if last.isalpha() and last.lower() in suffix_table:
        return int(s[:-1]) * suffix_table[last.lower()]
    return int(s)
