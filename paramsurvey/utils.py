import time
import sys
from collections import defaultdict

from . import stats


def accumulate_return(user_ret, system_kwargs, user_kwargs):
    if 'user_ret' not in system_kwargs:
        system_kwargs['user_ret'] = []
    system_kwargs['user_ret'].append(user_ret)


def report_progress(system_kwargs, final=False):
    t = time.time()
    if final or t - system_kwargs['progress_last'] > system_kwargs['progress_dt']:
        system_kwargs['progress_last'] = t
        print(system_kwargs['name'], 'progress:',
              ', '.join([k+': '+str(v) for k, v in system_kwargs['progress'].items()]),
              file=sys.stderr)
        sys.stderr.flush()


def remaining(system_kwargs):
    progress = system_kwargs['progress']
    return progress['started'] - progress.get('retired', 0)


def get_pset_group(psets, group_size):
    group = []
    for _ in range(group_size):
        try:
            group.append(psets.pop(0))
        except IndexError:
            pass
    return group


def map_prep(name, chdir, outfile, out_subdirs, psets_len, verbose, **kwargs):
    print('starting work on', name, file=sys.stderr)
    sys.stderr.flush()

    system_kwargs = {}
    if chdir:
        system_kwargs['chdir'] = chdir
    if outfile:
        system_kwargs['outfile'] = outfile
    if out_subdirs:
        system_kwargs['out_subdirs'] = out_subdirs
    if name:
        system_kwargs['name'] = name
    if verbose:
        system_kwargs['verbose'] = verbose

    if 'raise_in_wrapper' in kwargs:
        system_kwargs['raise_in_wrapper'] = kwargs['raise_in_wrapper']

    system_stats = stats.StatsObject()
    progress = defaultdict(int)
    progress['total'] = psets_len
    system_kwargs['progress'] = progress
    system_kwargs['progress_last'] = 0.
    system_kwargs['progress_dt'] = 0.

    return system_stats, system_kwargs
