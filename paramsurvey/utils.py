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
    if final or 'verbose' in system_kwargs and system_kwargs.get('verbose', 0) > 1:
        force = True
    else:
        force = False
    if force or t - system_kwargs['progress_last'] > system_kwargs['progress_dt']:
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


def flatten_result(result, drop_exceptions=False):
    raise ValueError('untested')
    # promote pset and result but report clashes
    seen_pset_keys = set()
    seen_result_keys = set()
    ret = []

    for r in result:
        if 'exception' in r:
            if drop_exceptions:
                continue
            else:
                raise ValueError(
        if 'pset' in r:
            seen_pset_keys.add(r['pset'].keys())
        if 'result' in r:
            seen_result_keys.add(r['result'].keys())
        rr = r.get('pset', {})
        rr.update(r.et('result', {}))
        ret.append(rr)
    return ret
