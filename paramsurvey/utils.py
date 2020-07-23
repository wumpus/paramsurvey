import time
import sys
from collections import defaultdict
import uuid

from . import stats


class ProgressObject(object):
    def __init__(self, progress):
        self._progress = progress

    @property
    def total(self):
        return self._progress['total']

    @property
    def total(self):
        return self._progress['started']

    @property
    def finished(self):
        return self._progress['finished']

    @property
    def failures(self):
        return self._progress['failures']

    @property
    def exceptions(self):
        return self._progress['exceptions']


class ResultsObject(object):
    '''
    A container object for the outcome of paramsurvey.map()
    '''
    def __init__(self, results, missing, progress, stats):
        self._results = results
        self._missing = missing
        self._progress = progress
        self._stats = stats

    @property
    def results(self):
        return self._results

    @property
    def missing(self):
        return self._missing

    @property
    def progress(self):
        return ProgressObject(self._progress)

    @property
    def stats(self):
        # stats.StatsObject
        return self._stats


def report_progress(system_kwargs, final=False):
    t = time.time()
    force = bool(final or system_kwargs.get('verbose', 0) > 1)

    if force or t - system_kwargs['progress_last'] > system_kwargs['progress_dt']:
        system_kwargs['progress_last'] = t
        print(system_kwargs['name'], 'progress:',
              ', '.join([k+': '+str(v) for k, v in system_kwargs['progress'].items()]),
              file=sys.stderr)

        if final and system_kwargs['pset_ids']:
            print('left-over psets:', file=sys.stderr)
            for pset_id, pset in system_kwargs['pset_ids'].items():
                print(' ', pset, file=sys.stderr)

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


def map_prep(psets, name, chdir, outfile, out_subdirs, verbose, **kwargs):
    print('starting work on', name, file=sys.stderr)
    sys.stderr.flush()

    system_kwargs = {'results': []}
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

    psets, pset_ids = make_pset_ids(psets)
    system_kwargs['pset_ids'] = pset_ids

    system_stats = stats.StatsObject()
    progress = defaultdict(int)
    progress['total'] = len(psets)
    system_kwargs['progress'] = progress
    system_kwargs['progress_last'] = 0.
    system_kwargs['progress_dt'] = 0.

    return psets, system_stats, system_kwargs


def flatten_results(result, raise_if_exceptions=False):
    seen_pset_keys = set()
    seen_result_keys = set()
    ret = []

    for r in result:
        if 'exception' in r:
            if raise_if_exceptions:
                raise ValueError('Exception seen: '+r['exception'])
            else:
                continue
        if 'pset' in r:
            [seen_pset_keys.add(k) for k in r['pset'].keys()]
        if 'result' in r:
            [seen_result_keys.add(k) for k in r['result'].keys()]
        rr = r.get('pset', {}).copy()
        rr.update(r.get('result', {}))
        ret.append(rr)

    conflict = seen_pset_keys.intersection(seen_result_keys)
    if conflict:
        raise ValueError('conflicting key(s) seen in both pset and result: '+repr(conflict))

    return ret


def make_pset_ids(psets):
    pset_ids = {}
    ret = []
    for pset in psets:
        pset = pset.copy()  # essentially a 2-level copy of the user's list
        pset_id = str(uuid.uuid4())  # flatten object because of serialization problems downstream
        pset_ids[pset_id] = pset
        if '_pset_id' in pset:
            print('pset already has a _pset_id:', pset)
        pset['_pset_id'] = pset_id
        ret.append(pset)
    return ret, pset_ids


def finalize_progress(system_kwargs):
    progress = system_kwargs['progress']
    failures = progress.get('failures', 0)
    actual_failures = len(system_kwargs['pset_ids'])

    verbose = system_kwargs.get('verbose', 0)

    # needed to fixup wrapper failures
    if actual_failures > failures:
        print('correcting failure count from {} to {}'.format(failures, actual_failures), file=sys.stderr)
        progress['failures'] = actual_failures
    elif actual_failures < failures:
        print('can\'t happen! missing pset_ids {} less than failures {}'.format(actual_failures, failures), file=sys.stderr)
    else:
        if verbose > 1 and failures > 0:
            print('failures equal to actual failures, hurrah', file=sys.stderr)
