import time
import sys
import uuid
import math
import random

from . import stats


class MapProgress(object):
    '''Class to track progress of a map()'''
    # would be perfect as a dataclass, once python 3.7 is our minimum
    def __init__(self, d={}):
        self.total = d.get('total', 0)
        self.started = d.get('started', 0)
        self.finished = d.get('finished', 0)
        self.failures = d.get('failures', 0)
        self.exceptions = d.get('exceptions', 0)

    def __str__(self):
        return ', '.join([k+': '+str(v) for k, v in vars(self).items()])


class MapResults(object):
    '''
    A container object for the outcome of paramsurvey.map()
    '''
    def __init__(self, results, missing, progress, stats):
        self._results = results
        self._results_flattened = None
        self._missing = missing
        self._progress = progress
        self._stats = stats

    @property
    def results(self):
        return self._results

    @property
    def results_flattened(self):
        if self._results_flattened is None:
            self._results_flattened = flatten_results(self._results)
        return self._results_flattened

    @property
    def missing(self):
        return self._missing

    @property
    def progress(self):
        return self._progress

    @property
    def stats(self):
        # stats.PerfStats
        return self._stats


def report_progress(system_kwargs, final=False):
    t = time.time()
    verbose = system_kwargs['verbose']

    force = bool(final or verbose > 1)

    if force or t - system_kwargs['progress_last'] > system_kwargs['progress_dt']:
        system_kwargs['progress_last'] = t
        print(system_kwargs['name'], 'progress:', str(system_kwargs['progress']),
              file=sys.stderr)

        if final and verbose and system_kwargs['pset_ids']:
            print('missing psets:', file=sys.stderr)
            for pset_id, pset in system_kwargs['pset_ids'].items():
                print(' ', pset, file=sys.stderr)

        sys.stderr.flush()


def remaining(system_kwargs):
    progress = system_kwargs['progress']
    return progress.started - progress.finished


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

    system_kwargs = {'progress': MapProgress({'total': len(psets)}), 'results': []}
    if chdir:
        system_kwargs['chdir'] = chdir
    if outfile:
        system_kwargs['outfile'] = outfile
    if out_subdirs:
        system_kwargs['out_subdirs'] = out_subdirs
    if name:
        system_kwargs['name'] = name
    system_kwargs['verbose'] = verbose or 0

    if 'raise_in_wrapper' in kwargs:
        system_kwargs['raise_in_wrapper'] = kwargs['raise_in_wrapper']

    if verbose:
        print('making pset ids', file=sys.stderr)
    psets, pset_ids = make_pset_ids(psets)
    system_kwargs['pset_ids'] = pset_ids

    system_stats = stats.PerfStats()
    system_kwargs['progress_last'] = 0.
    system_kwargs['progress_dt'] = 0.

    return psets, system_stats, system_kwargs


def flatten_results(results):
    seen_pset_keys = set()
    seen_result_keys = set()
    ret = []

    for r in results:
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
        if '_exception' in pset:
            print('warning: key _exception seen in a pset, this key is used by the paramsurvey system', file=sys.stderr)
        pset = pset.copy()  # essentially a 2-level copy of the user's list
        if '_pset_id' in pset:
            print('pset already has a _pset_id:', pset)
        pset_sans_id = pset.copy()
        pset_id = str(uuid.uuid4())  # flatten object because of serialization problems downstream
        pset_ids[pset_id] = pset_sans_id
        pset['_pset_id'] = pset_id
        ret.append(pset)
    return ret, pset_ids


def finalize_progress(system_kwargs):
    progress = system_kwargs['progress']
    failures = progress.failures
    actual_failures = len(system_kwargs['pset_ids'])

    # needed to fixup wrapper failures
    if actual_failures > failures:
        print('correcting failure count from {} to {}'.format(failures, actual_failures), file=sys.stderr)
        progress.failures = actual_failures
    elif actual_failures < failures:
        print('can\'t happen! missing pset_ids {} less than failures {}'.format(actual_failures, failures), file=sys.stderr)
    else:
        if system_kwargs['verbose'] > 1 and failures > 0:
            print('failures equal to actual failures, hurrah', file=sys.stderr)


def handle_return_common(out_func, ret, system_stats, system_kwargs, user_kwargs):
    progress = system_kwargs['progress']
    verbose = system_kwargs['verbose']
    for user_ret, system_ret in ret:
        if 'result' in user_ret and not isinstance(user_ret['result'], dict) and user_ret['result'] is not None:
            # fake an exception, make this case look like other failures
            if verbose > 1:
                print('user function did not return a dict. faking an exception that says that.', file=sys.stderr)
            user_ret['exception'] = "ValueError('user function did not return a dict: {}')".format(
                repr(user_ret['result']))
            user_ret['result'] = {}
        if 'raw_stats' in system_ret:
            system_stats.combine_stats(system_ret['raw_stats'])
        pset_id = user_ret['pset']['_pset_id']
        if 'exception' in user_ret:
            progress.failures += 1
            progress.exceptions += 1
            system_kwargs['pset_ids'][pset_id]['_exception'] = user_ret['exception']
            if verbose > 1:
                print('saw exception', user_ret['exception'], file=sys.stderr)
        else:
            del system_kwargs['pset_ids'][pset_id]
            user_ret['pset'].pop('_pset_id', None)
            system_kwargs['results'].append(user_ret)
            progress.finished += len(ret)
            if verbose > 1:
                print('finished: pset {} result {}'.format(repr(user_ret['pset']), repr(user_ret['result'])), file=sys.stderr)
        if out_func:
            out_func(user_ret, system_kwargs, user_kwargs)

    report_progress(system_kwargs)


def make_subdir_name(count, prefix='ps'):
    try:
        digits = math.ceil(math.log10(count))
    except Exception:
        print('count argument must be a number greater than 0', file=sys.stderr)
        raise

    return prefix + str(random.randint(0, count-1)).zfill(digits)
