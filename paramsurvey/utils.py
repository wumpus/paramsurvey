import time
import sys
import uuid
import math
import random
import keyword
import resource
import os

import pandas as pd
from pandas_appender import DF_Appender

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
    def __init__(self, results, missing, progress, stats, verbose=0):
        if results is not None:
            self._results = results
        else:
            self._results = pd.DataFrame(None)
        self._results_as_dict = None
        self._missing = missing
        self._progress = progress
        self._stats = stats
        self._verbose = verbose

    @property
    def df(self):
        return self._results

    @property
    def verbose(self):
        return self._verbose

    def __iter__(self):
        return self._results.itertuples(index=False)

    def __len__(self):
        return self._results.shape[0]  # rows

    def to_listdict(self):
        # memory inefficient
        if not self._results_as_dict:
            size = self.df.memory_usage().sum()
            if self._verbose > 1 or self._verbose > 0 and size > 1000000:
                print('converting Pandas DataFrame to listdict, pandas size was {} bytes'.format(size))
            self._results_as_dict = self._results.to_dict(orient='records')
        return self._results_as_dict

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


'''
class PDFWrapper(object):
    def __init__(self, df, verbose=0):
        self.df = df
        self._as_dict = None
        self._verbose = verbose

    def __iter__(self):
        return self.df.itertuples(index=False)

    def __len__(self):
        #return self.df.shape[0]  # rows
        return len(self.df)

    def to_listdict(self):
        # memory inefficient
        if not self._as_dict:
            size = self.df.memory_usage().sum()
            if self._verbose > 1 or self._verbose > 0 and size > 1000000:
                print('converting Pandas DataFrame to listdict, size was {} bytes'.format(size))
            self._as_dict = self.pd.to_dict(orient='records')
        return self._as_dict
'''


def report_progress(system_kwargs, final=False):
    t = time.time()
    verbose = system_kwargs['verbose']

    force = bool(final or verbose > 1)

    if force or t - system_kwargs['progress_last'] > system_kwargs['progress_dt']:
        system_kwargs['progress_last'] = t
        print(system_kwargs['name'], 'progress:', str(system_kwargs['progress']),
              file=sys.stderr)

        if final and verbose and system_kwargs['pset_ids']:
            if verbose > 1:
                print('missing psets:', file=sys.stderr)
                for pset_id, pset in system_kwargs['pset_ids'].items():
                    print(' ', pset, file=sys.stderr)

        sys.stderr.flush()


def remaining(system_kwargs):
    progress = system_kwargs['progress']
    return progress.started - progress.finished


def get_pset_group(psets, pset_index, group_size):
    group = psets.iloc[pset_index:pset_index+group_size]
    pset_index += group_size

    return group, pset_index


def psets_prep(psets):
    if not isinstance(psets, pd.DataFrame):
        psets = pd.DataFrame(psets)

    for bad in ('_pset_id', '_exception'):
        if bad in psets.columns:
            raise ValueError('disallowed column name {} appears in pset'.format(bad))
    for c in psets.columns.values:
        if not c.isidentifier():
            raise ValueError('pset key "{}" is not a valid Python identifier; this cases problems with pandas'.format(c))
        if keyword.iskeyword(c):
            raise ValueError('pset key "{}" is a Python keyword; this cases problems with pandas'.format(c))
        if c.startswith('_'):
            raise ValueError('pset key "{}" starts with an underscore; this cases problems with pandas'.format(c))
    return psets


def map_prep(psets, name, system_kwargs, chdir, outfile, out_subdirs, keep_results=True, **kwargs):
    if system_kwargs['verbose'] > 0:
        print('starting work on', name, file=sys.stderr)
        sys.stderr.flush()

    psets = psets_prep(psets)
    if system_kwargs['limit'] >= 0:
        psets = psets.iloc[:system_kwargs['limit']]

    system_kwargs['progress'] = MapProgress({'total': len(psets)})

    if keep_results:
        system_kwargs['results'] = DF_Appender(ignore_index=True)
    if chdir:
        system_kwargs['chdir'] = chdir
    if outfile:
        system_kwargs['outfile'] = outfile
    if out_subdirs:
        system_kwargs['out_subdirs'] = out_subdirs
    if name:
        system_kwargs['name'] = name
    if 'raise_in_wrapper' in kwargs:
        system_kwargs['raise_in_wrapper'] = kwargs['raise_in_wrapper']

    system_kwargs['pset_ids'] = {}

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
    for pset in psets.itertuples(index=False):
        pset_sans_id = pset
        pset_id = str(uuid.uuid4())  # flatten object because of serialization problems downstream
        pset_ids[pset_id] = pset_sans_id._asdict().copy()
        new_pset = pset._asdict().copy()
        new_pset.update({'_pset_id': pset_id})
        ret.append(new_pset)

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

            new_row = user_ret['pset'].copy()
            if user_ret['result']:  # allowed to be None
                new_row.update(user_ret['result'])

            if 'results' in system_kwargs:
                system_kwargs['results'].append(new_row)

            progress.finished += 1
            if verbose > 1:
                print('finished: pset {} result {}'.format(repr(user_ret['pset']), repr(user_ret['result'])), file=sys.stderr)
        if out_func:
            out_func(user_ret, system_kwargs, user_kwargs)

    report_progress(system_kwargs)


def initialize_kwargs(global_kwargs, kwargs):
    for k, v in global_kwargs.items():
        value = None
        if v['env'] in os.environ:
            value = os.environ[v['env']]
            v['strong'] = True
        elif kwargs.get(k) is not None:
            value = kwargs.get(k)
        else:
            value = v.get('default')
        type = v.get('type', int)
        v['value'] = type(value)


def resolve_kwargs(global_kwargs, kwargs):
    # not quite perfect hack, so we can be verbose here
    verbose = kwargs.get('verbose', 0) or global_kwargs['verbose']

    system_kwargs = {}
    other_kwargs = {}

    for k in kwargs:
        if k in global_kwargs:
            gkw = global_kwargs[k]
            if gkw.get('strong'):
                if verbose:
                    print('environment variable overrides passed in value for', k, file=sys.stderr)
                system_kwargs[k] = gkw['value']
            else:
                system_kwargs[k] = kwargs[k]
        else:
            other_kwargs[k] = kwargs[k]
    for k in global_kwargs:
        if k not in system_kwargs:
            system_kwargs[k] = global_kwargs[k]['value']

    return system_kwargs, other_kwargs


def make_subdir_name(count, prefix='ps'):
    try:
        digits = math.ceil(math.log10(count))
    except Exception:
        print('count argument must be a number greater than 0', file=sys.stderr)
        raise

    return prefix + str(random.randint(0, count-1)).zfill(digits)


def psets_empty(psets):
    if isinstance(psets, pd.DataFrame):
        return psets.empty
    if not psets:
        return True


def vmem():
    ru = resource.getrusage(resource.RUSAGE_SELF)
    return ru[2]/1000000.  # gigabytes
