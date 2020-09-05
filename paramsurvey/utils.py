import time
import sys
import uuid
import math
import random
import keyword
import resource
import os
import datetime
import traceback
import warnings

import pandas as pd
from pandas_appender import DF_Appender

from . import stats
from . import pslogger


class MapProgress(object):
    '''Class to track progress of a map()'''
    # would be perfect as a dataclass, once python 3.7 is our minimum
    def __init__(self, name, d={}, verbose=1, progress_dt=None):
        self.name = name
        self.print_last = time.time()
        if progress_dt is not None:
            self.print_dt = progress_dt
        else:
            self.print_dt = self.pick_dt(verbose)
        self.print_log_last = time.time()
        self.print_log_dt = 30  # hardwired

        self.total = d.get('total', 0)
        self.active = d.get('active', 0)
        self.finished = d.get('finished', 0)
        self.failures = d.get('failures', 0)
        self.exceptions = d.get('exceptions', 0)

    def __str__(self):
        attrs = ('total', 'active', 'finished', 'failures', 'exceptions')
        return ', '.join([k+': '+str(getattr(self, k)) for k in attrs])

    def pick_dt(self, verbose):
        if verbose >= 3:
            return 0
        elif verbose >= 2:
            return 1
        elif verbose == 1:
            return 30
        else:
            return 1000000

    def finalize(self, verbose, missing):
        self.active = 0
        failures = self.failures
        actual_failures = len(missing)

        # fixup wrapper failures
        if actual_failures > failures:
            pslogger.log('correcting failure count from {} to {}'.format(failures, actual_failures))
            self.failures = actual_failures
        elif actual_failures < failures:
            pslogger.log('can\'t happen! missing pset_ids {} less than failures {}'.format(actual_failures, failures))
        else:
            if verbose > 1 and failures > 0:
                print('failures equal to actual failures, hurrah', file=sys.stderr)

    def report(self, final=None):
        t = time.time()
        last = t - self.print_last > self.print_dt
        log_last = t - self.print_log_last > self.print_log_dt

        if last:
            self.print_last = t
        if log_last:
            self.print_log_last = t

        if final or last or log_last:
            pslogger.log(self.name, 'progress:', str(self), stderr=final or last)


class MapResults(pd.DataFrame):
    '''
    A container object for the outcome of paramsurvey.map()

    Properties
    ----------

    to_df : DataFrame

    '''
    def __init__(self, results, missing, progress, stats, verbose=0):
        super().__init__(results)
        self._results_as_listdict = None
        with warnings.catch_warnings():
            # suppress "Pandas doesn't allow columns to be created via a new attribute name"
            warnings.simplefilter('ignore', category=UserWarning)
            self._missing = DFIterDictsWrapper(missing)
        self._progress = progress
        self._stats = stats
        self._verbose = verbose

    @property
    def verbose(self):
        return self._verbose

    @property
    def missing(self):
        return self._missing

    @property
    def progress(self):
        return self._progress

    @property
    def stats(self):
        return self._stats

    def to_df(self):
        return pd.DataFrame(self)

    def to_dict(self, orient=None):
        if orient != 'records':
            raise ValueError('only orient=records is supported')

        df = self.to_df()
        if not self._results_as_listdict:
            vmem0 = vmem()
            size = df.memory_usage().sum()
            if self._verbose > 1 or self._verbose > 0 and size > 1000000:
                pslogger.log('converting Pandas DataFrame to listdict, pandas size was {} bytes'.format(size))
            self._results_as_listdict = df.to_dict(orient='records')
            delta = vmem() - vmem0
            if self._verbose > 1 or self._verbose > 0 and delta > 1.:
                pslogger.log(' vmem increased by {} gigabytes'.format(delta))
        return self._results_as_listdict

    def iterdicts(self):
        return DFListDictIter(self.to_df())


class DFIterDictsWrapper(pd.DataFrame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def iterdicts(self):
        return DFListDictIter(self)

    def to_df(self):
        return pd.DataFrame(self)


class DFListDictIter:
    def __init__(self, df):
        self._it = df.itertuples(index=False)
        self._colmap = [(c, '_{}'.format(i)) for i, c in enumerate(df.columns)]

    def __iter__(self):
        return self

    def __next__(self):
        d = next(self._it)._asdict()
        for k1, k2 in self._colmap:
            if k2 in d:
                d[k1] = d.pop(k2)
        return d


def report_missing(missing, verbose):
    if missing:
        pslogger.log('missing psets:', stderr=verbose > 1)
        for pset in missing:
            pslogger.log(' ', pset, stderr=verbose > 1)


def get_pset_group(psets, pset_index, group_size):
    group = psets.iloc[pset_index:pset_index+group_size]
    pset_index += group_size

    return group, pset_index


def psets_prep(psets):
    if not isinstance(psets, pd.DataFrame):
        psets = pd.DataFrame(psets)

    for bad in ('_pset_id', '_exception', '_traceback'):
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


def map_prep(psets, name, system_kwargs, chdir, out_subdirs, keep_results=True, progress_dt=None, **kwargs):
    verbose = system_kwargs['verbose']
    vstats = system_kwargs['vstats']

    pslogger.log('paramsurvey.map start time', datetime.datetime.utcnow().strftime('%Y%m%d-%H%M%S'))
    pslogger.log('paramsurvey.map starting work on '+name, stderr=verbose)
    pslogger.log('paramsurvey.map system_kwargs '+repr(system_kwargs), stderr=verbose > 1)

    psets = psets_prep(psets)
    if system_kwargs['limit'] >= 0:
        psets = psets.iloc[:system_kwargs['limit']]

    system_kwargs['progress'] = MapProgress(name, {'total': len(psets)}, verbose=verbose, progress_dt=progress_dt)

    if keep_results:
        system_kwargs['results'] = DF_Appender(ignore_index=True)
    if chdir:
        system_kwargs['chdir'] = chdir
    if out_subdirs:
        system_kwargs['out_subdirs'] = out_subdirs
    if name:
        system_kwargs['name'] = name
    if 'raise_in_wrapper' in kwargs:
        system_kwargs['raise_in_wrapper'] = kwargs['raise_in_wrapper']

    system_kwargs['pset_ids'] = {}

    system_stats = stats.PerfStats(vstats=vstats)

    pslogger.log('paramsurvey.map pset count {}, pset columns {}'.format(len(psets), list(psets.columns.values)))

    return psets, system_stats, system_kwargs


def map_finalize(name, system_kwargs, system_stats):
    verbose = system_kwargs['verbose']
    vstats = system_kwargs['vstats']

    pslogger.log('finished getting results', stderr=verbose)

    progress = system_kwargs['progress']
    progress.finalize(verbose, system_kwargs['pset_ids'])
    progress.report(final=True)
    system_stats.report(final=True)

    missing = list(system_kwargs['pset_ids'].values())
    report_missing(missing, verbose)
    missing = pd.DataFrame(missing)

    if 'results' in system_kwargs:
        results = system_kwargs['results'].finalize()
    else:
        results = None

    pslogger.log('paramsurvey.map end time', datetime.datetime.utcnow().strftime('%Y%m%d-%H%M%S'))
    pslogger.log('paramsurvey.map returning results')

    return MapResults(results, missing, system_kwargs['progress'], system_stats)


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


def handle_return_common(out_func, ret, system_stats, system_kwargs, user_kwargs):
    progress = system_kwargs['progress']
    verbose = system_kwargs['verbose']
    vstats = system_kwargs['vstats']

    for user_ret, system_ret in ret:
        if 'result' in user_ret and not isinstance(user_ret['result'], dict) and user_ret['result'] is not None:
            # fake an exception, make this case look like other failures
            pslogger.log('user function did not return a dict. faking an exception that says that.', stderr=verbose > 1)
            user_ret['exception'] = "ValueError('user function did not return a dict: {}')".format(
                repr(user_ret['result']))
            user_ret['result'] = {}
        if 'raw_stats' in system_ret:
            system_stats.combine_stats(system_ret['raw_stats'])

        pset_id = user_ret['pset']['_pset_id']

        if 'exception' in user_ret:
            progress.failures += 1
            progress.exceptions += 1
            progress.active -= 1
            pslogger.log('saw exception in worker: ' + user_ret['exception'], stderr=verbose)
            pslogger.log('pset: '+repr(system_kwargs['pset_ids'][pset_id]), stderr=verbose > 1)

            system_kwargs['pset_ids'][pset_id]['_exception'] = user_ret['exception']

            if 'traceback' in user_ret:
                system_kwargs['pset_ids'][pset_id]['_traceback'] = user_ret['traceback']
                pslogger.log('traceback:\n' + user_ret['traceback'], stderr=verbose > 1)
        else:
            del system_kwargs['pset_ids'][pset_id]
            user_ret['pset'].pop('_pset_id', None)

            new_row = user_ret['pset'].copy()
            if user_ret['result']:  # allowed to be None
                new_row.update(user_ret['result'])

            if 'results' in system_kwargs:
                system_kwargs['results'].append(new_row)

            progress.finished += 1
            progress.active -= 1
            if verbose > 2:
                pslogger.log('finished: pset {} result {}'.format(repr(user_ret['pset']), repr(user_ret['result'])))

        if out_func:
            try:
                out_func(user_ret, system_kwargs, user_kwargs)
            except Exception as e:
                progress.exceptions += 1
                progress.failures += 1
                pslogger.log('saw exception in a user-supplied out_func:', repr(e), stderr=verbose)
                pslogger.log('traceback:\n' + traceback.format_exc(), stderr=verbose)

    progress.report()
    system_stats.report()


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
    verbose = kwargs.get('verbose', 0) or global_kwargs['verbose']['value']

    system_kwargs = {}
    other_kwargs = {}

    for k in kwargs:
        if k in global_kwargs:
            gkw = global_kwargs[k]
            if gkw.get('strong'):
                pslogger.log('environment variable overrides passed in value for', k, verbose > 0)
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
