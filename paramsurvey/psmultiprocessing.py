import os
import sys
import traceback
import json
import functools
import time
import multiprocessing

from . import utils
from . import stats
from .utils import MapResults

pool = None
our_ncores = None


def init(ncores=None, verbose=None):
    global pool
    global our_ncores
    if pool:  # yes we can be called multiple times  # pragma: no cover
        return

    if ncores is None:
        ncores = multiprocessing.cpu_count()
    if verbose:
        print('initializing multiprocessing pool with {} processes'.format(ncores), file=sys.stderr)
    pool = multiprocessing.Pool(processes=ncores)
    our_ncores = ncores


def finalize():
    # needed to make things like pytest coverage reporting work
    pool.close()
    pool.join()


def current_core_count():
    # XXX should be the pool size, if configured by init(ncores=)
    # XXX also affected by os.sched_getaffinity
    return multiprocessing.cpu_count()


def pick_chunksize(length, cores, factor=4):
    # chunksize computation similar to what Python does for a multiprocessing.Pool
    # except the fudge factor can be changed. bigger factor == smaller chunks.
    chunksize, extra = divmod(length, cores * factor)
    if extra:
        chunksize += 1
    return chunksize


def do_work_wrapper(func, system_kwargs, user_kwargs, psets):
    try:
        if 'raise_in_wrapper' in system_kwargs and any(pset.get('actually_raise', False) for pset in psets):
            raise system_kwargs['raise_in_wrapper']  # for testing

        if 'out_subdirs' in system_kwargs:
            # the entire pset group gets the same out_subdir
            system_kwargs['out_subdir'] = utils.make_subdir_name(system_kwargs['out_subdirs'])

        # multiprocesing workers start with parent's PWD so this probably won't get used
        if 'chdir' in system_kwargs:
            os.chdir(system_kwargs['chdir'])

        name = system_kwargs['name']

        ret = []
        for pset in psets:
            raw_stats = dict()
            system_ret = {'raw_stats': raw_stats}
            user_ret = {'pset': pset}

            try:
                with stats.record_wallclock(name+'_wall', raw_stats):
                    with stats.record_iowait(name+'_io', raw_stats):
                        result = func(pset, system_kwargs, user_kwargs, raw_stats)
                user_ret['result'] = result
            except Exception as e:
                user_ret['exception'] = repr(e)
                print('saw an exception in the worker function', file=sys.stderr)
                print('it was working on', json.dumps(pset, sort_keys=True), file=sys.stderr)
                traceback.print_exc()
            ret.append([user_ret, system_ret])
        return ret
    except Exception as e:
        print('\nException {} raised in the do_work_wrapper,\n'
              'an unknown number of results lost\n'.format(e), file=sys.stderr)
        traceback.print_exc()
        sys.stderr.flush()
        # cannot increment progress[failures] here because we are in the child & it is not returned
        # fake up a single return value
        user_ret = {'pset': psets[0], 'exception': repr(e)}
        return [[user_ret, {}]]


def callback(out_func, system_stats, system_kwargs, user_kwargs, ret):
    system_kwargs['outstanding'] -= 1
    utils.handle_return_common(out_func, ret, system_stats, system_kwargs, user_kwargs)


def error_callback(out_func, system_stats, system_kwargs, user_kwargs, ret):
    system_kwargs['outstanding'] -= 1
    system_kwargs['progress'].failures += 1
    print('error_callback, exception is', repr(ret), file=sys.stderr)


def progress_until_fewer(cores, factor, out_func, system_stats, system_kwargs, user_kwargs, group_size):
    verbose = system_kwargs['verbose']

    while system_kwargs['outstanding'] > cores*factor:
        time.sleep(0.1)
        if verbose > 1:
            system_stats.bingo()

    return group_size


def map(func, psets, out_func=None, user_kwargs=None, chdir=None, outfile=None, out_subdirs=None,
        progress_dt=60., group_size=None, name='default', verbose=None, **kwargs):

    if utils.psets_empty(psets):
        return

    verbose = verbose or 0

    psets, system_stats, system_kwargs = utils.map_prep(psets, name, chdir, outfile, out_subdirs, verbose, **kwargs)

    progress = system_kwargs['progress']
    cores = current_core_count()

    # make a cut-down copy to minimize size of args
    worker_system_kwargs = {}
    for key in ('raise_in_wrapper', 'out_subdirs', 'chdir', 'name'):
        if key in system_kwargs:
            worker_system_kwargs[key] = system_kwargs[key]

    factor = 2.4  # XXX should be set based on args size

    if group_size is None:
        # make this dynamic someday
        group_size = pick_chunksize(len(psets), cores, factor=100)
        if verbose > 1:
            print('initial group_size is', group_size, file=sys.stderr)

    callback_partial = functools.partial(callback, out_func, system_stats, system_kwargs, user_kwargs)
    error_callback_partial = functools.partial(error_callback, out_func, system_stats, system_kwargs, user_kwargs)

    system_kwargs['outstanding'] = 0
    pset_index = 0

    while True:
        while system_kwargs['outstanding'] < cores * factor:
            with stats.record_wallclock('get_pset_group', obj=system_stats):
                pset_group, pset_index = utils.get_pset_group(psets, pset_index, group_size)
            if len(pset_group) == 0:
                break

            pset_group, pset_ids = utils.make_pset_ids(pset_group)
            system_kwargs['pset_ids'].update(pset_ids)

            with stats.record_wallclock('multiprocessing.apply_async', obj=system_stats):
                pool.apply_async(do_work_wrapper,
                                 (func, worker_system_kwargs, user_kwargs, pset_group),
                                 {}, callback_partial, error_callback_partial)
            if verbose > 1:
                system_stats.bingo()
            system_kwargs['outstanding'] += 1
            progress.started += len(pset_group)
            utils.report_progress(system_kwargs)

        if pset_index >= len(psets):
            break

        # group_size can change within this function
        group_size = progress_until_fewer(cores, factor, out_func, system_stats, system_kwargs, user_kwargs, group_size)

    if verbose:
        print('getting the residue, length', utils.remaining(system_kwargs), file=sys.stderr)
        sys.stderr.flush()

    progress_until_fewer(cores, 0, out_func, system_stats, system_kwargs, user_kwargs, group_size)

    if verbose:
        print('finished getting results', file=sys.stderr)
        sys.stderr.flush()

    utils.finalize_progress(system_kwargs)
    utils.report_progress(system_kwargs, final=True)

    system_stats.print_percentiles(name)
    missing = list(system_kwargs['pset_ids'].values())

    return MapResults(system_kwargs['results'], missing, system_kwargs['progress'], system_stats)
