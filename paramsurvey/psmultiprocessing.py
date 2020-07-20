import os
import sys
import random
import traceback
import json
import functools
import multiprocessing

from . import utils
from . import stats

pool = None


def init(ncores=None, verbose=None):
    global pool
    if pool:  # yes we can be called multiple times  # pragma: no cover
        return

    if ncores is None:
        ncores = multiprocessing.cpu_count()
    if verbose:
        print('initializing multiprocessing pool with {} processes'.format(ncores), file=sys.stderr)
    pool = multiprocessing.Pool(processes=ncores)


def finalize():
    # needed to make things like pytest coverage reporting work
    pool.close()
    pool.join()


def current_core_count():
    # XXX should be the pool size
    # XXX also affected by os.sched_getaffinity
    return multiprocessing.cpu_count()


def pick_chunksize(length, factor=4):
    # default chunksize computation similar to what Python does for a multiprocessing.Pool
    # except the fudge factor can be changed. bigger == smaller chunks.
    # for an hour-long run on a 4 core laptop, factor=100 divides the work into 36 second chunks
    cores = multiprocessing.cpu_count()
    chunksize, extra = divmod(length, cores * factor)
    if extra:
        chunksize += 1
    return chunksize


def do_work_wrapper(func, system_kwargs, user_kwargs, psets):
    try:
        if 'raise_in_wrapper' in system_kwargs and 'actually_raise' in psets[0]:
            raise system_kwargs['raise_in_wrapper']  # for testing

        if 'out_subdirs' in system_kwargs:
            # the entire pset group gets the same out_subdir
            system_kwargs['out_subdir'] = 'ray'+str(random.randint(0, system_kwargs['out_subdirs'])).zfill(5)

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
                with stats.record_wallclock(name, raw_stats):
                    result = func(pset, system_kwargs, user_kwargs, raw_stats)
                user_ret['result'] = result
            except Exception as e:
                user_ret['exception'] = str(e)
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
        user_ret = {'pset': psets[0], 'exception': str(e)}
        return [[user_ret, {}]]


def handle_return(out_func, ret, system_stats, system_kwargs, user_kwargs):
    progress = system_kwargs['progress']
    progress['retired'] += len(ret)

    for user_ret, system_ret in ret:
        if 'result' in user_ret and not isinstance(user_ret['result'], dict) and user_ret['result'] is not None:
            raise ValueError('user function did not return a dict: '+repr(user_ret['result']))
        out_func(user_ret, system_kwargs, user_kwargs)
        if 'raw_stats' in system_ret:
            system_stats.combine_stats(system_ret['raw_stats'])
        if 'exception' in user_ret:
            progress['failures'] += 1

    utils.report_progress(system_kwargs)


def map(func, psets, out_func=utils.accumulate_return, user_kwargs=None, chdir=None, outfile=None, out_subdirs=None,
        progress_dt=60., group_size=None, name='default', verbose=None, **kwargs):
    if not psets:
        return

    system_stats, system_kwargs = utils.map_prep(name, chdir, outfile, out_subdirs, len(psets), verbose, **kwargs)

    do_partial = functools.partial(do_work_wrapper, func, system_kwargs, user_kwargs)

    # because of the ray implementation, our work is done in groups
    # use the chunksize feature in multiprocessing instead
    if group_size is not None:
        chunksize = group_size
    else:
        # for an hour-long run on a 4 core laptop, factor=100 divides the work into 36 second chunks
        chunksize = pick_chunksize(len(psets), factor=100)

    # form our work into groups of length 1, to disable our groups feature
    psets = [[x] for x in psets]

    if verbose:
        print('starting map, chunksize is', chunksize, file=sys.stderr)
        sys.stderr.flush()

    for ret in pool.imap_unordered(do_partial, psets, chunksize):
        if ret is not None:
            handle_return(out_func, ret, system_stats, system_kwargs, user_kwargs)

    if verbose:
        print('finished getting results for', name, file=sys.stderr)
        sys.stderr.flush()
    utils.report_progress(system_kwargs, final=True)

    system_stats.print_histograms(name)

    if 'user_ret' in system_kwargs:
        return system_kwargs['user_ret']
