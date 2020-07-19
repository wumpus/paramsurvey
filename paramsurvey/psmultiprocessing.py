import os
import sys
import random
import traceback
import json
import functools
from collections import defaultdict
import multiprocessing

from . import utils
from . import stats

pool = None


def init(ncores=None):
    global pool
    if pool:  # yes we can be called multiple times
        return

    if ncores is None:
        ncores = multiprocessing.cpu_count()
    print('initializing multiprocessing pool with {} processes'.format(ncores))
    pool = multiprocessing.Pool(processes=ncores)


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


def do_work_wrapper(func, system_kwargs, user_kwargs, work_units):
    if 'out_subdirs' in system_kwargs:
        # the entire work unit group gets the same out_subdir
        system_kwargs['out_subdir'] = 'ray'+str(random.randint(0, system_kwargs['out_subdirs'])).zfill(5)

    # ray workers start at "cd ~"
    if 'chdir' in system_kwargs:
        os.chdir(system_kwargs['chdir'])

    name = system_kwargs['name']

    ret = []
    for work_unit in work_units:
        raw_stats = dict()
        system_ret = {'raw_stats': raw_stats}

        try:
            with stats.record_wallclock(name, raw_stats):
                user_ret = func(work_unit, system_kwargs, user_kwargs, raw_stats)
        except Exception as e:
            user_ret = {'work_unit': work_unit}
            system_ret['exception'] = str(e)
            print('saw an exception in the worker function', file=sys.stderr)
            print('it was working on', json.dumps(work_unit, sort_keys=True), file=sys.stderr)
            traceback.print_exc()
        ret.append([user_ret, system_ret])
    return ret


def handle_return(out_func, ret, system_stats, system_kwargs, user_kwargs):
    progress = system_kwargs['progress']
    progress['retired'] += len(ret)

    for user_ret, system_ret in ret:
        out_func(user_ret, system_kwargs, user_kwargs)
        if 'raw_stats' in system_ret:
            system_stats.combine_stats(system_ret['raw_stats'])
        if 'exception' in system_ret:
            progress['failures'] += 1

    utils.report_progress(system_kwargs)


def map(func, work, out_func=utils.accumulate_return, user_kwargs=None, chdir=None, outfile=None, out_subdirs=None,
        progress_dt=60., group_size=None, name='work_unit', **kwargs):
    if not work:
        return

    system_stats, system_kwargs = utils.map_prep(name, chdir, outfile, out_subdirs, len(work))

    do_partial = functools.partial(do_work_wrapper, func, system_kwargs, user_kwargs)

    # because of the ray implementation, our work is done in groups
    # use the chunksize feature in multiprocessing instead
    if group_size is not None:
        chunksize = group_size
    else:
        # for an hour-long run on a 4 core laptop, factor=100 divides the work into 36 second chunks
        chunksize = pick_chunksize(len(work), factor=100)

    # form our work into groups of length 1, to disable our groups feature
    work = [[x] for x in work]

    for ret in pool.imap_unordered(do_partial, work, chunksize):
        handle_return(out_func, ret, system_stats, system_kwargs, user_kwargs)

    print('finished getting results for', name)
    sys.stdout.flush()

    system_stats.print_histograms(name)

    if 'user_ret' in system_kwargs:
        return system_kwargs['user_ret']
