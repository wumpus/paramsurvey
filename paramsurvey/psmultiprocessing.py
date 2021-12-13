import os
import sys
import traceback
import functools
import time
import multiprocessing

import psutil

from . import utils
from . import stats
from . import pslogger

pool = None
our_ncores = None
finalize_needed = False
max_tasks_per_child_seen = False


def init(system_kwargs, backend_kwargs):
    global pool
    global our_ncores
    if pool:  # yes we can be called multiple times  # pragma: no cover
        return

    ncores = system_kwargs.pop('ncores', None)
    if ncores is None or ncores == 0:
        ncores = _core_count()
    elif ncores < 0:
        # negative ncores means subtract
        ncores = max(_core_count() + ncores, 1)
    backend_kwargs['processes'] = ncores
    our_ncores = ncores

    max_tasks_per_child = system_kwargs.pop('max_tasks_per_child', None)
    if max_tasks_per_child:
        backend_kwargs['maxtasksperchild'] = max_tasks_per_child
        global max_tasks_per_child_seen
        max_tasks_per_child_seen = True

    verbose = system_kwargs['verbose']
    pslogger.log('initializing multiprocessing pool with {} processes'.format(ncores), stderr=verbose)
    pslogger.log('Pool() kwargs are', backend_kwargs, stderr=verbose > 1)

    pool = multiprocessing.Pool(**backend_kwargs)
    global finalize_needed
    finalize_needed = True


def finalize():
    global finalize_needed
    if not finalize_needed:
        return
    finalize_needed = False

    pslogger.finalize()

    pool.close()

    # pytest expects that both pool.close and pool.join are called to collect coverage
    # pool.join makes sure that the children have all exited

    # pool.join tends to hang when used with max_tasks_per_child
    # https://bugs.python.org/issue10332
    # https://bugs.python.org/issue38799
    # and many more.

    # pool.join hangs if nested Pool __init__ has happened
    # can't find a bugs.python.org ticket

    if not max_tasks_per_child_seen:
        pool.join()


def _core_count():
    try:
        # recent Linux
        return len(os.sched_getaffinity(0))
    except (AttributeError, NotImplementedError, OSError):
        try:
            # Windows, MacOS, FreeBSD
            return len(psutil.Process().cpu_affinity())
        except (AttributeError, NotImplementedError, OSError):
            # older Linux, MacOS. Can raise NotImplementedError
            return multiprocessing.cpu_count()


def current_core_count():
    if our_ncores is not None:
        return our_ncores
    else:
        return _core_count()


def current_resources():
    # return a Ray-esque resource dict for our one node
    # in theory /sys/fs/cgroup/memory/ has a useful number, but sadly, it doesn't
    # XXX document find a gpu:
    # import torch
    # use_cuda = torch.cuda_is_available()
    return [{'num_cores': current_core_count(), 'memory': psutil.virtual_memory().total}]


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
            system_kwargs['raw_stats'] = raw_stats
            system_ret = {'raw_stats': raw_stats}
            user_ret = {'pset': pset}

            try:
                with stats.record_wallclock(name+'_wallclock', raw_stats):
                    with stats.record_iowait(name+'_iowait', raw_stats):
                        result = func(pset, system_kwargs, user_kwargs)
                user_ret['result'] = result
            except Exception as e:
                user_ret['exception'] = repr(e)
                user_ret['traceback'] = traceback.format_exc()
                #print('saw an exception in the worker function', file=sys.stderr)
                #print('it was working on', json.dumps(pset, sort_keys=True), file=sys.stderr)
                #traceback.print_exc()
            ret.append([user_ret, system_ret])
        return ret
    except Exception as e:
        err = ('\nException {} raised in the do_work_wrapper,\n'
               'an unknown number of results lost\n'.format(e))
        print(err, file=sys.stderr)
        traceback.print_exc()
        sys.stderr.flush()
        # cannot increment progress[failures] here because we are in the child & it is not returned
        # fake up a single return value
        user_ret = {'pset': psets[0], 'exception': repr(e)}
        user_ret['traceback'] = traceback.format_exc()
        return [[user_ret, {}]]


def callback(out_func, system_stats, system_kwargs, user_kwargs, ret):
    system_kwargs['outstanding'] -= 1
    utils.handle_return_common(out_func, ret, system_stats, system_kwargs, user_kwargs)


def error_callback(out_func, system_stats, system_kwargs, user_kwargs, e):
    system_kwargs['outstanding'] -= 1

    pslogger.log('python multiprocessing error_callback, exception is', repr(e))
    # do not raise here, it causes a hang
    # we do not know the pset, so we cannot fake a return value


def progress_until_fewer(cores, factor, out_func, system_stats, system_kwargs, user_kwargs, group_size):
    progress = system_kwargs['progress']

    while system_kwargs['outstanding'] > cores*factor:
        time.sleep(0.1)
        progress.report()
        system_stats.report()

    return group_size


def map(func, psets, out_func=None, system_kwargs=None, user_kwargs=None, chdir=None, out_subdirs=None,
        progress_dt=None, group_size=None, name='default', max_tasks_per_child=None, backend_kwargs={},
        **kwargs):

    verbose = system_kwargs['verbose']
    vstats = system_kwargs['vstats']

    if utils.psets_empty(psets):
        return

    if backend_kwargs:
        raise TypeError('multiprocessing.map does not currently take any backend kwargs')

    psets, system_stats, system_kwargs = utils.map_prep(psets, name, system_kwargs, chdir,
                                                        out_subdirs, progress_dt=progress_dt, **kwargs)

    if max_tasks_per_child is not None:
        pslogger.log('max_tasks_per_child={} is ignored by the multiprocessing map() call, do it in init()'.format(max_tasks_per_child), stderr=verbose)

    progress = system_kwargs['progress']
    cores = current_core_count()

    # make a cut-down copy to minimize size of args passed
    worker_system_kwargs = {}
    for key in ('raise_in_wrapper', 'out_subdirs', 'chdir', 'name'):
        if key in system_kwargs:
            worker_system_kwargs[key] = system_kwargs[key]

    factor = utils.pick_factor((func, worker_system_kwargs, user_kwargs, psets[0:0]))  # TODO: eventually will adjust group_size

    if group_size is None:
        # make this dynamic someday
        group_size = pick_chunksize(len(psets), cores, factor=100)
        pslogger.log('initial group_size is', group_size, stderr=verbose > 1)

    callback_partial = functools.partial(callback, out_func, system_stats, system_kwargs, user_kwargs)
    error_callback_partial = functools.partial(error_callback, out_func, system_stats, system_kwargs, user_kwargs)

    system_kwargs['outstanding'] = 0
    pset_index = 0

    while True:
        while system_kwargs['outstanding'] <= cores * factor:
            pset_group, pset_index = utils.get_pset_group(psets, pset_index, group_size)
            if len(pset_group) == 0:
                break

            pset_group, pset_ids = utils.make_pset_ids(pset_group)
            system_kwargs['pset_ids'].update(pset_ids)

            pool.apply_async(do_work_wrapper,
                             (func, worker_system_kwargs, user_kwargs, pset_group),
                             {}, callback_partial, error_callback_partial)
            system_kwargs['outstanding'] += 1
            progress.active += len(pset_group)
            progress.report()
            system_stats.report()

        if pset_index >= len(psets):
            break

        # group_size can change within this function
        group_size = progress_until_fewer(cores, factor, out_func, system_stats, system_kwargs, user_kwargs, group_size)

    pslogger.log('getting the residue, length', progress.active, stderr=verbose > 0)

    progress_until_fewer(cores, 0, out_func, system_stats, system_kwargs, user_kwargs, group_size)

    return utils.map_finalize(name, system_kwargs, system_stats)
