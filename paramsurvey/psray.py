import os
import sys
import traceback
import json
import time

import ray
#import pyarrow

from . import utils
from . import stats
from . import pslogger


def read_ray_config():
    rayfile = os.environ.get('RAY_HEAD_FILE', None)
    if rayfile is None:  # pragma: no cover
        rayfile = os.path.expanduser('~/.ray-head-details')
    with open(rayfile) as f:
        address, password = f.read().split()
    return address, password


def init(system_kwargs, ncores=None, **kwargs):
    if ray.is_initialized():
        # this happens with our test scripts
        # also a user might call init() repeatedly, perhaps with inconsistant args
        pslogger.log('ray.init called repeatedly')
        pslogger.log('system_kwargs', repr(system_kwargs), stderr=False)
        pslogger.log('ncores', ncores, stderr=False)
        pslogger.log('kwargs', repr(kwargs), stderr=False)
        return

    ray_kwargs = {}

    if ncores:
        ray_kwargs['num_cpus'] = ncores

    # should allow these to be kwargs
    address, password = read_ray_config()
    kwargs['address'] = address
    kwargs['redis_password'] = password

    if os.environ.get('RAY_LOCAL_MODE', False):
        kwargs['local_mode'] = True

    # should we create a ray head if there is no cluster?
    ray.init(**kwargs)


def finalize():
    pslogger.finalize()
    ray.shutdown()


def current_core_count():
    cores = 0
    for node in ray.nodes():
        if not node.get('Alive', False):  # pragma: no cover
            continue
        cores += node.get('Resources', {}).get('CPU', 0)
    return int(cores)


@ray.remote
def do_work_wrapper(func, system_kwargs, user_kwargs, psets):
    if 'raise_in_wrapper' in system_kwargs and any(pset.get('actually_raise', False) for pset in psets):
        raise system_kwargs['raise_in_wrapper']  # for testing

    if 'out_subdirs' in system_kwargs:
        # the entire pset group gets the same out_subdir
        system_kwargs['out_subdir'] = utils.make_subdir_name(system_kwargs['out_subdirs'])

    # ray workers start at "cd ~"
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
            #print('saw an exception in the worker function', file=sys.stderr)
            #print('it was working on', json.dumps(pset, sort_keys=True), file=sys.stderr)
            #traceback.print_exc()
            user_ret['traceback'] = traceback.format_exc()
        ret.append([user_ret, system_ret])
    return ret


def handle_return(out_func, ret, system_stats, system_kwargs, user_kwargs):
    verbose = system_kwargs['verbose']
    progress = system_kwargs['progress']

    try:
        with stats.record_wallclock('ray.get', obj=system_stats):
            ret = ray.get(ret)
    except Exception as e:
        # RayTaskError has been seen here
        err = '\nSurprised exception in ray.get, an unknown number of results lost\n{}'.format(e)
        pslogger.log(err)
        pslogger.log(traceback.format_exc())
        return

    utils.handle_return_common(out_func, ret, system_stats, system_kwargs, user_kwargs)


'''
def check_serialized_size(args, factor=1.2):
    big_data = 10 * 1024 * 1024 * 1024  # TODO: make this dynamic with cluster resources
    cores = current_core_count()

    # this is apprently not the current method used in ray, because ray can
    # successfully pass ValueError() but pyarrow can't serialize it
    serialized_size = len(pyarrow.serialize(args).to_buffer())

    if serialized_size*cores*factor > big_data:
        print('warning: in-flight data size seems to be too big', file=sys.stderr)
    if serialized_size*cores*factor < big_data/3:
        print('due to small in-flight data size, goosing factor by 2x', file=sys.stderr)
        factor *= 2
    return factor
'''


def progress_until_fewer(futures, cores, factor, out_func, system_stats, system_kwargs, user_kwargs, group_size):
    verbose = system_kwargs['verbose']
    vstats = system_kwargs['vstats']
    progress = system_kwargs['progress']

    while len(futures) > cores*factor:
        t0 = time.time()
        done, pending = ray.wait(futures, num_returns=len(futures), timeout=1)
        elapsed = time.time() - t0

        print_nums = False
        if elapsed < 0.8:  # pragma: no cover
            if len(pending):
                # only observed at the end, when pending == 0
                err = 'something bad happened in ray.wait, normally 1.0 seconds, but it took {}'.format(elapsed)
                pslogger.log(err)
                print_nums = True

        if len(futures) != len(done) + len(pending):  # pragma: no cover
            # never observed
            pslogger.log('something bad happened in ray.wait, counts do not add up:')
            print_nums = True

        if verbose > 1 or print_nums:
            pslogger.log('futures {}, cores*factor {}, done {}, pending {}'.format(
                len(futures), cores*factor, len(done), len(pending)))

        futures = pending

        if len(done):
            if verbose and len(done) > 100:  # pragma: no cover
                # this indicates the driver isn't keeping up
                pslogger.log('surprised to see {} pset groups done at once'.format(len(done)))
            for ret in done:
                handle_return(out_func, ret, system_stats, system_kwargs, user_kwargs)
        else:  # not tested  # pragma: no cover
            progress.report()
            system_stats.report()

        new_cores = current_core_count()
        if new_cores != cores:  # not tested  # pragma: no cover
            if verbose:
                pslogger.log('core count changed from {} to {}'.format(cores, new_cores))
            cores = new_cores

        # dynamic group_size adjustment

    return futures, cores, group_size


def map(func, psets, out_func=None, system_kwargs=None, user_kwargs=None, chdir=None, out_subdirs=None,
        progress_dt=None, group_size=None, name='default', **kwargs):

    verbose = system_kwargs['verbose']
    vstats = system_kwargs['vstats']

    if utils.psets_empty(psets):
        return

    psets, system_stats, system_kwargs = utils.map_prep(psets, name, system_kwargs, chdir,
                                                        out_subdirs, progress_dt=progress_dt, **kwargs)
    if 'chdir' not in system_kwargs:
        # ray workers default to ~
        system_kwargs['chdir'] = os.getcwd()

    progress = system_kwargs['progress']
    cores = current_core_count()

    # make a cut-down copy to minimize size of args
    worker_system_kwargs = {}
    for key in ('out_subdirs', 'chdir', 'name'):
        if key in system_kwargs:
            worker_system_kwargs[key] = system_kwargs[key]

    # XXX temporarily diabled for Pandas
    #factor = check_serialized_size((func, worker_system_kwargs, user_kwargs, psets[0]), factor=1.2)
    factor = 2.4

    # temporary: this works in ray map calls, but check serialize raises on it
    if 'raise_in_wrapper' in system_kwargs:
        worker_system_kwargs['raise_in_wrapper'] = system_kwargs['raise_in_wrapper']

    if group_size is None:
        # make this dynamic someday
        group_size = 1

    pslogger.log('paramsurvey.psray map: psets {}, cores {}, group_size {}, verbose {}'.format(
        len(psets), cores, group_size, system_kwargs['verbose']
    ), stderr=verbose > 1)

    futures = []
    pset_index = 0

    while True:
        while len(futures) <= cores * factor:
            pset_group, pset_index = utils.get_pset_group(psets, pset_index, group_size)
            if len(pset_group) == 0:
                break

            pset_group, pset_ids = utils.make_pset_ids(pset_group)
            system_kwargs['pset_ids'].update(pset_ids)

            futures.append(do_work_wrapper.remote(func, worker_system_kwargs, user_kwargs, pset_group))
            progress.active += len(pset_group)
            progress.report()
            system_stats.report()

        if pset_index >= len(psets):
            break

        # cores and group_size can change within this function
        futures, cores, group_size = progress_until_fewer(futures, cores, factor, out_func, system_stats, system_kwargs, user_kwargs, group_size)

    pslogger.log('getting the residue, length', progress.active, stderr=verbose > 0)

    progress_until_fewer(futures, cores, 0, out_func, system_stats, system_kwargs, user_kwargs, group_size)

    return utils.map_finalize(name, system_kwargs, system_stats)
