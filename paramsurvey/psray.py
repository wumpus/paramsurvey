import os
import sys
import random
import traceback
import json

import ray
import pyarrow

from . import utils
from . import stats


def read_ray_config():
    rayfile = os.environ.get('RAY_HEAD_FILE', None)
    if rayfile is None:  # pragma: no cover
        rayfile = os.path.expanduser('~/.ray-head-details')
    with open(rayfile) as f:
        address, password = f.read().split()
    return address, password


def init(**kwargs):
    ray_kwargs = {}

    if 'ncores' in kwargs:
        # what does num_cpus actually do if the the cluster pre-exists?
        ray_kwargs['num_cpus'] = kwargs['ncores']
        kwargs.pop('ncores')

    address, password = read_ray_config()
    kwargs['address'] = address
    kwargs['redis_password'] = password

    if os.environ.get('RAY_LOCAL_MODE', False):
        kwargs['local_mode'] = True

    # XXX if the cluster does not pre-exist, should we create it?
    ray.init(**kwargs)


def finalize():
    pass


def current_core_count():
    cores = 0
    for node in ray.nodes():
        if not node.get('Alive', False):  # pragma: no cover
            continue
        cores += node.get('Resources', {}).get('CPU', 0)
    return int(cores)


@ray.remote
def do_work_wrapper(func, system_kwargs, user_kwargs, psets):
    if 'raise_in_wrapper' in system_kwargs and any('actually_raise' in pset for pset in psets):
        raise system_kwargs['raise_in_wrapper']  # for testing

    if 'out_subdirs' in system_kwargs:
        # the entire pset group gets the same out_subdir
        system_kwargs['out_subdir'] = 'ray'+str(random.randint(0, system_kwargs['out_subdirs'])).zfill(5)

    # ray workers start at "cd ~"
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


def handle_return(out_func, ret, system_stats, system_kwargs, user_kwargs):
    try:
        ret = ray.get(ret)
    except Exception as e:
        # RayTaskError has been seen here
        print('\nSurprised by exception {} getting a result,\n'
              'an unknown number of results lost\n'.format(e), file=sys.stderr)
        traceback.print_exc()
        sys.stderr.flush()
        progress = system_kwargs['progress']
        progress['failures'] += 1
        utils.report_progress(system_kwargs)
        return

    progress = system_kwargs['progress']
    progress['retired'] += len(ret)

    for user_ret, system_ret in ret:
        if 'result' in user_ret and not isinstance(user_ret['result'], dict) and user_ret['result'] is not None:
            raise ValueError('user function did not return a dict')
        out_func(user_ret, system_kwargs, user_kwargs)
        if 'raw_stats' in system_ret:
            system_stats.combine_stats(system_ret['raw_stats'])
        if 'exception' in user_ret:
            progress['failures'] += 1

    utils.report_progress(system_kwargs)


def check_serialized_size(args, factor=1.2):
    big_data = 10 * 1024 * 1024 * 1024  # TODO: make this dynamic with cluster resources
    cores = current_core_count()
    serialized_size = len(pyarrow.serialize(args).to_buffer())

    if serialized_size*cores*factor > big_data:
        print('warning: in-flight data size seems to be too big', file=sys.stderr)
    if serialized_size*cores*factor < big_data/3:
        print('due to small in-flight data size, goosing factor by 2x', file=sys.stderr)
        factor *= 2
    return factor


def progress_until_fewer(futures, cores, factor, out_func, system_stats, system_kwargs, user_kwargs, group_size):
    while len(futures) > cores*factor:
        done, pending = ray.wait(futures, num_returns=len(futures), timeout=1)
        futures = pending
        if len(done):
            for ret in done:
                handle_return(out_func, ret, system_stats, system_kwargs, user_kwargs)

        new_cores = current_core_count()
        if new_cores != cores:
            print('core count changed from {} to {}'.format(cores, new_cores), file=sys.stderr)
            cores = new_cores
            sys.stderr.flush()

        # dynamic group_size adjustment

    return futures, cores, group_size


def map(func, psets, out_func=utils.accumulate_return, user_kwargs=None, chdir=None, outfile=None, out_subdirs=None,
        progress_dt=60., group_size=None, name='pset', verbose=None, **kwargs):
    if not psets:
        return

    psets = psets.copy()  # we are going to be popping it

    system_stats, system_kwargs = utils.map_prep(name, chdir, outfile, out_subdirs, len(psets), verbose, **kwargs)

    progress = system_kwargs['progress']
    cores = current_core_count()
    factor = check_serialized_size((psets[0], user_kwargs), factor=1.2)

    if group_size is None:
        # make this dynamic someday
        group_size = 1

    if verbose:
        print('starting map, inital core count is', cores, file=sys.stderr)
        sys.stderr.flush()

    futures = []

    while psets:
        pset_group = utils.get_pset_group(psets, group_size)
        futures.append(do_work_wrapper.remote(func, system_kwargs, user_kwargs, pset_group))
        progress['started'] += len(pset_group)

        # cores and group_size can change within this function
        futures, cores, group_size = progress_until_fewer(futures, cores, factor, out_func, system_stats, system_kwargs, user_kwargs, group_size)

    if verbose:
        print('getting the residue, length', utils.remaining(system_kwargs), file=sys.stderr)
        sys.stderr.flush()

    progress_until_fewer(futures, cores, 0, out_func, system_stats, system_kwargs, user_kwargs, group_size)

    if verbose:
        print('finished getting results', file=sys.stderr)
        sys.stderr.flush()
    utils.report_progress(system_kwargs, final=True)

    system_stats.print_histograms(name)

    if 'user_ret' in system_kwargs:
        return system_kwargs['user_ret']
