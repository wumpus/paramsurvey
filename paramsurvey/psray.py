import os
import sys
import traceback
import json

import ray
import pyarrow

from . import utils
from . import stats
from .utils import MapResults


def read_ray_config():
    rayfile = os.environ.get('RAY_HEAD_FILE', None)
    if rayfile is None:  # pragma: no cover
        rayfile = os.path.expanduser('~/.ray-head-details')
    with open(rayfile) as f:
        address, password = f.read().split()
    return address, password


def init(verbose=False, **kwargs):
    ray_kwargs = {}

    if 'ncores' in kwargs:
        # what does num_cpus actually do if the the cluster pre-exists?
        ray_kwargs['num_cpus'] = kwargs['ncores']
        kwargs.pop('ncores')

    address, password = read_ray_config()
    kwargs['address'] = address
    kwargs['redis_password'] = password

    if 'ignore_reinit_error' not in kwargs:
        kwargs['ignore_reinit_error'] = True  # XXX needed for our test infra

    if os.environ.get('RAY_LOCAL_MODE', False):
        kwargs['local_mode'] = True

    # XXX if the cluster does not pre-exist, should we create it?
    ray.init(**kwargs)


def finalize():
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


def handle_return(out_func, ret, system_stats, system_kwargs, user_kwargs):
    progress = system_kwargs['progress']

    try:
        with stats.record_wallclock('ray.get', obj=system_stats):
            ret = ray.get(ret)
    except Exception as e:
        # RayTaskError has been seen here
        print('\nSurprised by exception {} getting a result,\n'
              'an unknown number of results lost\n'.format(e), file=sys.stderr)
        traceback.print_exc()
        sys.stderr.flush()
        progress.failures += 1
        utils.report_progress(system_kwargs)
        return

    utils.handle_return_common(out_func, ret, system_stats, system_kwargs, user_kwargs)


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


def progress_until_fewer(futures, cores, factor, out_func, system_stats, system_kwargs, user_kwargs, group_size):
    verbose = system_kwargs['verbose']

    while len(futures) > cores*factor:
        with stats.record_wallclock('ray.wait', obj=system_stats):
            done, pending = ray.wait(futures, num_returns=len(futures), timeout=1)

        if verbose > 1:
            print('until_fewer: futures {}, cores*factor {}, done {}, pending {}'.format(len(futures), cores*factor, len(done), len(pending)), file=sys.stderr)
            sys.stderr.flush()

        futures = pending

        if len(done):
            if verbose and len(done) > 100:
                print('surprised to see {} psets done at once'.format(len(done)), file=sys.stderr)
                sys.stderr.flush()
            for ret in done:
                handle_return(out_func, ret, system_stats, system_kwargs, user_kwargs)

        if verbose > 1:
            system_stats.bingo()

        new_cores = current_core_count()
        if new_cores != cores:
            print('core count changed from {} to {}'.format(cores, new_cores), file=sys.stderr)
            cores = new_cores
            sys.stderr.flush()

        # dynamic group_size adjustment

    return futures, cores, group_size


def map(func, psets, out_func=None, user_kwargs=None, chdir=None, outfile=None, out_subdirs=None,
        progress_dt=60., group_size=None, name='default', verbose=None, **kwargs):

    if utils.psets_empty(psets):
        return

    verbose = verbose or 0

    psets, system_stats, system_kwargs = utils.map_prep(psets, name, chdir, outfile, out_subdirs, verbose, **kwargs)
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
    factor = 1.2

    # temporary: this works in ray map calls, but check serialize raises on it
    if 'raise_in_wrapper' in system_kwargs:
        worker_system_kwargs['raise_in_wrapper'] = system_kwargs['raise_in_wrapper']

    if group_size is None:
        # make this dynamic someday
        group_size = 1

    if verbose:
        print('starting map: psets {}, cores {}, group_size {}, verbose {}'.format(
            len(psets), cores, group_size, system_kwargs['verbose']
        ), file=sys.stderr)
        sys.stderr.flush()

    futures = []
    pset_index = 0

    while True:
        while len(futures) < cores * factor:
            with stats.record_wallclock('get_pset_group', obj=system_stats):
                pset_group, pset_index = utils.get_pset_group(psets, pset_index, group_size)
            if len(pset_group) == 0:
                break

            pset_group, pset_ids = utils.make_pset_ids(pset_group)
            system_kwargs['pset_ids'].update(pset_ids)

            with stats.record_wallclock('ray.remote', obj=system_stats):
                futures.append(do_work_wrapper.remote(func, worker_system_kwargs, user_kwargs, pset_group))
            if verbose > 1:
                system_stats.bingo()
            progress.started += len(pset_group)
            utils.report_progress(system_kwargs)

        if pset_index >= len(psets):
            break

        # cores and group_size can change within this function
        futures, cores, group_size = progress_until_fewer(futures, cores, factor, out_func, system_stats, system_kwargs, user_kwargs, group_size)

    if verbose:
        print('getting the residue, length', utils.remaining(system_kwargs), file=sys.stderr)
        sys.stderr.flush()

    progress_until_fewer(futures, cores, 0, out_func, system_stats, system_kwargs, user_kwargs, group_size)

    if verbose:
        print('finished getting results', file=sys.stderr)
        sys.stderr.flush()

    utils.finalize_progress(system_kwargs)
    utils.report_progress(system_kwargs, final=True)

    system_stats.print_percentiles(name)
    missing = list(system_kwargs['pset_ids'].values())

    return MapResults(system_kwargs['results'], missing, system_kwargs['progress'], system_stats)
