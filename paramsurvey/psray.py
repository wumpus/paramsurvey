import os
import sys
import traceback
import json
import time

import ray

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


def init(system_kwargs, backend_kwargs):
    if ray.is_initialized():
        # this happens with our test scripts
        # also a user might call init() repeatedly, perhaps with inconsistant args
        pslogger.log('init.ray called repeatedly')
        return

    verbose = system_kwargs['verbose']

    ncores = system_kwargs.pop('ncores', None)
    if ncores is not None:
        pslogger.log('init.ray ncores={} ignored'.format(ncores), stderr=verbose)
        # when we actually are willing to start ray here, pay attention to ncores

    max_tasks_per_child = system_kwargs.pop('max_tasks_per_child', None)
    if max_tasks_per_child:
        if verbose:
            pslogger.log('init.ray max_tasks_per_child=', max_tasks_per_child, stderr=verbose > 1)

        # the docs say this should work, but it gets: TypeError: options() got an unexpected keyword argument 'max_calls'
        # paramsurvey.psray.do_work_wrapper = paramsurvey.psray.do_work_wrapper.options(max_calls=max_tasks_per_child)

        # this is a hack that works, if it is used before the workers are instantiated
        do_work_wrapper._max_calls = max_tasks_per_child

    if 'address' not in backend_kwargs:
        address, password = read_ray_config()
        backend_kwargs['address'] = address
        backend_kwargs['_redis_password'] = password  # added the leading _ in ray 1.0.0

    if os.environ.get('RAY_LOCAL_MODE', False):
        backend_kwargs['local_mode'] = True

    # should we create a ray head if there is no cluster?
    ray.init(**backend_kwargs)


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


def current_resources():
    # slurm: --mem 4g --gres=gpu:1 
    # 'Resources': {'object_store_memory': 112329590784.0, 'memory': 252102378496.0, 'accelerator_type:V100': 1.0, 'GPU': 1.0, 'node:10.31.143.116': 1.0, 'CPU': 32.0}}
    # as you can see the memory limit is not visible

    nodes = []
    for node in ray.nodes():
        if not node.get('Alive', False):  # pragma: no cover
            continue
        resources = node.get('Resources', {})
        this_node = {}
        for k1, k2 in (('num_cores', 'CPU'), ('num_gpus', 'GPU'), ('memory', 'memory')):
            if k2 in resources:
                this_node[k1] = resources[k2]
        # somewhere between ray 0.X and ray 1.2, memory changed from units of 50 megabytes to units of bytes
        nodes.append(this_node)
    return nodes


@ray.remote
def do_work_wrapper(func, system_kwargs, user_kwargs, psets):
    if 'raise_in_wrapper' in system_kwargs and any(pset.get('actually_raise', False) for pset in psets):
        raise system_kwargs['raise_in_wrapper']  # for testing

    if 'out_subdirs' in system_kwargs:
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
        progress_dt=None, group_size=None, name='default', backend_kwargs={}, **kwargs):

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

    # make a cut-down copy to minimize size of args passed
    worker_system_kwargs = {}
    for key in ('raise_in_wrapper', 'out_subdirs', 'chdir', 'name'):
        if key in system_kwargs:
            worker_system_kwargs[key] = system_kwargs[key]

    factor = utils.pick_factor((func, worker_system_kwargs, user_kwargs, psets[0:0]))  # TODO: eventually will adjust group_size

    if group_size is None:
        # make this dynamic someday
        group_size = 1

    pslogger.log('paramsurvey.psray map: psets {}, cores {}, group_size {}, verbose {}'.format(
        len(psets), cores, group_size, system_kwargs['verbose']
    ), stderr=verbose > 1)

    wrapper = do_work_wrapper

    if backend_kwargs:
        wrapper = wrapper.options(**backend_kwargs)

    futures = []
    pset_index = 0

    while True:
        while len(futures) <= cores * factor:
            pset_group, pset_index = utils.get_pset_group(psets, pset_index, group_size)
            if len(pset_group) == 0:
                break

            if 'out_subdirs' in system_kwargs:
                if len(pset_group) > 1:
                    raise ValueError('out_subdirs does not work with pset groups')

            pset_group, pset_ids = utils.make_pset_ids(pset_group)
            system_kwargs['pset_ids'].update(pset_ids)

            our_wrapper = wrapper
            if 'ray' in pset_group[0]:
                if len(pset_group) > 1:
                    raise ValueError('pset ray overrides are not allowed in groups')

                # name, memory, num_cpus, num_gpus
                r = pset_group[0]['ray']
                if 'num_cores' in r:
                    r['num_cpus'] = r.pop('num_cores')
                our_wrapper = wrapper.options(**r)
                worker_system_kwargs['ray'] = r

            futures.append(our_wrapper.remote(func, worker_system_kwargs, user_kwargs, pset_group))
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
