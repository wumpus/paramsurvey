import sys
from pkg_resources import get_distribution, DistributionNotFound

from . import psmultiprocessing
from .utils import flatten_results, initialize_kwargs, resolve_kwargs
from . import pslogger


try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:  # pragma: no cover
    __version__ = 'unknown'


def lazy_load_ray():
    try:
        from . import psray
    except ImportError:
        raise ValueError('Import of ray module failed, did you "pip install paramsurvey[ray]"?')
    return {
        'init': psray.init,
        'map': psray.map,
        'current_core_count': psray.current_core_count,
        'finalize': psray.finalize,
    }


def lazy_load_mpi():  # pragma: no cover
    try:
        from . import psmpi
    except ImportError:
        raise ValueError('Import of mpi module failed, did you "pip install paramsurvey[mpi]"?')
    return {
        'init': psmpi.init,
        'map': psmpi.map,
        'curent_core_count': psmpi.current_core_count,
        'finalize': psmpi.finalize,
    }


backends = {
    'multiprocessing': {
        'init': psmultiprocessing.init,
        'map': psmultiprocessing.map,
        'current_core_count': psmultiprocessing.current_core_count,
        'finalize': psmultiprocessing.finalize,
    },
    'ray': {
        'lazy': lazy_load_ray,
    },
    'mpi': {
        'lazy': lazy_load_mpi,
    },
}

global_kwargs = {}
default_backend = 'multiprocessing'


global_kwargs = {
    'verbose': {'env': 'PARAMSURVEY_VERBOSE', 'default': 1},
    'backend': {'env': 'PARAMSURVEY_BACKEND', 'default': default_backend, 'type': str},
    'limit': {'env': 'PARAMSURVEY_LIMIT', 'default': -1},
    'ncores': {'env': 'PARAMSURVEY_NCORES', 'default': -1},
    'vstats': {'env': 'PARAMSURVEY_VSTATS', 'default': 1},
}


def init(**kwargs):
    '''Initialize the paramsurvey system.

    Paramters
    ---------

    verbose : int, default 1
        Verbosity level for the paramsurvey system. 0=quiet, 1 = print some
        status every 30 seconds, 2 = print status every second, 3 = print
        status for every activity. Will be overridden by the environment
        variable `PARAMSURVEY_VERBOSE`, if set. The purppose of these environment
        variables is to aid debugging without editing your source code.
    backend : str, default 'multiprocessing'
        Which backend to use. Currently paramsurvey supports 'multiprocessing'
        and 'ray'. Will be overridden by the environment variable
        `PARAMSURVEY_BACKEND`, if set.
    limit : int, default -1
        Artifically limit the number of parameter sets computed. The default of
        `-1` means to compute everything. Useful for user testing. Will be
        overridden by the environment variable `PARAMSURVEY_VERBOSE`, if set.
    ncores : int, default -1
        The number of cores to use. The default of `-1` means to use all cores.
        Will be overridden by the environment variable `PARAMSURVEY_NCORES`, if set.
    vstats : int, default 1
        Similar to `verbose`, but for performance statistics reporting.
        Will be overridden by the environment variable `PARAMSURVEY_VSTATS`, if set.
    pslogger_prefix : str, default '.paramsurvey-'
        Specifies a prefix for the logging system filename.
    pslogger_fd : fd, optional
        Specifies an already-open stream for logging. Used in tests.

    Any additional keyword arguments will be passed to the `.init()` call
    for the backend.
    '''
    initialize_kwargs(global_kwargs, kwargs)
    verbose = global_kwargs['verbose']['value']
    backend = global_kwargs['backend']['value']
    pslogger.init(**kwargs)
    kwargs.pop('pslogger_prefix', None)
    kwargs.pop('pslogger_fd', None)

    global our_backend
    if backend in backends:
        pslogger.log('initializing paramsurvey {} backend'.format(backend), stderr=verbose)
        our_backend = backends[backend]
        our_backend['name'] = backend
        if 'lazy' in our_backend:
            our_backend.update(our_backend['lazy']())
        system_kwargs, other_kwargs = resolve_kwargs(global_kwargs, kwargs)
        our_backend['init'](system_kwargs, **other_kwargs)
    else:  # pragma: no cover
        raise ValueError('unknown backend '+backend+', valid backends: '+', '.join(backends.keys()))


def backend():
    '''Returns the paramsurvey backend in use.

    Returns
    -------
    str
    '''
    return our_backend['name']


def finalize(*args, **kwargs):
    '''Finalizes the paramsurvey run. Optional. Useful for situations
    like doing test coverage analysis with the multiprocessing module.
    '''
    return our_backend['finalize'](*args, **kwargs)


def current_core_count(*args, **kwargs):
    '''Returns the count of compute cpu cores in the current cluster.

    Returns
    -------
    int
    '''
    return our_backend['current_core_count'](*args, **kwargs)


def map(*args, **kwargs):
    '''Runs a worker function over a list of parameters, returning the results.

    Parameters
    ----------
    func : function
    psets : a pandas DataFrame or list of dicts
    out_func : function, optional
    system_kwargs : dict, optional
    user_kwargs : dict, optional
    chdir : str, optional
    out_subdirs : int, optional
    progress_dt : float, optional
    group_size : int, optional
    name : str, default 'default'

    Returns
    -------
    MapResults

    '''

    # help the user out a bit, in case there is buffering
    sys.stderr.flush()
    sys.stdout.flush()

    system_kwargs, other_kwargs = resolve_kwargs(global_kwargs, kwargs)
    return our_backend['map'](*args, system_kwargs=system_kwargs, **other_kwargs)
