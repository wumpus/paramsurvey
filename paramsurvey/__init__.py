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
    initialize_kwargs(global_kwargs, kwargs)
    verbose = global_kwargs['verbose']['value']
    backend = global_kwargs['backend']['value']
    pslogger_fd = kwargs.pop('pslogger_fd', None)

    global our_backend
    if backend in backends:
        if verbose:
            print('initializing paramsurvey {} backend'.format(backend), file=sys.stderr)
        our_backend = backends[backend]
        our_backend['name'] = backend
        if 'lazy' in our_backend:
            our_backend.update(our_backend['lazy']())
        system_kwargs, other_kwargs = resolve_kwargs(global_kwargs, kwargs)
        our_backend['init'](system_kwargs, **other_kwargs)
    else:  # pragma: no cover
        raise ValueError('unknown backend '+backend+', valid backends: '+', '.join(backends.keys()))

    pslogger.init(global_kwargs, pslogger_fd=pslogger_fd, **other_kwargs)


def backend():
    return our_backend['name']


def finalize(*args, **kwargs):
    return our_backend['finalize'](*args, **kwargs)


def current_core_count(*args, **kwargs):
    return our_backend['current_core_count'](*args, **kwargs)


def map(*args, **kwargs):
    system_kwargs, other_kwargs = resolve_kwargs(global_kwargs, kwargs)
    return our_backend['map'](*args, system_kwargs=system_kwargs, **other_kwargs)
