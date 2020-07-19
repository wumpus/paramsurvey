import os
import sys

from . import psmultiprocessing


def lazy_load_ray():
    from . import psray
    return {
        'init': psray.init,
        'map': psray.map,
        'current_core_count': psray.current_core_count,
        'finalize': psray.finalize,
    }


def lazy_load_mpi():
    from . import psmpi
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

our_backend = None
our_verbose = None


def init(backend=None, ncores=None, verbose=None, **kwargs):
    if backend is None:
        backend = os.environ.get('PARAMSURVEY_BACKEND', 'multiprocessing')

    if verbose or os.environ.get('PARAMSURVEY_VERBOSE', 0) > 1:
        global our_verbose
        our_verbose = verbose or os.environ.get('PARAMSURVEY_VERBOSE', 0)
        print('initializing paramsurvey {} backend'.format(backend), file=sys.stderr)

    global our_backend
    if backend in backends:
        our_backend = backends[backend]
        if 'lazy' in our_backend:
            our_backend.update(our_backend['lazy']())
        print('our_backend', our_backend)
        our_backend['init'](ncores=ncores, **kwargs)
    else:  # pragma: no cover
        raise ValueError('unknown backend '+backend+', valid backends: '+', '.join(backends.keys()))


def finalize(*args, **kwargs):
    if our_verbose:
        print('finalizing paramsurvey', file=sys.stderr)
    return our_backend['finalize'](*args, **kwargs)


def current_core_count(*args, **kwargs):
    return our_backend['current_core_count'](*args, **kwargs)


def map(*args, **kwargs):
    if our_verbose and 'verbose' not in kwargs:
        kwargs['verbose'] = our_verbose
    return our_backend['map'](*args, **kwargs)
