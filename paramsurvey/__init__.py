import os
import sys
from pkg_resources import get_distribution, DistributionNotFound

from . import psmultiprocessing
from .utils import flatten_results


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

our_backend = None
our_verbose = None
default_backend = 'multiprocessing'


def init(backend=None, ncores=None, verbose=None, **kwargs):
    if backend is None:
        backend = os.environ.get('PARAMSURVEY_BACKEND', default_backend)

    if verbose or int(os.environ.get('PARAMSURVEY_VERBOSE', '0')) > 0:
        global our_verbose
        our_verbose = verbose or int(os.environ.get('PARAMSURVEY_VERBOSE', '0'))
        kwargs['verbose'] = verbose or our_verbose
        print('initializing paramsurvey {} backend'.format(backend), file=sys.stderr)

    global our_backend
    if backend in backends:
        our_backend = backends[backend]
        our_backend['name'] = backend
        if 'lazy' in our_backend:
            our_backend.update(our_backend['lazy']())
        our_backend['init'](ncores=ncores, **kwargs)
    else:  # pragma: no cover
        raise ValueError('unknown backend '+backend+', valid backends: '+', '.join(backends.keys()))


def backend():
    return our_backend['name']


def finalize(*args, **kwargs):
    if our_verbose:
        print('finalizing paramsurvey', file=sys.stderr)
    return our_backend['finalize'](*args, **kwargs)


def current_core_count(*args, **kwargs):
    return our_backend['current_core_count'](*args, **kwargs)


def map(*args, **kwargs):
    if our_verbose and 'verbose' not in kwargs:
        if our_verbose > 1:
            print('adding verbose= to map kwargs', file=sys.stderr)
        kwargs['verbose'] = our_verbose
    return our_backend['map'](*args, **kwargs)
