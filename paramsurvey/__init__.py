import warnings
import os

from astropy.utils.exceptions import ErfaWarning

from . import psmultiprocessing


def lazy_load_ray():
    from . import psray
    return {
        'init': psray.init,
        'map': psray.map,
        'current_core_count': psray.current_core_count,
    }


def lazy_load_mpi():
    from . import psmpi
    return {
        'init': psmpi.init,
        'map': psmpi.map,
        'curent_core_count': psmpi.current_core_count,
    }


backends = {
    'multiprocessing': {
        'init': psmultiprocessing.init,
        'map': psmultiprocessing.map,
        'current_core_count': psmultiprocessing.current_core_count,
    },
    'ray': {
        'lazy': lazy_load_ray,
    },
    'mpi': {
        'lazy': lazy_load_mpi,
    },
}

our_backend = None


def astropy_workarounds():
    # https://github.com/astropy/astropy/issues/9603
    # ERFA function "d2dtf" yielded N of "dubious year (Note 5)" [astropy._erfa.core]
    # ERFA function "utctai" yielded N of "dubious year (Note 3)" [astropy._erfa.core]
    # ERFA function "utcut1" yielded N of "dubious year (Note 3)" [astropy._erfa.core]
    warnings.simplefilter('ignore', ErfaWarning)


def init(backend=None, ncores=None, **kwargs):
    astropy_workarounds()

    if backend is None:
        if 'PARAMSURVEY_BACKEND' not in os.environ:  # pragma: no cover
            raise ValueError('must set PARAMSURVEY_BACKEND env var or pass in backend= to init')
        backend = os.environ['PARAMSURVEY_BACKEND']

    global our_backend
    if backend in backends:
        our_backend = backends[backend]
        if 'lazy' in our_backend:
            our_backend.update(our_backend['lazy']())
        print('our_backend', our_backend)
        our_backend['init'](ncores=ncores, **kwargs)
    else:  # pragma: no cover
        raise ValueError('unknown backend '+backend+', valid backends: '+', '.join(backends.keys()))


def map(*args, **kwargs):
    return our_backend['map'](*args, **kwargs)


def current_core_count(*args, **kwargs):
    return our_backend['current_core_count'](*args, **kwargs)
