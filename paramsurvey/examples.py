import time
from . import stats


def sleep_worker(pset, system_kwargs, user_kwargs, raw_stats):
    time.sleep(pset['duration'])
    return {'slept': pset['duration']}


def burn_worker(pset, system_kwargs, user_kwargs, raw_stats):
    start = time.time()
    with stats.record_wallclock('foo', raw_stats):
        while time.time() < start + pset['duration']:
            pass
    return {'burned': pset['duration']}
