# how much memory does the CI have? need to fit... 1 gigabyte?
#  both ray and multiprocessing have some memory overhead...

import sys
import pytest
from io import StringIO

import paramsurvey
from paramsurvey import psresource
from paramsurvey.examples import sleep_worker


pslogger_fd = StringIO()


@pytest.fixture(scope="session")
def paramsurvey_init(request):
    paramsurvey.init(pslogger_fd=pslogger_fd)

    def finalize():
        # needed to get pytest multiprocessing coverage
        paramsurvey.finalize()

    request.session.addfinalizer(finalize)


def get_available():
    ret = psresource.resource_stats()
    available = ret['available']  # on the "main node", that's fine in the test harness
    if available < 100 * 1024**2:  # 100 megabytes
        print('Available memory looks low, but we\'ll try anyway: {:.1f} mb'.format(available/1024/1024), file=sys.stderr)
    return available


# when can I specify memory? init() or map(), usual dance with env variable PARAMSURVEY_MEMORY


def test_memory(capsys, paramsurvey_init):
    ncores = max(4, paramsurvey.current_core_count())

    duration = 0.1
    psets = [{'duration': duration}] * ncores * 4

    available = get_available()
    small_mem = min(available, 100000000) / (ncores + 1)
    overly_big_mem = available

    results = paramsurvey.map(sleep_worker, psets, name='overly_big_mem', memory=overly_big_mem)
    captured = capsys.readouterr()
    print(captured.err, file=sys.stderr)
    #assert False

    results = paramsurvey.map(sleep_worker, psets, name='small_mem', memory=small_mem)
    # the default case with trivial memory should have no memory complaints in pslogger
    captured = capsys.readouterr()
    residue = [x for x in captured.err.splitlines() if 'initializing' not in x]
    print(residue, file=sys.stderr)
    #assert not residue, 'the only line in stderr is the initiaizing line'

# want way too much, raises ValueError

# want so much that only 1 core used (or N for cluster)
# want so much that only 1/2 of cores can be used (or 1/2 for cluster)

# use more than we asked for:
#  should write a soliloquy to pslogger
#  not enforced so don't overdo it in the test

