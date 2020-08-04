import pytest
import sys

import paramsurvey
import paramsurvey.params
import paramsurvey.utils


@pytest.fixture(scope="session")
def paramsurvey_init(request):
    paramsurvey.init()

    def finalize():
        # needed to get pytest multiprocessing coverage
        print('paramsurvey.finalize called for session', file=sys.stderr)
        paramsurvey.finalize()

    request.session.addfinalizer(finalize)


def add_worker(pset, system_kwargs, user_kwargs, raw_stats):
    return {'c': pset['a'] + pset['b']}


def sums(n):
    a = range(n)
    b = range(n)

    psets = paramsurvey.params.product({'a': a, 'b': b})

    results = paramsurvey.map(add_worker, psets, name='stress_{}'.format(n))

    assert len(results.df) == n**2
    assert len(results.missing) == 0
    assert results.df['c'].sum() == n * n * (n-1)


def params(n):
    a = range(n)
    b = range(n)

    psets = paramsurvey.params.product({'a': a, 'b': b})
    return psets


def test_stress_10(paramsurvey_init):
    vmem0 = paramsurvey.utils.vmem()
    sums(10)  # 100
    vmem1 = paramsurvey.utils.vmem()
    assert vmem1 - vmem0 < 0.01  # gigabytes


def test_stress_100(paramsurvey_init):
    vmem0 = paramsurvey.utils.vmem()
    sums(50)  # 2,500 ... 6 seconds on 4 cores
    vmem1 = paramsurvey.utils.vmem()
    assert vmem1 - vmem0 < 0.01  # gigabytes


def test_param_stress():
    vmem0 = paramsurvey.utils.vmem()
    psets = params(1000)  # 1 million
    vmem1 = paramsurvey.utils.vmem()
    assert vmem1 - vmem0 < 0.1  # gigabytes

    vmem0 = paramsurvey.utils.vmem()
    psets = params(5000)  # 25 million
    vmem1 = paramsurvey.utils.vmem()
    assert vmem1 - vmem0 < 2.0
