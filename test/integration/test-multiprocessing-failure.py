import pytest
import sys

import paramsurvey
from paramsurvey.examples import sleep_worker


@pytest.fixture(scope="session")
def paramsurvey_init(request):
    paramsurvey.init(ncores=2)


def test_multiprocessing_error_callback(paramsurvey_init):
    if paramsurvey.backend() != 'multiprocessing':
        pytest.skip('only valid for multiprocessing backend')

    psets = [{'fd': sys.stdout, 'duration': 0.1}]  # won't pickle, calls the error_callback
    results = paramsurvey.map(sleep_worker, psets)
    assert results.progress.total == 1
    assert results.progress.finished == 0
    assert results.progress.failures == 1
