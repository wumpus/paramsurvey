import multiprocessing
from io import StringIO
import os
from unittest import mock
import pytest

import paramsurvey

pslogger_fd = StringIO()


@pytest.fixture(scope="session")
def paramsurvey_init(request):
    paramsurvey.init(ncores=2, pslogger_fd=pslogger_fd)


def multiprocessing_worker(pset, system_kwargs, user_kwargs):
    assert os.environ['OMP_NUM_THREADS'] == '3'


def test_multiprocessing_omp(paramsurvey_init):
    assert os.environ['OMP_NUM_THREADS'] == '3'

    psets = [{'foo': True}]
    results = paramsurvey.map(multiprocessing_worker, psets)

    assert results.progress.total == 1
    assert results.progress.finished == 1
    assert results.progress.failures == 0
    assert results.progress.exceptions == 0
