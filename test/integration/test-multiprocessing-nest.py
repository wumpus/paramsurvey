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
    pool = multiprocessing.Pool(processes=2)  # should raise an exception for nesting


def test_multiprocessing_nesting(paramsurvey_init):
    os._exit = mock.MagicMock()

    psets = [{'foo': True}]
    results = paramsurvey.map(multiprocessing_worker, psets)
    # because we mocked os._exit(), this .map will fall through

    os._exit.assert_called_once_with(1)

    assert 'AssertionError' in pslogger_fd.getvalue()
    assert 'Traceback ' in pslogger_fd.getvalue()

    assert results.progress.total == 1
    assert results.progress.finished == 0
    assert results.progress.failures == 1
    assert results.progress.exceptions == 1
