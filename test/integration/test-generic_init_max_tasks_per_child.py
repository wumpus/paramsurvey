import os
from io import StringIO
import pytest

import paramsurvey
import paramsurvey.stats


pslogger_fd = StringIO()


def return_pid(pset, system_kwargs, user_kwargs):
    return {'pid': os.getpid()}


def test_init_max_tasks_per_child():
    # this test is in a separate file because it cannot share a session with test_generic
    paramsurvey.init(pslogger_fd=pslogger_fd, max_tasks_per_child=1)

    psets = [{'foo': 1}] * 10
    results = paramsurvey.map(return_pid, psets, name='init_max_tasks_per_child set')
    pids = set(r.pid for r in results.itertuples())
    assert len(pids) == len(results), 'init_max_tasks_per_child=1 has unique pids'

    with pytest.raises(ValueError):
        results = paramsurvey.map(return_pid, psets, max_tasks_per_child=1, name='map_max_tasks_per_child set')
