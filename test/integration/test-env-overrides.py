import os
from io import StringIO

import paramsurvey
from paramsurvey.examples import sleep_worker

pslogger_fd = StringIO()


def test_all_env_variables(monkeypatch, capsys):
    var = {
        # everything but PARAMSURVEY_BACKEND, which is always set externally
        'PARAMSURVEY_VERBOSE': '3',
        'PARAMSURVEY_LIMIT': '1',
        'PARAMSURVEY_NCORES': '2',
        'PARAMSURVEY_MAX_TASKS_PER_CHILD': '2',
        'PARAMSURVEY_VSTATS': '3',
    }
    for k, v in var.items():
        monkeypatch.setenv(k, v)

    env_entries = 0
    for k, v in paramsurvey.global_kwargs.items():
        if 'env' in v:
            env_entries += 1
    assert len(var) == env_entries - 1, 'are we setting every env var'

    # this is separate from the other tests because the env vars are read at init time
    paramsurvey.init(pslogger_fd=pslogger_fd)

    duration = 0.1
    psets = [{'duration': duration}] * 10
    results = paramsurvey.map(sleep_worker, psets, verbose=0, vstats=0, limit=3, name='sleep with env var limit')
    assert len(results) == 1, 'env limit overrides code'

    captured = capsys.readouterr()
    assert len(captured.err) > 3, 'not very good test for verbosity'
