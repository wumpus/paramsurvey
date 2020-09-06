import time
import os
import tempfile
import pytest
import sys
from io import StringIO

import pandas as pd

import paramsurvey
import paramsurvey.stats
from paramsurvey.examples import sleep_worker, burn_worker


pslogger_fd = StringIO()


@pytest.fixture(scope="session")
def paramsurvey_init(request):
    paramsurvey.init(pslogger_fd=pslogger_fd)

    def finalize():
        # needed to get pytest multiprocessing coverage
        paramsurvey.finalize()

    request.session.addfinalizer(finalize)


def readouterr_and_dump(capsys):
    captured = capsys.readouterr()
    # this dumping is useful when there are failures to debug
    sys.stdout.write('Dumped stdout:\n' + captured.out)
    sys.stderr.write('Dumped stderr:\n' + captured.err)
    return captured


def test_basics(paramsurvey_init):
    ncores = max(4, paramsurvey.current_core_count())

    duration = 0.1
    psets = [{'duration': duration}] * ncores * 4

    start = time.time()
    results = paramsurvey.map(sleep_worker, psets, name='simple')
    elapsed = time.time() - start
    assert elapsed > duration, 'must take at least {} time'.format(duration)

    assert [r.slept == duration for r in results.itertuples()], 'everyone slept '+str(duration)
    assert len(results) == len(psets), 'one return for each pset'
    assert len(results.missing) == 0
    assert results.progress.total == len(psets)
    assert results.progress.active == 0
    assert results.progress.finished == len(psets)
    assert results.progress.failures == 0
    assert results.progress.exceptions == 0
    assert isinstance(results.verbose, int)

    df_as_listdict = results.to_dict('records')
    assert len(df_as_listdict) == len(results)
    assert [d['slept'] == duration for d in df_as_listdict]

    results.stats.print_histograms()

    psets = psets[:ncores]
    start = time.time()
    results = paramsurvey.map(sleep_worker, psets, name='group_size 5', group_size=5)
    elapsed = time.time() - start
    assert elapsed > duration*3, 'must take at least {} time'.format(duration)
    assert len(results) == len(psets)
    assert results.progress.total == len(psets)
    assert results.progress.finished == len(psets)
    assert results.progress.failures == 0
    assert results.progress.exceptions == 0

    assert [r.slept == duration for r in results.itertuples()], 'everyone slept '+str(duration)
    assert len(results) == len(psets), 'one return for each pset'

    start = time.time()
    results = paramsurvey.map(burn_worker, psets, name='burn group_size 4', group_size=4)
    elapsed = time.time() - start
    assert elapsed > duration*3, 'must take at least {} time'.format(duration)

    assert [r.burned == duration for r in results.itertuples()], 'everyone burned '+str(duration)
    assert len(results) == len(psets), 'one return for each pset'

    results = paramsurvey.map(sleep_worker, psets, name='sleep_no_results', keep_results=False)
    assert len(results) == 0


def test_results(paramsurvey_init):
    ncores = max(4, paramsurvey.current_core_count())

    duration = 0.1
    psets = [{'duration': duration}] * ncores * 4

    results = paramsurvey.map(sleep_worker, psets, name='simple')

    df = results.to_df()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(results)
    with pytest.raises(ValueError):
        results.to_dict()
    with pytest.raises(ValueError):
        results.to_dict(orient='dict')
    assert [r.slept for r in results.itertuples()] == [r['slept'] for r in results.iterdicts()]

    assert len(results.missing) == 0
    for d in results.missing.iterdicts():
        assert False, 'this should be empty'

    df = results.missing.to_df()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(results.missing)


def do_test_args(pset, system_kwargs, user_kwargs):
    # this function cannot be nested inside test_args() because nested funcs can't be pickled
    assert os.getcwd() == user_kwargs['expected_cwd'], 'chdir appears to work'
    assert 'out_subdir' in system_kwargs


def test_args(capsys, paramsurvey_init):
    chdir = tempfile.gettempdir()
    if os.getcwd() == chdir:
        # whoops
        pass

    out_func_called = False
    test_user_kwargs = {'test': 1, 'expected_cwd': chdir}

    def out_func(user_ret, system_kwargs, user_kwargs):
        nonlocal out_func_called
        out_func_called = True
        user_kwargs['out_func_called'] = True
        assert user_kwargs == test_user_kwargs

    psets = [{'duration': 0.1}] * 2

    name = 'test_args dt=0'
    results = paramsurvey.map(do_test_args, psets,
                              out_func=out_func, user_kwargs=test_user_kwargs,
                              chdir=chdir, out_subdirs=10,
                              progress_dt=0., name=name)

    assert out_func_called
    assert test_user_kwargs.get('out_func_called')
    assert len(results) == 2

    captured = readouterr_and_dump(capsys)
    assert len(captured.err.splitlines()) >= len(psets)

    # because of progress_dt being 0., we should have at least len(psets) progress lines
    has_name = [line for line in captured.err.splitlines() if 'progress' in line and name in line]
    assert len(has_name) >= len(psets)

    # same as previous but verbose=3 instead of progress_dt=0
    name = 'test_args verbose=3'
    results = paramsurvey.map(do_test_args, psets,
                              out_func=out_func, user_kwargs=test_user_kwargs,
                              chdir=chdir, out_subdirs=10,
                              verbose=3, name=name)

    assert out_func_called
    assert test_user_kwargs.get('out_func_called')
    assert len(results) == 2

    captured = readouterr_and_dump(capsys)
    assert len(captured.err.splitlines()) >= len(psets)

    # because of verbose=3, we should have at least len(psets) progress lines
    has_name = [line for line in captured.err.splitlines() if 'progress' in line and name in line]
    assert len(has_name) >= len(psets)

    results = paramsurvey.map(do_test_args, [], name='no psets')
    assert results is None


def do_raise(pset, system_kwargs, user_kwargs):
    if 'raises' in pset and pset['raises']:
        raise ValueError('foo')
    return {'foo': 'bar'}


def test_worker_exception(capsys, paramsurvey_init):
    psets = [{'raises': False}, {'raises': False}, {'raises': False}, {'raises': True}, {'raises': False}, {'raises': False}, {'raises': False}]

    pslogger_fd.seek(0)
    results = paramsurvey.map(do_raise, psets, name='test_worker_exception')
    assert len(results) == 6
    assert len(results.missing) == 1
    m1 = next(results.missing.iterdicts())
    assert '_exception' in m1
    assert '_traceback' in m1
    assert results.progress.total == 7
    assert results.progress.finished == 6
    assert results.progress.failures == 1
    assert results.progress.exceptions == 1

    captured = readouterr_and_dump(capsys)

    # ray redirects stderr to stdout, while multiprocessing prints it in the worker
    # TODO: add stderr/out capture everywhere and use it here
    #assert 'Traceback ' in captured.out or 'Traceback ' in captured.err

    # the standard progress function prints this
    assert 'failures: 1' in captured.out or 'failures: 1' in captured.err

    log = pslogger_fd.getvalue()
    assert 'Traceback' in log, 'python traceback seen'
    assert 'ValueError' in log, 'python exception seen'
    assert 'pset:' in log, 'our pset logline seen'


def test_out_func_raise():
    def out_func(user_ret, system_kwargs, user_kwargs):
        raise ValueError()

    psets = [{'duration': 0.1}] * 2

    name = 'out func raises'

    results = paramsurvey.map(sleep_worker, psets, out_func=out_func, name=name, verbose=2)

    assert results.progress.exceptions == 2


def do_nothing(pset, system_kwargs, user_kwargs):
    return {'foo': True}


def test_wrapper_exception(capsys, paramsurvey_init):
    psets = [{'actually_raise': False}, {'actually_raise': True}, {'actually_raise': False}, {'actually_raise': True}, {'actually_raise': False}]

    results = paramsurvey.map(do_nothing, psets, raise_in_wrapper=ValueError('test_wrapper_exception'), verbose=0)

    assert len(results) == 3
    assert len(results.missing) == 2
    assert results.progress.total == 5
    assert results.progress.finished == 3
    assert results.progress.failures == 2

    # ray and multiprocessing behave differently for wrapper 'exception'
    # multiprocessing has exception and traceback, both in results.missing and pslogger
    # our ray wrapper doesn't even try to catch it? does it print 'Surprised'?
    #assert '_exception' in results.missing[0]
    #assert results.progress.exceptions == 1

    assert len(results) == 3

    # XXX ray prints traceback in the worker, multiprocessing and ray local_mode prints in the parent

    captured = readouterr_and_dump(capsys)
    assert 'failures: 2' in captured.out or 'failures: 2' in captured.err


def bad_user_function(pset, system_kwargs, user_kwargs):
    return 3


def test_bad_user_function(paramsurvey_init):
    psets = [{'a': 1}, {'a': 2}]

    results = paramsurvey.map(bad_user_function, psets, name='bad_user_function')

    print(results.to_df())
    print(results.missing)
    print(results.stats)

    assert len(results.missing) == 2
    assert results.progress.total == 2
    assert results.progress.finished == 0
    assert results.progress.failures == 2
    assert results.progress.exceptions == 2


def test_toplevel(paramsurvey_init):
    assert paramsurvey.backend() == os.environ.get('PARAMSURVEY_BACKEND', paramsurvey.default_backend)


def test_kwargs(paramsurvey_init):
    duration = 0.1
    psets = [{'duration': duration}] * 10
    results = paramsurvey.map(sleep_worker, psets, limit=3, name='sleep with limit')
    assert len(results) == 3


def test_overlarge_pset():
    pass
