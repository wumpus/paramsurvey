import time
import os
import tempfile
import pytest

import paramsurvey
import paramsurvey.stats


def do_sleep(pset, system_kwargs, user_kwargs, raw_stats):
    time.sleep(pset['duration'])
    return {'slept': pset['duration']}


def do_burn(pset, system_kwargs, user_kwargs, raw_stats):
    start = time.time()
    with paramsurvey.stats.record_wallclock('foo', raw_stats):
        while time.time() < start + pset['duration']:
            pass
    return {'burned': pset['duration']}


@pytest.fixture(scope="module")
def paramsurvey_init(request):
    paramsurvey.init()

    def finalize():
        # needed to get pytest multiprocessing coverage
        paramsurvey.finalize()

    request.addfinalizer(finalize)


def test_basics(paramsurvey_init):
    ncores = max(4, paramsurvey.current_core_count())

    duration = 0.1
    psets = [{'duration': duration}] * ncores * 4

    start = time.time()
    ret = paramsurvey.map(do_sleep, psets, name='simple')
    assert [r['result']['slept'] == duration for r in ret], 'everyone slept '+str(duration)
    assert len(ret) == len(psets), 'one return for each pset'
    elapsed = time.time() - start
    assert elapsed > duration, 'must take at least {} time'.format(duration)

    psets = psets[:ncores]
    start = time.time()
    ret = paramsurvey.map(do_sleep, psets, name='group_size 5', group_size=5)
    assert [r['result']['slept'] == duration for r in ret], 'everyone slept '+str(duration)
    assert len(ret) == len(psets), 'one return for each pset'
    elapsed = time.time() - start
    assert elapsed > duration*3, 'must take at least {} time'.format(duration)

    ret = paramsurvey.map(do_burn, psets, name='burn group_size 5', group_size=4)
    assert [r['result']['burned'] == duration for r in ret], 'everyone burned '+str(duration)
    assert len(ret) == len(psets), 'one return for each pset'
    elapsed = time.time() - start
    assert elapsed > duration*3, 'must take at least {} time'.format(duration)


def do_test_args(pset, system_kwargs, user_kwargs, raw_stats):
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
    outfile = 'foo'
    name = 'foobiebletch'

    def out_func(user_ret, system_kwargs, user_kwargs):
        nonlocal out_func_called
        out_func_called = True
        user_kwargs['out_func_called'] = True
        assert user_kwargs == test_user_kwargs
        assert 'outfile' in system_kwargs
        assert system_kwargs['outfile'] == outfile

    psets = [{'duration': 0.1}] * 2

    ret = paramsurvey.map(do_test_args, psets,
                          out_func=out_func, user_kwargs=test_user_kwargs,
                          chdir=chdir, outfile=outfile, out_subdirs=10,
                          progress_dt=0., name=name)

    assert out_func_called
    assert test_user_kwargs.get('out_func_called')
    assert ret is None  # because of out_func

    captured = capsys.readouterr()
    assert len(captured.err.splitlines()) >= len(psets)

    # because of progress_dt being 0., we should have at least len(psets) progress lines
    has_name = [line for line in captured.err.splitlines() if 'progress' in line and name in line]
    assert len(has_name) >= len(psets)

    # same as previous but verbose=2 instead of progress_dt=0
    ret = paramsurvey.map(do_test_args, psets,
                          out_func=out_func, user_kwargs=test_user_kwargs,
                          chdir=chdir, outfile=outfile, out_subdirs=10,
                          verbose=2, name=name)

    assert out_func_called
    assert test_user_kwargs.get('out_func_called')
    assert ret is None  # because of out_func

    captured = capsys.readouterr()
    assert len(captured.err.splitlines()) >= len(psets)

    # because of progress_dt being 0., we should have at least len(psets) progress lines
    has_name = [line for line in captured.err.splitlines() if 'progress' in line and name in line]
    assert len(has_name) >= len(psets)

    ret = paramsurvey.map(do_test_args, [])
    assert ret is None


def do_raise(pset, system_kwargs, user_kwargs, raw_stats):
    if 'raise' in pset and pset['raise']:
        raise ValueError('foo')
    return {'foo': 'bar'}


def test_worker_exception(capsys, paramsurvey_init):
    psets = [{}, {}, {}, {'raise': True}, {}, {}, {}]

    ret = paramsurvey.map(do_raise, psets)
    assert len(ret) == 7
    assert sum('exception' in r for r in ret if r is not None) == 1
    assert sum('pset' in r for r in ret) == 7
    assert sum('result' in r for r in ret) == 6

    out, err = capsys.readouterr()

    # ray redirects stderr to stdout, while multiprocessing prints it in the worker
    # TODO: add stderr/out capture everywhere and use it here
    #assert 'Traceback ' in out or 'Traceback ' in err

    # the standard progress function prints this
    assert 'failures: 1' in out or 'failures: 1' in err


def do_nothing(pset, system_kwargs, user_kwargs, raw_stats):
    return {'foo': True}


def test_wrapper_exception(capsys, paramsurvey_init):
    psets = [{}, {'actually_raise': True}, {}, {}, {}]

    ret = paramsurvey.map(do_nothing, psets, raise_in_wrapper=ValueError('test_wrapper_exception'))

    # ray eats a return when it raises
    assert len(ret) == 5 or len(ret) == 4
    assert sum('result' in r for r in ret) == 4  # same for ray and multi
    if len(ret) == 5:
        assert sum('exception' in r for r in ret) == 1
    else:
        assert sum('exception' in r for r in ret) == 0

    # XXX ray prints traceback in the worker, multiprocessing and ray local_mode prints in the parent

    out, err = capsys.readouterr()
    assert 'failures: 1' in out or 'failures: 1' in err


def test_overlarge_pset():
    pass
