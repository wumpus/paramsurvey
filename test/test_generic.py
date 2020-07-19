import time
import os
import tempfile
import pytest

import paramsurvey


def do_sleep(work_unit, system_kwargs, user_kwargs, stats_dict):
    time.sleep(work_unit['duration'])
    return {'slept': work_unit['duration']}


def do_burn(work_unit, system_kwargs, user_kwargs, stats_dict):
    start = time.time()
    while time.time() < start + work_unit['duration']:
        pass
    return {'burned': work_unit['duration']}


@pytest.fixture(scope="module")
def paramsurvey_init(request):
    paramsurvey.init()

    def finalize():
        print('cleanup finalize actually called')
        paramsurvey.finalize()

    request.addfinalizer(finalize)
    print('add finalizer called')


def test_basics(paramsurvey_init):
    ncores = max(4, paramsurvey.current_core_count())

    duration = 0.1
    work = [{'duration': duration}] * ncores * 4

    start = time.time()
    ret = paramsurvey.map(do_sleep, work, name='simple')
    assert [r['slept'] == duration for r in ret], 'everyone slept '+str(duration)
    assert len(ret) == len(work), 'one return for each work unit'
    elapsed = time.time() - start
    assert elapsed > duration, 'must take at least {} time'.format(duration)

    work = work[:ncores]
    start = time.time()
    ret = paramsurvey.map(do_sleep, work, name='group_size 4', group_size=4)
    assert [r['slept'] == duration for r in ret], 'everyone slept '+str(duration)
    assert len(ret) == len(work), 'one return for each work unit'
    elapsed = time.time() - start
    assert elapsed > duration*4, 'must take at least {} time'.format(duration)

    ret = paramsurvey.map(do_burn, work, name='burn group_size 4', group_size=4)
    assert [r['burned'] == duration for r in ret], 'everyone burned '+str(duration)
    assert len(ret) == len(work), 'one return for each work unit'
    elapsed = time.time() - start
    assert elapsed > duration*4, 'must take at least {} time'.format(duration)


def do_test_args(work_unit, system_kwargs, user_kwargs, stats_dict):
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

    work = [{'duration': 0.1}] * 2

    ret = paramsurvey.map(do_test_args, work,
                          out_func=out_func, user_kwargs=test_user_kwargs,
                          chdir=chdir, outfile=outfile, out_subdirs=10,
                          progress_dt=0., name=name)

    assert out_func_called
    assert test_user_kwargs.get('out_func_called')
    assert ret is None  # because of out_func

    captured = capsys.readouterr()
    assert len(captured.err.splitlines()) >= len(work)

    # because of progress_dt being 0., we should have at least len(work) progress lines
    has_name = [line for line in captured.err.splitlines() if 'progress' in line and name in line]
    assert len(has_name) == len(work)


def do_raise(work_unit, system_kwargs, user_kwargs, stats_dict):
    raise ValueError('foo')


def test_worker_exception(capsys, paramsurvey_init):
    work = [{}]

    ret = paramsurvey.map(do_raise, work)
    assert len(ret) == 1
    assert 'work_unit' in ret[0]
    # XXX should the exception be visible here? -- it's in system_ret
    # XXX we only put work_unit in user_ret if there is an exception

    out, err = capsys.readouterr()
    # ray redirects stderr to stdout, while multiprocessing preserves it
    assert 'Traceback:' in out or 'Traceback:' in err

    # XXX progress failure == 1
