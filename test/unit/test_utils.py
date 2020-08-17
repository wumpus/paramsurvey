import pytest
from unittest.mock import patch
import os

import pandas as pd

import paramsurvey
from paramsurvey import utils


def test_psets_prep():
    with pytest.raises(ValueError):
        utils.psets_prep([{'_pset_id': None}])
    with pytest.raises(ValueError):
        utils.psets_prep([{'a b': None}])  # invalid identifier
    with pytest.raises(ValueError):
        utils.psets_prep([{'raise': None}])  # keyword
    with pytest.raises(ValueError):
        utils.psets_prep([{'_asdf': None}])


def test_flatten_results():
    normal = [
        {'pset': {'a': 1}, 'result': {'b': 1}},
        {'pset': {'a': 2}, 'result': {'b': 2}},
    ]
    flat = paramsurvey.flatten_results(normal)
    assert len(flat) == 2
    assert flat == [
        {'a': 1, 'b': 1},
        {'a': 2, 'b': 2},
    ]

    normal[1]['result']['a'] = 1
    with pytest.raises(ValueError):
        flat = paramsurvey.flatten_results(normal)


def test_make_subdir_name():
    ret = utils.make_subdir_name(100)
    assert len(ret) == 4

    ret = utils.make_subdir_name(100, prefix='prefix')
    assert len(ret) == 8

    with pytest.raises(Exception):
        ret = utils.make_subdir_name(0)


def test_get_pset_group():
    psets = [{'a': 1}, {'a': 2}, {'a': 3}, {'a': 4}, {'a': 5}]
    psets = pd.DataFrame(psets)
    pset_index = 0

    pset_group, pset_index = utils.get_pset_group(psets, pset_index, 3)
    assert len(pset_group) == 3
    assert pset_group.iloc[0].a == 1
    assert pset_group.iloc[-1].a == 3

    pset_group, pset_index = utils.get_pset_group(psets, pset_index, 3)
    assert len(pset_group) == 2
    assert pset_group.iloc[0].a == 4
    assert pset_group.iloc[-1].a == 5

    pset_group, pset_index = utils.get_pset_group(psets, pset_index, 3)
    assert len(pset_group) == 0


def copy2(orig):
    ret = orig.copy()  # shallow copy
    for k in ret:
        ret[k] = ret[k].copy()  # now deep copy
    return ret


def test_init_resolve_kwargs():
    gkwargs = copy2(paramsurvey.global_kwargs)
    with patch.dict(os.environ, {'PARAMSURVEY_VERBOSE': '3'}, clear=True):
        utils.initialize_kwargs(gkwargs, {'verbose': 1})
        system_kwargs, other_kwargs = utils.resolve_kwargs(gkwargs, {'verbose': 2, 'unrecognized': 'asdf'})
        assert system_kwargs['verbose'] == 3, 'env variables trump all'
        assert other_kwargs['unrecognized'] == 'asdf', 'unrecognized args pass through'

    gkwargs = copy2(paramsurvey.global_kwargs)
    with patch.dict(os.environ, {}, clear=True):
        utils.initialize_kwargs(gkwargs, {'verbose': 1})
        system_kwargs, other_kwargs = utils.resolve_kwargs(gkwargs, {'verbose': 2})
        assert system_kwargs['verbose'] == 2, 'closest wins'
        assert other_kwargs == {}

    gkwargs = copy2(paramsurvey.global_kwargs)
    with patch.dict(os.environ, {}, clear=True):
        utils.initialize_kwargs(gkwargs, {'verbose': 1})
        system_kwargs, other_kwargs = utils.resolve_kwargs(gkwargs, {})
        assert system_kwargs['verbose'] == 1, 'init kwarg comes last'
        assert other_kwargs == {}

    gkwargs = copy2(paramsurvey.global_kwargs)
    with patch.dict(os.environ, {}, clear=True):
        utils.initialize_kwargs(gkwargs, {})
        system_kwargs, other_kwargs = utils.resolve_kwargs(gkwargs, {})
        assert system_kwargs['verbose'] == 1, 'default'
        assert other_kwargs == {}

    gkwargs = copy2(paramsurvey.global_kwargs)
    with patch.dict(os.environ, {'PARAMSURVEY_BACKEND': 'FOO'}, clear=True):
        utils.initialize_kwargs(gkwargs, {})
        system_kwargs, other_kwargs = utils.resolve_kwargs(gkwargs, {})
        assert system_kwargs['backend'] == 'FOO', 'type works'
        assert other_kwargs == {}


def test_psets_empty():
    psets = []
    assert utils.psets_empty(psets)
    psets = [1]
    assert not utils.psets_empty(psets)

    df = pd.DataFrame()
    assert utils.psets_empty(df)
    df = pd.DataFrame({'a': [1, 2]})
    assert not utils.psets_empty(df)


def test_vmem():
    vmem0 = utils.vmem()
    big = bytearray(10000000)  # 10 megs
    vmem1 = utils.vmem()
    # vmem might not go up at all, if there is free memory
    assert vmem1 <= vmem0 + 0.011, 'vmem does not go up more than expected'
