import pytest
import paramsurvey
from paramsurvey import utils


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
