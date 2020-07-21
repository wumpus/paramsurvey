import pytest
import paramsurvey
import paramsurvey.utils


def test_flatten_results():
    normal = [
        {'pset': {'a': 1}, 'result': {'b': 1}},
        {'pset': {'a': 2}, 'result': {'b': 2}},
    ]
    flat = paramsurvey.flatten_result(normal)
    assert len(flat) == 2
    assert flat == [
        {'a': 1, 'b': 1},
        {'a': 2, 'b': 2},
    ]

    normal[1]['exception'] = 'blah'
    flat = paramsurvey.flatten_result(normal)
    assert flat == [
        {'a': 1, 'b': 1},
    ]

    with pytest.raises(ValueError):
        flat = paramsurvey.flatten_result(normal, raise_if_exceptions=True)
    del normal[1]['exception']

    flat = paramsurvey.flatten_result(normal)  # make sure it's valid again
    normal[1]['result']['a'] = 1
    with pytest.raises(ValueError):
        flat = paramsurvey.flatten_result(normal)
    with pytest.raises(ValueError):
        flat = paramsurvey.flatten_result(normal, raise_if_exceptions=True)
