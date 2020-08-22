import os.path
import pytest

from paramsurvey import pslogger


def test_atomic_create_ish(fs):
    filenames = ['a', 'b', 'c']
    for i, f in enumerate(filenames):
        pslogger.atomic_create_ish(filenames)
        i += 1
        for ff in filenames[:i]:
            assert os.path.exists(ff)
        for ff in filenames[i:]:
            assert not os.path.exists(ff)

    with pytest.raises(ValueError):
        pslogger.atomic_create_ish(filenames)
