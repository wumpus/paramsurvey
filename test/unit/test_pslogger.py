import os.path
import pytest

import paramsurvey


def test_atomic_create_ish(fs):
    filenames = ['a', 'b', 'c']
    for i, f in enumerate(filenames):
        paramsurvey.pslogger.atomic_create_ish(filenames)
        i += 1
        for ff in filenames[:i]:
            assert os.path.exists(ff)
        for ff in filenames[i:]:
            assert not os.path.exists(ff)

    with pytest.raises(ValueError):
        paramsurvey.pslogger.atomic_create_ish(filenames)
