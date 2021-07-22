import pytest

import paramsurvey


def test_only_builtins():
    with pytest.raises(ValueError):
        paramsurvey.init()
