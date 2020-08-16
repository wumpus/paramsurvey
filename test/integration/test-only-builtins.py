import pytest

import paramsurvey


with pytest.raises(ValueError):
    paramsurvey.init()
