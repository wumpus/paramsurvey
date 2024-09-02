import platform
import os
import pytest

import paramsurvey.psmultiprocessing


def test__core_count():
    psys = platform.system()
    if psys == 'Darwin':
        assert os.cpu_count() == paramsurvey.psmultiprocessing._core_count()
    elif psys == 'Windows':
        # len(psutil.Process().cpu_affinity()) -- not yet implemented by us
        assert os.cpu_count() == paramsurvey.psmultiprocessing._core_count()
    elif psys == 'Linux':
        if 'sched_getaffinity' not in dir(os):
            assert os.cpu_count() == paramsurvey.psmultiprocessing._core_count()
            return

        start = os.sched_getaffinity(0)
        assert paramsurvey.psmultiprocessing._core_count() == len(start)
        if len(start) == 1:
            pytest.skip('cannot test Linux setaffinity core_count because we only have 1 core')
            return

        first = start.copy().pop()
        os.sched_setaffinity(0, [first])
        assert paramsurvey.psmultiprocessing._core_count() == 1, 'set to just one core'
        os.sched_setaffinity(0, start)
        assert paramsurvey.psmultiprocessing._core_count() == len(start), 'reset to original list'
