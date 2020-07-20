import math

from paramsurvey import stats


def test_stats():
    s = stats.StatsObject()
    raw_stats = {
        'foo': [0.1, 0.3],
        'bar': [3.0, 4.0, 5.0],
    }
    s.combine_stats(raw_stats)

    assert s.read_stats('barf') is None

    assert len(s.all_stat_names()) == 2

    for name in s.all_stat_names():
        count, avg, hist = s.read_stats(name)
        assert count == len(raw_stats[name])
        assert math.isclose(avg, sum(raw_stats[name])/count)
