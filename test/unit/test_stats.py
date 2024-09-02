import math
import time
import pytest

from paramsurvey import stats


def test_stats():
    s = stats.PerfStats()
    raw_stats = {
        'foo': [0.1, 0.3],
        'bar': [3.0, 4.0, 5.0],
        'baz': [3.0, -4.0, 5.0],
    }
    s.combine_stats(raw_stats)

    assert s.read_stats('barf') is None

    assert len(s.all_stat_names()) == len(raw_stats.keys())

    for name in s.all_stat_names():
        count, avg, hist = s.read_stats(name)

        assert count == len(raw_stats[name])
        assert math.isclose(avg, sum(raw_stats[name])/count)

        assert count == hist.get_total_count(), 'histogram did count the negative value'
        hist99 = hist.get_value_at_percentile(99)/1000.
        biggest = max(raw_stats[name])
        assert math.isclose(hist99, biggest, rel_tol=.01)

        hist0 = hist.get_value_at_percentile(0)/1000.
        smallest = min(raw_stats[name])
        if smallest <= 0:
            with pytest.raises(AssertionError):
                assert math.isclose(hist0, smallest, rel_tol=.01)
            smallest = 1 / 1000.  # this is what we send to HdrHist for <=0. values
        assert math.isclose(hist0, smallest, rel_tol=.01)

        # TODO: make sure that value too big causes a dropped data point warning in s.print_percentile()


def test_record_wallclock():
    raw_stats = {}
    duration = 0.3
    start = time.time()
    with stats.record_wallclock('foo', raw_stats):
        while time.time() < start + duration:
            pass
    assert 'foo' in raw_stats
    assert len(raw_stats['foo']) == 1
    assert raw_stats['foo'][0] > duration * 0.9

    s = stats.PerfStats()
    with stats.record_wallclock('foo', obj=s):
        while time.time() < start + duration:
            pass
    assert len(s.all_stat_names()) == 1
    for name in s.all_stat_names():
        count, avg, hist = s.read_stats(name)
        assert count == 1.0
        # no good way to test the value


def test_record_iowait():
    raw_stats = {}
    duration = 0.1
    start = time.time()
    with stats.record_wallclock('wall', raw_stats):
        with stats.record_iowait('io', raw_stats):
            while time.time() < start + duration:
                pass
            time.sleep(duration*2)
    assert 'io' in raw_stats
    assert len(raw_stats['io']) == 1
    assert raw_stats['io'][0] >= duration*2

    duration = 0.1
    s = stats.PerfStats()
    with stats.record_wallclock('wall', obj=s):
        with stats.record_iowait('io', obj=s):
            while time.time() < start + duration:
                pass
            time.sleep(duration*2)
    assert len(s.all_stat_names()) == 2
    for name in s.all_stat_names():
        count, avg, hist = s.read_stats(name)
        assert count == 1.0, name
        # no good way to test the value


def test_percentiles():
    # just a crash test
    raw_stats = {'default': [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]}
    s = stats.PerfStats(raw_stats)
    s.print_percentile('default')
    s.print_percentiles('default')  # empty
    s.print_percentiles(None)  # same as first


def test_histograms():
    # just a crash test
    raw_stats = {'default': [0.1, 0.2, 0.2, 0.2, 0.2, 0.6, 0.7, 0.8, 0.9, 1.0]}

    s = stats.PerfStats(raw_stats)
    s.print_histogram('default')
    s.print_histograms('default')  # empty
    s.print_histograms(None)  # same as first
