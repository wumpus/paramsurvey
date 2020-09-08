import time
import sys
from contextlib import contextmanager
from collections import defaultdict

from hdrh.histogram import HdrHistogram
from hdrh.iterators import LinearIterator

from . import pslogger


class PerfStats(object):
    def __init__(self, raw_stats=None, vstats=1):
        self.d = dict()
        self.stats_last = time.time()
        self.stats_dt = self.pick_dt(vstats)
        self.stats_log_last = time.time()
        self.stats_log_dt = 30  # hardwired
        if raw_stats:
            self.combine_stats(raw_stats)

    def pick_dt(self, vstats):
        if vstats > 1:
            return 1
        elif vstats == 1:
            return 30
        else:
            return 1000000

    def combine_stats(self, raw_stats):
        for name, elapsed in raw_stats.items():
            g = self.d.get(name, defaultdict(float))
            if 'hist' not in g:
                maxelapsed = max(elapsed)
                maxhist = max(maxelapsed * 2, 600) * 1000  # milliseconds, minimum 10 minutes
                g['hist'] = HdrHistogram(10, int(maxhist), 2)  # 10 milliseconds-..., 2 sig figs
                g['maxhist'] = int(maxhist)
            for e in elapsed:
                g['count'] += 1.0
                g['time'] += e
                g['hist'].record_value(int(e * 1000))
                if e * 1000 > g['maxhist']:
                    # will be silently not recorded in hist
                    pass
            self.d[name] = g
        #for k in list(raw_stats.keys()):
        #    del raw_stats[k]

    def read_stats(self, name):
        if name in self.d:
            entry = self.d[name]
            return entry['count'], entry['time']/entry['count'], entry['hist']

    def all_stat_names(self):
        return self.d.keys()

    def report(self, final=False):
        t = time.time()
        last = t - self.stats_last > self.stats_dt
        log_last = t - self.stats_log_last > self.stats_log_dt

        if last:
            self.stats_last = t
        if log_last:
            self.stats_log_last = t

        if final or last or log_last:
            self.print_percentiles(stderr=final or last)

    def print_percentiles(self, name='default', stderr=False):
        self.print_percentile(name, stderr=stderr)
        for n in sorted(self.all_stat_names()):
            if n != name:
                self.print_percentile(n, stderr=stderr)

    def print_percentile(self, name, stderr=False):
        if name in self.d:
            hist = self.d[name]['hist']
            total = self.d[name]['time']
            mean = total / self.d[name]['count']
            pslogger.log('counter {}, total {:.0f}s, mean {:.2f}s, counts {}'.format(name, total, mean, hist.get_total_count()), stderr=stderr)
            for pct in (50, 90, 95, 99):
                pslogger.log('counter {}, {}%tile: {:.2f}s'.format(name, pct, hist.get_value_at_percentile(pct)/1000.), stderr=stderr)

    def print_histograms(self, name='default', stderr=False):
        self.print_histogram(name, stderr=stderr)
        for n in sorted(self.all_stat_names()):
            if n != name:
                self.print_histogram(n, stderr=stderr)

    def print_histogram(self, name, value_units_per_bucket=3, stderr=False):
        if name in self.d:
            hist = self.d[name]['hist']
            ivalues = [x for x in LinearIterator(hist, value_units_per_bucket)]
            valuemax = max([x.count_at_value_iterated_to for x in ivalues])
            if not valuemax:
                valuemax = 1.
            for ivalue in ivalues:
                pslogger.log('counter {}, {} {}'.format(name,
                                                        ivalue.value_iterated_to/1000.,
                                                        ivalue.count_at_value_iterated_to),
                             stderr=stderr)


@contextmanager
def record_wallclock(name, raw_stats=None, obj=None):
    try:
        start = time.time()
        yield
    finally:
        value = time.time() - start
        if raw_stats is not None:
            if name not in raw_stats:
                raw_stats[name] = []
            raw_stats[name].append(value)
        if obj:
            obj.combine_stats({name: [value]})


@contextmanager
def record_iowait(name, raw_stats=None, obj=None):
    try:
        start_t = time.time()
        start_c = time.process_time()
        yield
    finally:
        duration = time.time() - start_t
        cpu = time.process_time() - start_c
        if raw_stats is not None:
            if name not in raw_stats:
                raw_stats[name] = []
            raw_stats[name].append(duration - cpu)
        if obj:
            obj.combine_stats({name: [duration - cpu]})
