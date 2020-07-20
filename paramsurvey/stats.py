import time
from contextlib import contextmanager
from collections import defaultdict
import sys

from hdrh.histogram import HdrHistogram


class StatsObject(object):
    def __init__(self):
        self.d = dict()

    def combine_stats(self, raw_stats):
        for name, elapsed in raw_stats.items():
            g = self.d.get(name, defaultdict(float))
            if 'hist' not in g:
                maxhist = max(elapsed[0] * 2, 30) * 1000  # milliseconds
                g['hist'] = HdrHistogram(10, int(maxhist), 2)  # 10 milliseconds-..., 2 sig figs
            for e in elapsed:
                g['count'] += 1.0
                g['time'] += e
                g['hist'].record_value(int(e * 1000))
            self.d[name] = g

    def read_stats(self, name):
        if name in self.d:
            entry = self.d[name]
            return entry['count'], entry['time']/entry['count'], entry['hist']

    def all_stat_names(self):
        return self.d.keys()

    def print_histograms(self, name='default', file=sys.stdout):
        self.print_histogram(name, file=file)
        for n in sorted(self.all_stat_names()):
            if n != name:
                self.print_histogram(n, file=file)

    def print_histogram(self, name, file=sys.stdout):
        '''
        if name in self.d:
            for item in self.d[name]['hist'].get_recorded_iterator():
                print('value={} count={} percentile={}'.format(
                    item.value_iterated_to,
                    item.count_added_in_this_iter_step,
                    item.percentile
                ), file=file)
        '''
        if name in self.d:
            hist = self.d[name]['hist']
            print('counter {}, counts {}'.format(name, hist.get_total_count()), file=file)
            for pct in (50, 90, 95, 99):
                print('counter {}, {}%tile: {:.1f}s'.format(name, pct, hist.get_value_at_percentile(pct)/1000.), file=file)

@contextmanager
def record_wallclock(name, raw_stats):
    try:
        start = time.time()
        yield
    finally:
        if name not in raw_stats:
            raw_stats[name] = []
        raw_stats[name].append(time.time() - start)
