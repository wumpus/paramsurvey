#!/usr/bin/env python

import time
import paramsurvey
import paramsurvey.stats


def stats_worker(pset, system_kwargs, user_kwargs):
    raw_stats = system_kwargs['raw_stats']

    with paramsurvey.stats.record_wallclock('cpu-loop wallclock', raw_stats):
        with paramsurvey.stats.record_iowait('cpu-loop iowait', raw_stats):
            t = time.time()
            while t + 1.0 > time.time():  # spins eating cpu for 1 second
                pass

    with paramsurvey.stats.record_wallclock('sleep1-loop wallclock', raw_stats):
        with paramsurvey.stats.record_iowait('sleep1-loop iowait', raw_stats):
            time.sleep(1.0)  # sleeps for 1 second


def main():
    paramsurvey.init(backend='multiprocessing')  # or 'ray', if you installed it

    psets = [{'foo': True}] * 5

    results = paramsurvey.map(stats_worker, psets, name='stats-example', verbose=0)

    assert results.progress.failures == 0

    results.stats.print_histograms()


if __name__ == '__main__':
    main()

'''
The cpu loop doesn't have any idle time, so iowait = 0

counter cpu-loop iowait, total 0s, mean 0.00s, counts 5
counter cpu-loop iowait, 50%tile: 0.01s
counter cpu-loop iowait, 90%tile: 0.01s
counter cpu-loop iowait, 95%tile: 0.01s
counter cpu-loop iowait, 99%tile: 0.01s
counter cpu-loop wallclock, total 10s, mean 2.00s, counts 5
counter cpu-loop wallclock, 50%tile: 2.01s
counter cpu-loop wallclock, 90%tile: 2.01s
counter cpu-loop wallclock, 95%tile: 2.01s
counter cpu-loop wallclock, 99%tile: 2.01s

The sleep loop isn't using any cpu, so it shows iowait == wallclock

counter sleep1-loop iowait, total 5s, mean 1.00s, counts 5
counter sleep1-loop iowait, 50%tile: 1.01s
counter sleep1-loop iowait, 90%tile: 1.01s
counter sleep1-loop iowait, 95%tile: 1.01s
counter sleep1-loop iowait, 99%tile: 1.01s
counter sleep1-loop wallclock, total 5s, mean 1.00s, counts 5
counter sleep1-loop wallclock, 50%tile: 1.01s
counter sleep1-loop wallclock, 90%tile: 1.01s
counter sleep1-loop wallclock, 95%tile: 1.01s
counter sleep1-loop wallclock, 99%tile: 1.01s

This is both loops together:

counter stats-example_iowait, total 5s, mean 1.00s, counts 5
counter stats-example_iowait, 50%tile: 1.01s
counter stats-example_iowait, 90%tile: 1.01s
counter stats-example_iowait, 95%tile: 1.01s
counter stats-example_iowait, 99%tile: 1.01s
counter stats-example_wallclock, total 15s, mean 3.00s, counts 5
counter stats-example_wallclock, 50%tile: 3.01s
counter stats-example_wallclock, 90%tile: 3.01s
counter stats-example_wallclock, 95%tile: 3.01s
counter stats-example_wallclock, 99%tile: 3.01s
'''
