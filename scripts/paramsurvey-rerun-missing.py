#!/usr/bin/env python

import random
import paramsurvey
import paramsurvey.utils


def crash_worker(pset, system_kwargs, user_kwargs):
    print('start of a worker, vmem is', paramsurvey.utils.vmem(), 'GB')
    if random.randint(1, 100) <= pset['pct_chance_crash']:
        raise NotImplementedError


def main():
    print('initial driver vmem', paramsurvey.utils.vmem(), 'GB')
    paramsurvey.init(backend='multiprocessing', max_tasks_per_child=10)  # max_tasks_per_child is useful for slow memory leaks

    psets = [{'pct_chance_crash': 10}] * 100
    count = 0

    while True:
        print('driver about call map', paramsurvey.utils.vmem(), 'GB')
        results = paramsurvey.map(crash_worker, psets)

        print('missing count', len(results.missing))

        count += 1
        if len(results.missing) == 0 or count > 10:
            break

        # before I can use results.missing as psets, I have to remove the added columns
        psets = results.missing.to_psets()

    print('final missing count', len(results.missing))
    print('final driver vmem', paramsurvey.utils.vmem(), 'GB')


if __name__ == '__main__':
    main()
