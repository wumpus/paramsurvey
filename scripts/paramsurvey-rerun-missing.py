#!/usr/bin/env python

import random
import paramsurvey


def crash_worker(pset, system_kwargs, user_kwargs):
    if random.randint(1, 100) <= pset['pct_chance_crash']:
        raise NotImplementedError


def main():
    paramsurvey.init(backend='multiprocessing')  # or 'ray', if you installed it

    psets = [{'pct_chance_crash': 10}] * 100
    count = 0

    while True:
        results = paramsurvey.map(crash_worker, psets)

        print('missing count', len(results.missing))

        count += 1
        if len(results.missing) == 0 or count > 10:
            break

        # before I can use results.missing as psets, I have to remove the added columns
        # XXX make helper function
        psets = results.missing.drop(columns=['_exception', '_traceback'])

    print('final missing count', len(results.missing))


if __name__ == '__main__':
    main()
