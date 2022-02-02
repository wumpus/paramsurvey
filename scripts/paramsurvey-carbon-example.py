#!/usr/bin/env python

import time
import paramsurvey


def sleep_worker(pset, system_kwargs, user_kwargs):
    time.sleep(pset['duration'])
    return {'slept': pset['duration']}


def main():
    paramsurvey.init(backend='multiprocessing',
                     carbon_server='127.0.0.1',
                     carbon_port=2005,
                     carbon_prefix='paramsurvey.test')

    psets = [{'duration': 0.3}] * 500

    results = paramsurvey.map(sleep_worker, psets, verbose=2)

    assert results.progress.failures == 0


if __name__ == '__main__':
    main()
