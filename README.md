# paramsurvey

[![Build Status](https://travis-ci.org/wumpus/paramsurvey.svg?branch=master)](https://travis-ci.org/wumpus/paramsurvey) [![Coverage Status](https://coveralls.io/repos/github/wumpus/paramsurvey/badge.svg?branch=master)](https://coveralls.io/github/wumpus/paramsurvey?branch=master) [![Apache License 2.0](https://img.shields.io/github/license/wumpus/paramsurvey.svg)](LICENSE)

paramsurvey is a set of tools for creating and executing parameter surveys.

paramsurvey has a pluggable parallel backend. The supported backends at present
are python's multiprocessing module, and computing cluster software `ray`. An `mpi` backend is planned.

## Example

```
import time
import paramsurvey


def sleep_worker(pset, system_kwargs, user_kwargs, raw_stats):
    time.sleep(pset['duration'])
    return {'slept': pset['duration']}


paramsurvey.init(backend='multiprocessing')  # or 'ray', if you installed it

psets = [{'duration': 0.3}] * 5

results = paramsurvey.map(sleep_worker, psets, verbose=2)

for r in results.results:
    print(r['pset'], r['result'])
```

prints, in addition to some debugging output, a result from each of the 5 sleep_worker calls.

## Philosophy




## Installing

```
$ pip install paramsurvey
$ pip install paramsurvey[ray]
```

