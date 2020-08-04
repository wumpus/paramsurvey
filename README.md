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

for r in results:
    print(r.duration, r.slept)
```

prints, in addition to some debugging output, a result from each of the 5 sleep_worker calls.

Here are a few more examples:

* [The above example, with a few notes](scripts/paramsurvey-readme-example.py)
* [An example of a multi-stage computation](scripts/paramsurvey-multistage-example.py), running several `map()` functions in a row
* [An example of greedy optimization](scripts/paramsurvey-greedy-example.py), selecting the best alternative from each `map()` result

## Philosophy

A parameter survey runs begins by initializing the software,
specifying which backend ('multiprocessing' or 'ray').

A list of parameter sets (psets) is constructed.

The `pararamsurvey.map()` function executes the worker function once for each pset.

It returns a `MapResults` object, containing the results, performance
statistics, and information about any failures.

Multiple calls to `paramsurvey.map()` can be made, with different lists of psets.

## Worker function limitations

The worker function runs in a different address space and possibly on a different server.
It shouldn't access any global variables.

For hard-to-explain Python issues, be sure to define the worker
function before calling `paramsurvey.init()`. The worker function should
not be nested inside another function. On Windows, the main program file
should have a [`if __name == '__main__'` guard similar to the examples
at the top of the Python multprocessing documentation.](https://docs.python.org/3/library/multiprocessing.html)

## The MapResults object

The MapResults object has several properties:

* results is a list of dictionaries; 'return' is the return value of the worker function, and 'pset' is the pset.
* failed is a list of failed psets, plus an extra '_exception' key if an exception was raised in the worker
* progress is a MapProgress object with properties containing the details of pset execution: total, started, finished, failures, exceptions
* stats is a PerfStats object containing performance statistics

## Installing

```
$ pip install paramsurvey
$ pip install paramsurvey[ray]
```

