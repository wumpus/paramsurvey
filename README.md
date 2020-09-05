# paramsurvey

[![Build Status](https://travis-ci.com/wumpus/paramsurvey.svg?branch=master)](https://travis-ci.com/wumpus/paramsurvey) [![Coverage Status](https://coveralls.io/repos/github/wumpus/paramsurvey/badge.svg?branch=master)](https://coveralls.io/github/wumpus/paramsurvey?branch=master) [![Apache License 2.0](https://img.shields.io/github/license/wumpus/paramsurvey.svg)](LICENSE)

paramsurvey is a set of tools for creating and executing parameter surveys.

paramsurvey has a pluggable parallel backend. The supported backends at present
are python's multiprocessing module, and computing cluster software `ray`. An `mpi` backend is planned.

## Example

```
import time
import paramsurvey


def sleep_worker(pset, system_kwargs, user_kwargs):
    time.sleep(pset['duration'])
    return {'slept': pset['duration']}


paramsurvey.init(backend='multiprocessing')  # or 'ray', if you installed it

psets = [{'duration': 0.3}] * 5

results = paramsurvey.map(sleep_worker, psets, verbose=2)

for r in results.itertuples():
    print(r.duration, r.slept)
for r in results.iterdicts():
    print(r['duration'], r['slept'])
```

prints, in addition to some debugging output, a result from each of the 5 sleep_worker calls.

Here are a few more examples:

* [The above example, with a few notes](scripts/paramsurvey-readme-example.py)
* [An example of a multi-stage computation](scripts/paramsurvey-multistage-example.py), running several `map()` functions in a row
* [An example of greedy optimization](scripts/paramsurvey-greedy-example.py), selecting the best alternative from each `map()` result

These examples are installed with the package, so you can run them like this:

```
$ paramsurvey-readme-example.py
$ paramsurvey-multistage-example.py
$ paramsurvey-greedy-example.py
```

## Philosophy

A parameter survey runs begins by initializing the software,
specifying a backend ('multiprocessing' or 'ray').

The user supplies a worker function, which takes a dict of parameters
(pset) and returns a dict of results.

The user also supplies a list of parameter sets (psets), perhaps
constructed using the helper function `paramsurvey.params.product()`.

Calling `pararamsurvey.map()` executes the worker function once for
each pset. It returns a `MapResults` object, containing the results,
performance statistics, and information about any failures.

You can call `paramsurvey.map()` more than once.

## Controlling verbosity

The `paramsurvey` code has a set of keyword arguments (and corresponding environment
variables) to aid debugging and testing. They are:

* `backend="multiprocessing"` -- which backend to use, currently "multiprocessing" (default) or "ray"
* `verbose=1` -- print information about the progress of the computation:
  * 0 = print nothing
  * 1 = print something every 30 seconds (default)
  * 2 = print something every second
  * 3 = print something for every action
* `vstats=1` -- controls the verbosity of the performance statistics system, with similar values as `verbose`
* `limit=-1` -- limits the number of psets actually computed to this number (-1 meaning "all")

Each of these has a corresponding environment variable,
e.g. `PARAMSURVEY_BACKEND`, `PARAMSURVEY_VERBOSE`.  If the environment
variable is set, it overrides the values set in the source code. If a
kwarg is set for a `map()` call, that value overrides any value
specified for the `init()` call.

For retrospective debugging, i.e. your run crashes and you are sad
that you specified a lower verbosity than you desire post-crash,
`paramsurvey` creates a hidden logfile in the current directory for
every run, named `.paramusurvey-DATE-TIME.log`.

## The MapResults object

The MapResults object has several properties:

* `results` is a Pandas DataFrame containing the values of the pset and the keys returned by the worker function. If you prefer to deal with dictionaries
and are not worried about memory usage, `results.to_listdict` returns a list of dictionaries.
* `failed` is a list of failed psets dictionaries, plus an extra '_exception' key if an exception was raised in the worker function.
* `progress` is a MapProgress object with properties containing the details of pset execution: total, active, finished, failures, exceptions.
* `stats` is a PerfStats object containing performance statistics.

## Worker function limitations

The worker function runs in a different address space and possibly on a different server.
It shouldn't access any global variables.

For hard-to-explain Python reasons, define the worker function before
calling `paramsurvey.init()`. The worker function should not be nested
inside another function. On Windows, the main program file should have
a [`if __name == '__main__'` guard similar to the examples at the top
of the Python multprocessing
documentation.](https://docs.python.org/3/library/multiprocessing.html)

## Installing

```
$ pip install paramsurvey
$ pip install paramsurvey[ray]
```

