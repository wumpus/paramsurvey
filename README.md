# paramsurvey

[![Build Status](https://dev.azure.com/lindahl0577/paramsurvey/_apis/build/status/wumpus.paramsurvey?branchName=main)](https://dev.azure.com/lindahl0577/paramsurvey/_build/latest?definitionId=1&branchName=main) [![Coverage](https://coveralls.io/repos/github/wumpus/paramsurvey/badge.svg?branch=main)](https://coveralls.io/github/wumpus/paramsurvey?branch=main) [![Apache License 2.0](https://img.shields.io/github/license/wumpus/paramsurvey.svg)](LICENSE)

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

## Keyword arguments to init() and map()

The `paramsurvey` code has a set of keyword arguments (and corresponding environment
variables) to aid debugging and testing. They are:

* `backend="multiprocessing"` -- which backend to use, currently "multiprocessing" (default) or "ray"
* `verbose=1` -- print information about the progress of the computation:
  * 0 = print nothing
  * 1 = print something every 30 seconds (default)
  * 2 = print something every second
  * 3 = print something for every action
* `vstats=1` -- controls the verbosity of the performance statistics system, with similar values as `verbose`
* `limit=0` -- limits the number of psets actually computed to this number (0 meaning "all")
* `ncores=-1` -- limits the number of cores used, in this case 1 less than the number available (multiprocessing only)
* `max_tasks_per_child=3` -- the number of tasks a child will do before restarting. Useful to limit memory leaks. Default: infinite

Each of these has a corresponding environment variable,
e.g. `PARAMSURVEY_BACKEND`, `PARAMSURVEY_VERBOSE`.  If the environment
variable is set, it overrides the values set in the source code. If a
kwarg is set for a `map()` call, that value overrides any value
specified for the `init()` call.

For example, if you wish to debug a large computation by running a small
subset of it on a single node, the environment variables allow you to do
this without editing your source code:

```
$ PARAMSURVEY_BACKEND=multiprocessing PARAMSURVEY_VERBOSE=3 PARAMSURVEY_LIMIT=10 ./myprogram.py
```

For retrospective debugging, i.e. your run crashes and you are sad
that you specified a lower verbosity than you desire post-crash,
`paramsurvey` creates a hidden logfile in the current directory for
every run, named `.paramusurvey-DATE-TIME.log`.

### Backend-specific arguments

Both `init()` and `map()` take a backend-specific keyword argument named for the backend, and
ignored by other backends. For example, to pass an argument only used by the `ray` backend,

```
paramsurvey.map(..., ray={'num_gpus': 1})
```

## The MapResults object

The MapResults object has several properties:

* `results` is a Pandas DataFrame containing the values of the pset
and the keys returned by the worker function. Iterating over these
results is documented above, as either dicts or tuples.
* `missing` is a DataFrame of psets that did not generate results,
plus extra '_exception' and '_traceback' columns if an exception was
raised in the worker function.
* `progress` is a MapProgress object with properties containing the
details of pset execution: total, active, finished, failures,
exceptions.
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

