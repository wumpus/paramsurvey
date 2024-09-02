# paramsurvey

[![build](https://github.com/wumpus/paramsurvey/actions/workflows/test-all.yml/badge.svg)](https://github.com/wumpus/paramsurvey/actions/workflows/test-all.yml) [![coverage](https://codecov.io/gh/wumpus/paramsurvey/graph/badge.svg?token=KYHT7WH9H2)](https://codecov.io/gh/wumpus/paramsurvey) [![Apache License 2.0](https://img.shields.io/github/license/wumpus/paramsurvey.svg)](LICENSE)

paramsurvey is a set of tools for creating and executing parameter
surveys, on systems ranging from laptops to supercomputing clusters.

paramsurvey has a pluggable parallel backend. The supported backends at present
are python's multiprocessing module, and computing cluster software `ray`.

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

* [The above example, with a few notes](https://github.com/wumpus/paramsurvey/blob/main/scripts/paramsurvey-readme-example.py)
* [An example of a multi-stage computation](https://github.com/wumpus/paramsurvey/blob/main/scripts/paramsurvey-multistage-example.py), running several `map()` functions in a row
* [An example of greedy optimization](https://github.com/wumpus/paramsurvey/blob/main/scripts/paramsurvey-greedy-example.py), selecting the best alternative from each `map()` result
* [An example that runs a command-line program for each pset](https://github.com/wumpus/paramsurvey/blob/main/scripts/paramsurvey-cli.py)
* [An example of using helper functions to create the list of psets to compute](https://github.com/wumpus/paramsurvey/blob/main/scripts/pset-creation-example.py)
* [An example of using the stats to time subsections of your worker function](https://github.com/wumpus/paramsurvey/blob/main/scripts/paramsurvey-stats-example.py)
* [An example of using results.missing to re-run missing psets](https://github.com/wumpus/paramsurvey/blob/main/scripts/paramsurvey-rerun-missing.py)

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
* `group_size=N` -- bundle psets into groups, which is useful if a single pset's runtime is too short for `ray` to efficiently run them. One minute is a good runtime.

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
every run, named `.paramusurvey-DATE-TIME.log`. For example, this
hidden logfile will always contain information about any exceptions
raised in your worker code.

### Pandas-related quirks

To preserve memory in the case of large numbers of psets, the psets and the results are stored as
a Pandas DataFrame. This creates a couple of quirks visible to user code:

* A key that exists in any pset or result will exist in every pset or result, with a default value of `nan`
* If you treat `nan` as a boolean in Python, it evaluates to `True`
* Because of a (fixable) quirk in `pandas-appender`, columns are not automatically promoted from integer
to float. So if you have a large number of psets with an integer value, and then throw in a float, it will be rounded to an integer.

### Backend-specific arguments

Both `init()` and `map()` take a backend-specific keyword argument named for the backend, and
ignored by other backends. For example, to pass an argument only used by the `ray` backend,

```
paramsurvey.init(..., ray={'address': 'auto'})
...
paramsurvey.map(..., ray={'num_gpus': 1})
```

Individual psets can also have backend-specific arguments, in the case
where different units of work need different memory limits or
different thread counts. The complete list of ray arguments is:
`name`, `memory`, `num_cpus`, and `num_gpus`. There is also
`num_cores` as an alias for `num_cpus`.

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

## Fault tolerance, and re-running failed computations

If you have bugs in your code that raise exceptions, paramsurvey will
diligently collect the exceptions and tracebacks and will print them
into the output (verbose=2 or more) and also in the hidden logfile
`.paramusurvey-DATE-TIME.log` mentioned above.  Any pset causing an
exception will be in the `results.missing` DataFrame.

In addition to exceptions thrown by user code, the paramsurvey module
and the laptop or cluster its running on can experience two kinds of
errors. The first is failures of some nodes or processes in one of the
distributed backends, like `ray`, `ray` will quietly re-run the pset
as long as the head node and the driver are still alive. So, for example,
it's safe to run most nodes in the computation as a "EC2 spot instance"
or "preemptable node" in a cluster queue system like Slurm.

The other kind of error is one that can't be caught by paramsurvey.
This might include python `multiprocessing` completely crashing
everything because your laptop is out of memory, or `ray` having
indigestion because some nodes are low on memory and are responding
slowly, but aren't totally dead.

## Debugging checklist

* Check the hidden logfile for details of previous runs
* Use environment variables to shrink your run to a single pset, as mentioned above:
```
$ PARAMSURVEY_BACKEND=multiprocessing PARAMSURVEY_VERBOSE=3 PARAMSURVEY_LIMIT=1 ./myprogram.py
```
* Memory debugging: `print(paramsurvey.utils.vmem())` in a few places, values are in GBytes
* Slow memory leaks: restart children after every Nth pset by adding `max_tasks_per_child=10` to the `paramsurvey.init()` call
* Look at the performance statistics
* Look at the example scripts linked above, which demonstrate most features mentioned in this README

## Carbon monitoring

If you'd like to monitor your paramsurvey run in real-time on one of those fancy modern dashboard thingies,
pass details to `paramsurvey.init(..., carbon_server="127.0.0.1", carbon_port=2004, carbon_prefix="paramsurvey")`
The integrated code only knows how to send `pickle` and (so far) only records progress.

![Progress graph example](https://github.com/wumpus/paramsurvey/blob/main/images/paramsurvey-graphite-graph.png)

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

