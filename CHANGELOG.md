- 0.4.21
    + removed numpy<2 pandas<2 now that pandas-appender is updated
	+ test on macos and windows
	+ test pandas<2 just to be sure

- 0.4.20
	+ moved CI to Github Actions, dropped py3.7
	+ moved coverage to CodeCov
	+ python 3.12 has no pandas wheel so it is not tested (too slow)
	+ added temporary (?) numpy<2 and pandas<2 because pandas-appender needs updating

- 0.4.19
	+ update setup.py for new setuptools strictness
	+ python 3.11 now has a ray wheel

- 0.4.18
	+ support python 3.10, thanks to a ray wheel ray>=1.13
	+ drop python 3.6, due to setuptools_scm dropping it
	+ support python 3.11, no ray wheel
	+ bugfix, switch to multiprocessing spawn, seems to fix some mysterious hangs on laptops
	+ bugfix, set OMP_NUM_THREADS=1 if undefined. ray already does this.

- 0.4.17
	+ scripts/paramsurvey-cli.py had a bug and was failing yet passing crash-only-testing
	+ make sure scripts/*.py fails testing if fail
	+ use Github Actions for CI
	+ don't be misled by empty env variables
	+ fixed hard-to-trigger race condition, reliably triggered by Github Actions

- 0.4.16
	+ allow 'address': 'auto' for ray in a cluster. 'password' is now optional.
	+ backend-specific args for non-grouped psets
	+ arg size warning reverted due to ray/pickle5 bug
	+ add current_resources() call
	+ add carbon stats collection for "progress" (likely to be replaced by influx soon)
	+ made "finalize" function run atexit, removed explicit calls from tests

- 0.4.15
	+ warnings on size of args and for missing shell=True in cli
	+ add example script for complicated pset creation (pset-creation-example.py)
	+ add example script for re-running missing psets, plus helper method results.missing.to_psets()
	+ add example script for using the built-in stats system
	+ README updates

- 0.4.14
	+ rename return from command-line helper from r.ret to r.cli

- 0.4.13
	+ fix README documention, "failed" was renamed to "missing" a while ago
	+ add helper function to run command-line programs in the worker (with example paramsurvey-cli.py)

- 0.4.12
	+ update changelog before tagging next time, eh

- 0.4.11
	+ new pandas raises fewer TypeErrors
	+ ray 3.9 now has a wheel, so test with it
	+ change to main from master

- 0.4.10
	+ infer_category=False option for .product(), because Pandas annoys users

- 0.4.9
	+ advertise hidden logfile when there are tracebacks in it
	+ change API for backend-specific keyword arguments to init/map

- 0.4.8
	+ requires (and works with) ray>=1
	+ added ncores and max_tasks_per_child kwargs for init()

- 0.4.7
	+ changed API for return value of paramsurvey.map()
	+ changed API for the worker function (raw_stats moved into system_kwargs)
	+ bugfix: environment variables now actually override everything

- 0.4.6
	+ communicate progress in ray backend even if no work finishng
	+ .paramsurvey-DATE-TIME.log
