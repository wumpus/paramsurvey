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
