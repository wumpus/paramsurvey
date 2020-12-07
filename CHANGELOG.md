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
