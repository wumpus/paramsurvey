#!/usr/bin/env python

import subprocess

import paramsurvey
from paramsurvey.utils import subprocess_run_worker


def main():
    paramsurvey.init(backend='multiprocessing')  # or 'ray', if you installed it

    command = 'echo {}'
    psets = [{'run_args': 1}, {'run_args': 2}, {'run_args': 3}]
    for p in psets:
        p['run_args'] = command.format(p['run_args']).split()

    # see the subprocess.run documentation for these options:
    #user_kwargs = {'run_kwargs': {'stdout': subprocess.PIPE, 'encoding': 'utf-8', 'shell': True}}
    user_kwargs = {'run_kwargs': {'stdout': subprocess.PIPE, 'encoding': 'utf-8'}}

    results = paramsurvey.map(subprocess_run_worker, psets, user_kwargs=user_kwargs)

    assert results.progress.failures == 0

    for r in results.itertuples():
        # r.cli is a subprocess.CompletedProcess object
        print(r.cli.returncode, r.cli.stdout.rstrip())


if __name__ == '__main__':
    main()
