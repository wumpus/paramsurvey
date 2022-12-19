#!/bin/sh

set -e

if [ ! -z "$COVERAGE" ]; then
    COVERAGE="--cov-append --cov-branch --cov paramsurvey -v -v"
fi

PARAMSURVEY_BACKEND=multiprocessing pytest $COVERAGE $PYTEST_STDERR_VISIBLE $1
PARAMSURVEY_BACKEND=multiprocessing pytest $COVERAGE $PYTEST_STDERR_VISIBLE $1/test-multiprocessing-failure.py
PARAMSURVEY_BACKEND=multiprocessing pytest $COVERAGE $PYTEST_STDERR_VISIBLE $1/test-env-overrides.py
PARAMSURVEY_BACKEND=multiprocessing pytest $COVERAGE $PYTEST_STDERR_VISIBLE $1/test-generic_init_max_tasks_per_child.py
PARAMSURVEY_BACKEND=multiprocessing pytest $COVERAGE $PYTEST_STDERR_VISIBLE $1/test-multiprocessing-nest.py
OMP_NUM_THREADS=3 PARAMSURVEY_BACKEND=multiprocessing pytest $COVERAGE $PYTEST_STDERR_VISIBLE $1/test-multiprocessing-omp.py
