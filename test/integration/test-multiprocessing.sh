#!/bin/sh

if [ ! -z "$COVERAGE" ]; then
    COVERAGE="--cov-report=xml --cov-append --cov-branch --cov paramsurvey -v -v"
fi

PARAMSURVEY_BACKEND=multiprocessing pytest $COVERAGE $PYTEST_STDERR_VISIBLE $1
PARAMSURVEY_BACKEND=multiprocessing pytest $COVERAGE $PYTEST_STDERR_VISIBLE $1/test-multiprocessing-failure.py
PARAMSURVEY_BACKEND=multiprocessing pytest $COVERAGE $PYTEST_STDERR_VISIBLE $1/test-env-overrides.py
PARAMSURVEY_BACKEND=multiprocessing pytest $COVERAGE $PYTEST_STDERR_VISIBLE $1/test-generic_init_max_tasks_per_child.py
