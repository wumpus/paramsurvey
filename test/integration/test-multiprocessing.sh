#!/bin/sh

if [ ! -z "$COVERAGE" ]; then
    COVERAGE="--cov-report= --cov-append --cov-branch --cov paramsurvey -v -v"
fi

PARAMSURVEY_BACKEND=multiprocessing pytest $COVERAGE $PYTEST_STDERR_VISIBLE $1
PARAMSURVEY_BACKEND=multiprocessing pytest $COVERAGE $PYTEST_STDERR_VISIBLE $1/test-multiprocessing-failure.py
PARAMSURVEY_BACKEND=multiprocessing pytest $COVERAGE $PYTEST_STDERR_VISIBLE $1/test-env-overrides.py

