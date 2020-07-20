#!/bin/sh

if [ ! -z "$COVERAGE" ]; then
    COVERAGE="--cov-report= --cov-append --cov paramsurvey -v -v"
fi

PARAMSURVEY_BACKEND=multiprocessing pytest $COVERAGE $PYTEST_STDERR_VISIBLE
