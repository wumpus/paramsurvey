#!/bin/sh

if [ ! -z "$COVERAGE" ]; then
    COVERAGE="--cov-report= --cov-append --cov-branch --cov paramsurvey -v -v"
fi

if [ ! -z "$ONLY_BUILTINS" ]; then
    PARAMSURVEY_BACKEND=ray pytest $COVERAGE $1
    # expected to fail
    # not exercised in "make test", but it is in travis CI
    test $0 -ne 0 || exit 1
fi

PORT=6379
REDIS_PASSWORD=thehfhghedhdjfhgfhdhdhdf
echo $(hostname):$PORT $REDIS_PASSWORD > ~/.ray-test-72363726-details

GIGABYTE=1000000000  # close enough

ray start --head --redis-port=$PORT --redis-password=$REDIS_PASSWORD --memory $GIGABYTE --object-store-memory $GIGABYTE --redis-max-memory $GIGABYTE

# in order to find both the uninstalled paramsurvey and our test program,
# which ray needs to pickle, the PYTHONPATH must be manipulated. See
# ../Makefile for clues.

RAY_HEAD_FILE=~/.ray-test-72363726-details PARAMSURVEY_BACKEND=ray pytest $COVERAGE $1
RAY_LOCAL_MODE=1 RAY_HEAD_FILE=~/.ray-test-72363726-details PARAMSURVEY_BACKEND=ray pytest $COVERAGE $PYTEST_STDERR_VISIBLE $1

ray stop

