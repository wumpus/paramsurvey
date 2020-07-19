#!/bin/sh

if [ ! -z "$ONLY_BUILTINS" ]; then
    exit 0
fi

PORT=6379
REDIS_PASSWORD=thehfhghedhdjfhgfhdhdhdf
echo $(hostname):$PORT $REDIS_PASSWORD > ~/.ray-test-72363726-details

ray start --head --redis-port=$PORT --redis-password=$REDIS_PASSWORD

if [ ! -z "$COVERAGE" ]; then
    COVERAGE="--cov-report= --cov-append --cov paramsurvey -v -v"
fi

# in order to find both the uninstalled paramsurvey and our test program,
# which ray needs to pickle, the PYTHONPATH must be manipulated. See
# ../Makefile for clues.

RAY_HEAD_FILE=~/.ray-test-72363726-details PARAMSURVEY_BACKEND=ray pytest $COVERAGE

ray stop
