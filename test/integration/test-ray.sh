#!/bin/sh

set -e

if [ ! -z "$COVERAGE" ]; then
    COVERAGE="--cov-append --cov-branch --cov paramsurvey -v -v"
fi

if [ ! -z "$ONLY_BUILTINS" ]; then
    PARAMSURVEY_BACKEND=ray pytest $COVERAGE $1/test-only-builtins.py
    exit 0
fi

# ray 1.13 introduced the following:
ray disable-usage-stats || /bin/true

RPORT=6379
OPORT=8076
CLIENT=10001

REDIS_PASSWORD=thehfhghedhdjfhgfhdhdhdf
# should this be RPORT or OPORT?
# RPORT works with ray 1.15
echo $(hostname):$RPORT $REDIS_PASSWORD > ~/.ray-test-72363726-details

GIGABYTE=1000000000  # close enough

ray stop  # in case there's an a leftover ray

# --include-dashboard starts at ray 1.13
ray start --head --port=$RPORT --object-manager-port=$OPORT --redis-password=$REDIS_PASSWORD --memory $GIGABYTE --object-store-memory $GIGABYTE --redis-max-memory $GIGABYTE $RAY_START_EXTRAS

# in order to find both the uninstalled paramsurvey and our test program,
# which ray needs to pickle, the PYTHONPATH must be manipulated. See
# ../../Makefile for clues.

RAY_HEAD_FILE=~/.ray-test-72363726-details PARAMSURVEY_BACKEND=ray pytest $COVERAGE $RAY_PYTEST_EXTRAS $1
RAY_LOCAL_MODE=1 RAY_HEAD_FILE=~/.ray-test-72363726-details PARAMSURVEY_BACKEND=ray pytest $COVERAGE $PYTEST_STDERR_VISIBLE $RAY_PYTEST_EXTRAS $1

RAY_HEAD_FILE=~/.ray-test-72363726-details PARAMSURVEY_BACKEND=ray pytest $COVERAGE $RAY_PYTEST_EXTRAS $1/test-generic_init_max_tasks_per_child.py
# do not test max_tasks with local mode

ray stop

