#!/bin/sh

if [ ! -z "$ONLY_BUILTINS" ]; then
    exit 0
fi

PORT=6379
REDIS_PASSWORD=thehfhghedhdjfhgfhdhdhdf
echo $(hostname):$PORT $REDIS_PASSWORD > ~/.ray-test-72363726-details

ray start --head --redis-port=$PORT --redis-password=$REDIS_PASSWORD

PYTHONPATH=.. RAY_HEAD_FILE=~/.ray-test-72363726-details PARAMSURVEY_BACKEND=ray pytest

ray stop
