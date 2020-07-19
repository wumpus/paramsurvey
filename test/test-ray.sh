#!/bin/sh

PORT=6379
REDIS_PASSWORD=thehfhghedhdjfhgfhdhdhdf
echo $(hostname):$PORT $REDIS_PASSWORD > ~/.ray-test-72363726-details

PYTHONPATH=.. RAY_HEAD_FILE=~/.ray-test-72363726-details PARAMSURVEY_BACKEND=ray pytest
