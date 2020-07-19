# paramsurvey

[![Build Status](https://travis-ci.org/wumpus/paramsurvey.svg?branch=master)](https://travis-ci.org/wumpus/paramsurvey) [![Coverage Status](https://coveralls.io/repos/github/wumpus/paramsurvey/badge.svg?branch=master)](https://coveralls.io/github/wumpus/paramsurvey?branch=master) [![Apache License 2.0](https://img.shields.io/github/license/wumpus/paramsurvey.svg)](LICENSE)

paramsurvey is a set of tools for creating and executing parameter surveys.

paramsurvey has a pluggable parallel backend. The supported backends at present
are python's multiprocessing module, and computing cluster software `ray`. An `mpi` backend is planned.

## Installing

```
$ pip install paramsurvey
$ pip install paramsurvey[ray]
```

## Philosophy

