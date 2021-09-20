#!/usr/bin/env python

import numpy as np

import paramsurvey
import paramsurvey.params


# A fully-complicated example of generating the list of psets

# this product() function works like itertools.product()
# see: https://docs.python.org/3/library/itertools.html#itertools.product

psets = paramsurvey.params.product({'a': [1, 2, 3], 'b': [4, 5, 6]})

# psets is a Pandas Dataframe, so it's a bit difficult to add additional columns to

# example of adding an integer job number named "i", which counts from 0..N-1 over the psets

psets['i'] = np.array(range(len(psets)))

# we also have a helper function, which isn't so useful for this running count, but it can copy

psets = paramsurvey.params.add_column(psets, 'j', lambda row: row['i'])

# but if the value is easy to determine using the other columns, it works well

psets = paramsurvey.params.add_column(psets, 'outname', lambda row: 'outfile-{}-{}'.format(row['a'], row['b']))

print(psets)
