'''
helper functions to assist building sets of parameters

build a human-friendly string usable for filenames and graphing purposes
examine a list of work and eliminate units that have already been done
'''
import sys

import pandas as pd

from . import utils


def product(*args, infer_category=True):
    df = pd.DataFrame()
    df['asdfasdf'] = 0

    for a in args:
        if isinstance(a, dict) and len(a) > 1:
            for key, value in a.items():
                df = product_step({key: value}, df, infer_category=infer_category)
        else:
            df = product_step(a, df, infer_category=infer_category)

    df = df.drop(columns=['asdfasdf'])
    return df


def _coerce_to_category(a):
    try:
        return pd.Series(a, dtype='category')
    except TypeError:  # e.g. unhashable type like 'list'
        return a


def _infer_category(a):
    try:
        c = pd.Series(a, dtype='category')
    except TypeError:
        # e.g. unhashable type
        return a

    asize = a.memory_usage(index=False, deep=True)
    csize = c.memory_usage(index=False, deep=True)
    if csize < asize:
        return c
    return a


def product_step(a, df, infer_category=True):
    # coerce a into a dtype='category' to save memory
    if not infer_category:
        pass
    elif isinstance(a, dict):
        assert len(a) == 1
        for k, v in a.items():
            a = pd.Series(v, name=k)
            a = _coerce_to_category(a)
    elif isinstance(a, pd.Series):
        is_category = False
        try:
            if a.dtype == 'category':
                is_category = True
        except TypeError:
            # why does this raise? well, in older versions of pandas...
            is_category = False

        if not is_category:
            a = _coerce_to_category(a)
    elif isinstance(a, pd.DataFrame):
        # this can be a dataframe with 1+ columns
        # we don't always want to coerce these to dtype='category'
        if len(a.columns) == 1:
            # extract the series
            name = a.columns.values[0]
            series = a[name]
            a = _coerce_to_category(series)
    else:
        raise ValueError('invalid type {} in product construction'.format(type(a)))

    dfa = pd.DataFrame(a)
    dfa['asdfasdf'] = 0

    vmem0 = utils.vmem()
    df = df.merge(dfa, how='outer')
    vmem1 = utils.vmem()
    if vmem1 - vmem0 > 0.1:
        print('paramsurvey.params.product memory warning: memory increased by {} gigabytes'.format(vmem1 - vmem0), file=sys.stderr)

    return df


def add_column(df, name, func, infer_category=True):
    values = []
    for pset in df.itertuples(index=False):
        values.append(func(pset._asdict()))
    s = pd.Series(values, index=df.index)

    if infer_category:
        s = _infer_category(s)

    ret = df.copy()
    ret[name] = s
    return ret
