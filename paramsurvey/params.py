'''
helper functions to assist building sets of parameters

build a human-friendly string usable for filenames and graphing purposes
examine a list of work and eliminate units that have already been done
'''
import pandas as pd


def product(*args):
    df = pd.DataFrame()
    df['asdfasdf'] = 0

    for a in args:
        if isinstance(a, dict) and len(a) > 1:
            for key, value in a.items():
                df = product_step({key: value}, df)
        else:
            df = product_step(a, df)

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
    except TypeError as e:
        # e.g. unhashable type                                                                                                                             
        return a

    asize = a.memory_usage(index=False, deep=True)
    csize = c.memory_usage(index=False, deep=True)
    if csize < asize:
        return c
    return a


def product_step(a, df):
    # coerce a into a dtype='category' to save memory
    if isinstance(a, dict):
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
            # why does this raise? well, it does.
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
            # if it is not dtype='category', make it so
            is_category = False
            try:
                if series.dtype == 'category':
                    is_category = True
            except TypeError:
                # why does this raise? well, it does.
                is_category = False

            if not is_category:
                a = _coerce_to_category(series)

    dfa = pd.DataFrame(a)
    dfa['asdfasdf'] = 0
    df = df.merge(dfa, how='outer')
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
