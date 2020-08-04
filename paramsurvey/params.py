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


def product_step(a, df):
    dfa = pd.DataFrame(a)
    dfa['asdfasdf'] = 0
    df = df.merge(dfa, how='outer')
    return df
