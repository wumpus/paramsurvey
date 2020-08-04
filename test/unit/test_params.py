from collections import OrderedDict
import sys

import pandas as pd
import numpy as np

import paramsurvey.params


def test_product():
    df1 = pd.DataFrame({'col1': [1, 2]})
    df2 = pd.DataFrame({'col2': [3, 4]})
    df3 = pd.DataFrame({'col3': [5, 6]})

    df = paramsurvey.params.product(df1, df2, df3)
    assert len(df) == 8

    nuniques = df.nunique('rows')
    assert np.array_equal(nuniques.values, np.array([2, 2, 2])), 'each column has 2 unique values'

    col3 = df['col3']
    col3a = pd.Series([5, 6, 5, 6, 5, 6, 5, 6], name='col3')
    assert col3.equals(col3a), 'verify that the last argument to product() varies the fastest'

    ps1 = pd.Series([1, 2], name='col1')
    ps2 = pd.Series([3, 4], name='col2')
    ps3 = pd.Series([5, 6], name='col3')
    df_series = paramsurvey.params.product(ps1, ps2, ps3)
    assert df.equals(df_series), 'can create a product from series, too'

    col3 = df['col3']
    assert col3.equals(col3a), 'verify that the last argument to product() varies the fastest'

    vi = sys.version_info
    if vi.major == 3 and vi.minor < 6:
        df_dicts = paramsurvey.params.product(OrderedDict((('col1', [1, 2]), ('col2',  [3, 4]), ('col3', [5, 6]))))
    else:
        df_dicts = paramsurvey.params.product({'col1': [1, 2], 'col2': [3, 4], 'col3': [5, 6]})
    assert df.equals(df_dicts)

    col3 = df['col3']
    assert col3.equals(col3a), 'verify that the last argument to product() varies the fastest'

    df_dicts = paramsurvey.params.product({'col1': [1, 2]}, {'col2': [3, 4]}, {'col3': [5, 6]})
    assert df.equals(df_dicts)

    col3 = df['col3']
    assert col3.equals(col3a), 'verify that the last argument to product() varies the fastest'

    df_dicts = paramsurvey.params.product({'col1': [1, 2]}, {'col2': [3, 4]})
    df_dicts = paramsurvey.params.product(df_dicts, {'col3': [5, 6]})
    assert df.equals(df_dicts)

    col3 = df['col3']
    assert col3.equals(col3a), 'verify that the last argument to product() varies the fastest'

    df_longer = paramsurvey.params.product(df_dicts, {'col4': [7, 8]})
    assert len(df_longer) == 16
    assert not df.equals(df_longer)
