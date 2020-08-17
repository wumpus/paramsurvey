from collections import OrderedDict
import sys
import pytest
import os

import pandas as pd
import numpy as np

import paramsurvey.params


def test__coerce_to_category():
    # always fires, unless the types don't work out
    # does not crash with weird datatypes
    # returns the same thing if doesn't fire

    s1 = pd.Series(range(10))
    s2 = paramsurvey.params._coerce_to_category(s1)
    assert s2.dtype == 'category'


def test__infer_category():
    s1 = pd.Series(range(10))
    s2 = paramsurvey.params._infer_category(s1)
    try:
        assert s2.dtype != 'category'
    except TypeError:  # oh, pandas
        assert True
    assert s1.equals(s2)

    s1 = pd.Series([[0]] * 1000)  # unhashable type
    s2 = paramsurvey.params._infer_category(s1)
    assert s1.equals(s2)

    s1 = pd.Series([0] * 1000)
    s2 = paramsurvey.params._infer_category(s1)
    assert s2.dtype == 'category'


def test_product():
    df1 = pd.DataFrame({'col1': [1, 2]})
    df2 = pd.DataFrame({'col2': [3, 4]})
    df3 = pd.DataFrame({'col3': [5, 6]})

    df = paramsurvey.params.product(df1, df2, df3)
    assert len(df) == 8
    assert df['col1'].dtype == 'category'

    nuniques = df.nunique('rows')
    assert np.array_equal(nuniques.values, np.array([2, 2, 2])), 'each column has 2 unique values'

    col3 = df['col3']
    col3ac = pd.Series([5, 6, 5, 6, 5, 6, 5, 6], name='col3', dtype='category')
    assert col3.equals(col3ac), 'verify that the last argument to product() varies the fastest'

    ps1 = pd.Series([1, 2], name='col1')
    ps2 = pd.Series([3, 4], name='col2')
    ps3 = pd.Series([5, 6], name='col3')
    df_series = paramsurvey.params.product(ps1, ps2, ps3)
    # assert pandas.testing.assert_frame_equal(df, df_series, check_column_type=False, check_dtype=False)  # no bueno
    assert df.equals(df_series), 'can create a product from series, too'

    col3 = df['col3']
    assert col3.equals(col3ac), 'verify that the last argument to product() varies the fastest'

    vi = sys.version_info
    if vi.major == 3 and vi.minor < 6:
        df_dicts = paramsurvey.params.product(OrderedDict((('col1', [1, 2]), ('col2',  [3, 4]), ('col3', [5, 6]))))
    else:
        df_dicts = paramsurvey.params.product({'col1': [1, 2], 'col2': [3, 4], 'col3': [5, 6]})
    assert df.equals(df_dicts)

    col3 = df['col3']
    assert col3.equals(col3ac), 'verify that the last argument to product() varies the fastest'

    df_dicts = paramsurvey.params.product({'col1': [1, 2]}, {'col2': [3, 4]}, {'col3': [5, 6]})
    assert df.equals(df_dicts)

    col3 = df['col3']
    assert col3.equals(col3ac), 'verify that the last argument to product() varies the fastest'

    df_dicts = paramsurvey.params.product({'col1': [1, 2]}, {'col2': [3, 4]})
    df_dicts = paramsurvey.params.product(df_dicts, {'col3': [5, 6]})
    assert df.equals(df_dicts)

    col3 = df['col3']
    assert col3.equals(col3ac), 'verify that the last argument to product() varies the fastest'

    df_longer = paramsurvey.params.product(df_dicts, {'col4': [7, 8]})
    assert len(df_longer) == 16
    assert not df.equals(df_longer)


def test_add_column():
    df = paramsurvey.params.product({'col1': [1, 2]}, {'col2': [3, 4]}, {'col3': [5, 6]})
    assert len(df) == 8

    df2 = paramsurvey.params.add_column(df, 'col4', lambda row: row['col1'] + row['col2'])

    assert len(df2) == 8
    assert df2['col4'].to_numpy().sum() == (1+2)*4 + (3+4)*4

    df3 = paramsurvey.params.add_column(df, 'col4', lambda row: row['col1'] + row['col2'], infer_category=False)
    assert df3.equals(df2)  # infer not called

    df = pd.DataFrame([{'a': 0}] * 1000)
    df4 = paramsurvey.params.add_column(df, 'col4', lambda row: 0)
    assert df4['col4'].dtype == 'category'

    df4 = paramsurvey.params.add_column(df, 'col4', lambda row: 0, infer_category=False)
    try:
        assert df4['col4'].dtype != 'category'
    except TypeError:  # oh, pandas
        pass


def params(m, n):
    d = {}
    for a in range(n):
        d['a{}'.format(a)] = range(m)
    psets = paramsurvey.params.product(d)
    return psets


def test_param_stress():
    vmem0 = paramsurvey.utils.vmem()
    psets = params(1000, 2)  # 1 million
    vmem1 = paramsurvey.utils.vmem()
    assert vmem1 - vmem0 < 0.1, 'pretty loose limit'

    vmem0 = paramsurvey.utils.vmem()
    psets = params(5000, 2)  # 25 million
    print('memory')
    print(psets.memory_usage(deep=True))
    vmem1 = paramsurvey.utils.vmem()
    print('change in vmem', vmem1-vmem0)
    # index is an int64, so usage is at least (int64 + 2*int16) = 8 bytes * 25mm = 0.4gbyte
    assert vmem1 - vmem0 < 1.04, 'tight limit that requires int16 type to pass'

    if 'TRAVIS_CPU_ARCH' in os.environ and os.environ['TRAVIS_CPU_ARCH'] == 's390x':
        return

    vmem0 = paramsurvey.utils.vmem()
    psets = params(100, 4)  # 100 million int8
    print('memory')
    print(psets.memory_usage(deep=True))
    vmem1 = paramsurvey.utils.vmem()
    print('change in vmem', vmem1-vmem0)
    # index is an int64, so usage is at least (int64 + 4*int8) = 8 bytes * 100mm = 0.8gbyte
    assert vmem1 - vmem0 < 3.4, 'tight limit that requires int8 to pass'


def test_param_quirks():
    # unhashable type 'list' will be inefficient but shouldn't crash
    psets = paramsurvey.params.product({'a': [[1], [2], [3]]})
    assert len(psets) == 3
    with pytest.raises(TypeError):
        # why does this assert a type error?
        assert psets['a'].dtype != 'category'
