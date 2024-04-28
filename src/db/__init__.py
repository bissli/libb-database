import wrapt

from db.db import *

__all__ = [
    'Options',
    'transaction',
    'callproc',
    'connect',
    'execute',
    'delete',
    'insert',
    'update',
    'insert_row',
    'insert_rows',
    'select',
    'select_column',
    'select_column_unique',
    'select_row',
    'select_row_or_none',
    'select_scalar',
    'select_scalar_or_none',
    'update_or_insert',
    'update_row',
    'create_record'
    ]


@wrapt.patch_function_wrapper('db.db', 'create_record')
def patch_create_record(wrapped, instance, args, kwargs):
    """Create a dataframe from the raw rows, column names
    and column types. Default behavior.
    """
    import pandas as pd
    cursor, *_ = args
    data = cursor.fetchall()  # iterdict (dictcursor)
    return pd.DataFrame.from_records(list(data))
