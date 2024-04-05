import atexit
import contextlib
import logging
import sqlite3
import time
from dataclasses import dataclass, fields
from functools import wraps

import pandas as pd
from sqlalchemy import URL, create_engine

from date import Date, DateTime
from libb import BaseOptions, collapse, load_options, scriptname

logger = logging.getLogger(__name__)

CONNECTIONOBJ = []


with contextlib.suppress(ImportError):
    import psycopg
    from psycopg.types.datetime import DateLoader, TimestampLoader
    from psycopg.types.datetime import TimestamptzLoader

    class DateMixin:
        def load(self, data): return Date(super().load(data))
    class DateTimeMixin:
        def load(self, data): return DateTime(super().load(data))
    class DateTimeTzMixin:
        def load(self, data): return DateTime(super().load(data))

    class CustomDateLoader(DateMixin, DateLoader): pass
    class CustomDateTimeLoader(DateTimeMixin, TimestampLoader): pass
    class CustomDateTimeTzLoader(DateTimeTzMixin, TimestamptzLoader): pass

    psycopg.adapters.register_loader('date', CustomDateLoader)
    psycopg.adapters.register_loader('timestamp', CustomDateTimeLoader)
    psycopg.adapters.register_loader('timestamptz', CustomDateTimeTzLoader)

    CONNECTIONOBJ.append(psycopg.Connection)


with contextlib.suppress(ImportError):
    import psycopg2

    DATE = psycopg2.extensions.new_type(
        psycopg2.extensions.DATE.values+psycopg2.extensions.PYDATE.values,
        'DATE', lambda val, cur: Date(val))
    DATETIME = psycopg2.extensions.new_type(
        psycopg2.DATETIME.values+psycopg2.extensions.PYDATETIME.values,
        'DATETIME', lambda val, cur: DateTime(val))
    psycopg2.extensions.register_type(DATE)
    psycopg2.extensions.register_type(DATETIME)

    CONNECTIONOBJ.append(psycopg2.extensions.connection)


with contextlib.suppress(ImportError):
    import pymssql

    CONNECTIONOBJ.append(pymssql.Connection)


CONNECTIONOBJ = tuple(CONNECTIONOBJ)


def isconnection(cn):
    """Utility to test for the presence of a mock connection

    >>> cn = __POSTGRES_CONNECTION('postgresql+psycopg')
    >>> isconnection(cn)
    True
    >>> isconnection('mock')
    False
    """
    try:
        return isinstance(cn.connection.connection, CONNECTIONOBJ)
    except:
        return False


class ConnectionWrapper:
    """Wraps a connection object so we can keep track of the
    calls and execution time of any cursors used by this connection.
      haq from https://groups.google.com/forum/?fromgroups#!topic/pyodbc/BVIZBYGXNsk
    """

    def __init__(self, connection, cleanup=True):
        self.connection = connection
        self.calls = 0
        self.time = 0
        if cleanup:
            atexit.register(self.cleanup)

    def __getattr__(self, name):
        """Delegate any members to the underlying connection."""
        return getattr(self.connection, name)

    def cursor(self, *args, **kwargs):
        return CursorWrapper(self.connection.cursor(*args, **kwargs), self)

    def addcall(self, elapsed):
        self.time += elapsed
        self.calls += 1

    def cleanup(self):
        try:
            self.connection.close()
            logger.debug(f'Database connection lasted {self.time or 0} seconds, {self.calls or 0} queries')
        except:
            pass


class CursorWrapper:
    """Wraps a cursor object so we can keep track of the
    execute calls and time used by this cursor.
    """

    def __init__(self, cursor, connwrapper):
        self.cursor = cursor
        self.connwrapper = connwrapper

    def __getattr__(self, name):
        """Delegate any members to the underlying cursor."""
        return getattr(self.cursor, name)

    def __iter__(self):
        return IterChunk(self.cursor)

    def execute(self, sql, *args, **kwargs):
        """Time the call and tell the connection wrapper that
        created this connection.
        """
        start = time.time()
        logger.debug('SQL:\n%s\nargs: %s\nkwargs: %s' % (sql, str(args), str(kwargs)))
        self.cursor.execute(sql, *args, **kwargs)
        end = time.time()
        self.connwrapper.addcall(end - start)
        logger.debug('Query time=%f' % (end - start))


def logsql(func):
    """This is a decorator for db module functions, for logging data flowing down to driver"""
    @wraps(func)
    def wrapper(cn, sql, *args, **kwargs):
        try:
            return func(cn, sql, *args, **kwargs)
        except Exception as exc:
            logger.error(f'Error with query:\nSQL: {sql}\nARGS: {args}\nKWARGS:{kwargs}')
            logger.exception(exc)
            raise exc
    return wrapper


@dataclass
class Options(BaseOptions):

    drivername: str = 'postgresql+psycopg'
    hostname: str = None
    username: str = None
    password: str = None
    database: str = None
    port: int = 0
    timeout: int = 0
    appname: str = None
    cleanup: bool = True

    def __post_init__(self):
        self.appname = self.appname or scriptname() or 'python_console'
        for field in fields(self):
            assert getattr(self, field.name)


@load_options(cls=Options)
def connect(options: str | dict | Options | None, config=None, **kw):
    """Database connection wrapper

    Use config.py to specify database

    config.sql.<appname>.<environment>.<foo>
    ...

    cn = connect('sql.<appname>.<environment>', config=config)
    OR (legacy)
    cn = connect(dbengine='foo', hostname='bar', ...)
    ...

    >>> cn = __POSTGRES_CONNECTION('postgresql+psycopg')
    >>> df = select(cn, __POSTGRES_TEST_QUERY())
    >>> assert len(df.columns) == 2
    >>> assert len(df) > 10
    """
    if options.drivername == 'sqlite':
        conn = create_engine(f'sqlite:///{options.database}', **kw)
    else:
        url = URL.create(options.drivername,
                         username=options.username,
                         password=options.password,
                         host=options.hostname,
                         database=options.database,
                         port=options.port)
        conn = create_engine(url, **kw)
    return ConnectionWrapper(conn.raw_connection(), options.cleanup)


def IterChunk(cursor, size=5000):
    """Alternative to builtin cursor generator
    breaks fetches into smaller chunks to avoid malloc problems
    for really large queries (index screen, etc.)
    """
    while True:
        try:
            chunked = cursor.fetchmany(size)
        except:
            chunked = []
        if not chunked:
            break
        yield from chunked


@logsql
def select(cn, sql, *args, **kwargs) -> pd.DataFrame:
    cursor = _dict_cur(cn)
    cursor.execute(sql, args)
    return create_dataframe(cursor, **kwargs)


@logsql
def callproc(cn, sql, *args, **kwargs) -> pd.DataFrame:
    """Just like select above but used for stored procs which
    often return multiple resultsets because of nocount being
    off (each rowcount is a separate resultset). We walk through
    each resultset, saving and processing the one with the most
    rows.
    """
    cursor=_dict_cur(cn)
    cursor.execute(sql, args)
    return create_dataframe(cursor, **kwargs)


def _dict_cur(cn):
    typ = type(cn.connection.connection)
    with contextlib.suppress(NameError):
        if typ == psycopg2.extensions.connection: return cn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    with contextlib.suppress(NameError):
        if typ == psycopg.Connection: return cn.cursor(row_factory=psycopg.rows.dict_row)
    with contextlib.suppress(NameError):
        if typ == pymssql.Connection: return cn.cursor(as_dict=True)
    with contextlib.suppress(NameError):
        if typ == sqlite3.Connection: return cn.cursor()
    raise ValueError('Unknown connection type')


def create_dataframe(cursor) -> pd.DataFrame:
    """Create a dataframe from the raw rows, column names and column types"""

    def is_psycopg(cursor):
        with contextlib.suppress(NameError):
            if isinstance(cursor.connection, psycopg2.extensions.connection):
                return True
        with contextlib.suppress(NameError):
            if isinstance(cursor.connection, psycopg.Connection):
                return True
        return False

    def is_pymsql(cursor):
        with contextlib.suppress(NameError):
            if isinstance(cursor.connection, pymssql.Connection):
                return True
        return False

    if is_psycopg(cursor):
        data = cursor.fetchall()
        return pd.DataFrame(data)
    if is_pymsql(cursor):
        data = cursor.fetchall()
        cols=[_[0] for _ in cursor.description]
        return pd.DataFrame.from_records(list(cursor), columns=cols)
    if isinstance(cursor.connection, sqlite3.Connection):
        cols = [column[0] for column in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame.from_records(data=data, columns=cols)


def select_column(cn, sql, *args):
    """When we query a single select parameter, return just
    that dataframe column.

    >>> cn = __POSTGRES_CONNECTION('postgresql+psycopg')
    >>> df = select_column(cn, __POSTGRES_TEST_QUERY(1))
    >>> assert isinstance(df, pd.Series)
    >>> assert len(df) > 10
    """
    df = select(cn, sql, *args)
    assert len(df.columns) == 1, 'Expected one col, got %d' % len(df.columns)
    return df[df.columns[0]]


def select_column_unique(cn, sql, *args):
    return set(select_column(cn, sql, *args))


def select_row(cn, sql, *args):
    rows = select(cn, sql, *args)
    assert len(rows) == 1, 'Expected one row, got %d' % len(rows)
    return rows[0]


def select_row_or_none(cn, sql, *args):
    rows = select(cn, sql, *args)
    if len(rows) == 1:
        return rows[0]
    return None


def select_scalar(cn, sql, *args):
    df = select(cn, sql, *args)
    assert len(df.index) == 1, 'Expected one row, got %d' % len(df.index)
    return df[df.columns[0]].iloc[0]


def select_scalar_or_none(cn, sql, *args):
    row = select_row_or_none(cn, sql, *args)
    if row:
        return row[list(row.keys())[0]]
    return None


@logsql
def execute(cn, sql, *args):
    cursor = cn.cursor()
    cursor.execute(sql, args)
    rowcount = cursor.rowcount
    cn.commit()
    return rowcount


insert = update = delete = execute


class transaction:
    """Context manager for running multiple commands in a transaction.

    with db.transaction(cn) as tx:
        tx.execute('delete from ...', args)
        tx.execute('update from ...', args)

    >>> cn = __POSTGRES_CONNECTION('postgresql+psycopg2')
    >>> with transaction(cn) as tx:
    ...     df = tx.select(__POSTGRES_TEST_QUERY())
    >>> assert len(df.columns) == 2
    >>> assert len(df) > 10
    """

    def __init__(self, cn):
        self.cn = cn

    def __enter__(self):
        self.cursor = _dict_cur(self.cn)
        return self

    def __exit__(self, exc_type, value, traceback):
        if exc_type is not None:
            self.cn.rollback()
            logger.warning('Rolling back the current transaction')
        else:
            self.cn.commit()
            logger.debug('Committed transaction.')

    @logsql
    def execute(self, sql, *args, **kwargs):
        self.cursor.execute(sql, args)
        return self.cursor.rowcount

    @logsql
    def select(self, sql, *args, **kwargs) -> pd.DataFrame:
        cursor = self.cursor
        cursor.execute(sql, args)
        return create_dataframe(cursor, **kwargs)

    @logsql
    def select_scalar(self, cn, sql, *args):
        col = select_column(cn, sql, *args)
        return col[list(col.keys())[0]]


@logsql
def insert_identity(cn, sql, *args):
    """Inject @@identity column into query for row by row unique id"""
    cursor=cn.cursor()
    cursor.execute(sql + '; select @@identity', args)
    # rowcount = cursor.rowcount
    cursor.nextset()
    identity=cursor.fetchone()[0]
    # must do the commit after retrieving data since commit closes cursor
    cn.commit()
    return identity


def update_or_insert(cn, update_sql, insert_sql, *args):
    """TODO: better way to do this query is with postgres on conflict do ..."""
    with transaction(cn) as tx:
        rc=tx.execute(update_sql, args)
        logger.info(f'Updated {rc} rows')
        if rc:
            return rc
        rc=tx.execute(insert_sql, args)
        logger.info(f'Inserted {rc} rows')
        return rc


def insert_dataframe(cn, df, table, key_cols=None, upd_only_none_cols=None, **kwargs):
    """One step database insert of dataframe

    :param key_cols: columns to check for conflict
    :param upd_only_none_cols: columns that will update only if null value
    :param reset_sequence: if the table has a sequence generator (autoincrementing id), a failed
                     transaction insert will nonetheless trigger the sequence and increment.
                     This param allows us to reset the sequence after the transaction.
    :param id_name: name of the primary table id (default: 'id') for reset_sequence
    """
    if upd_only_none_cols is None:
        upd_only_none_cols=[]
    if key_cols is None:
        key_cols=[]
    reset=kwargs.pop('reset_sequence', False)

    table_cols_sql=f'select skeys(hstore(null::{table})) as column'
    table_cols={c.lower() for c in select_column(cn, table_cols_sql)}
    for col in df.cols[:]:
        if col.lower() not in table_cols:
            df.remove_column(col)
            logger.debug(f'Removed {col}, not a column of {table}')

    if key_cols:
        upd_cols=key_cols and [c for c in df.cols if c not in key_cols]
    else:
        # look up table primary keys
        sql="""
select
    a.attname as column,
    format_type(a.atttypid, a.atttypmod) as type
from
    pg_index i
join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
where
    i.indrelid = %s::regclass
    and i.indisprimary
        """
        key_cols=list(select(cn, sql, table).unwind('column'))

    upd_cols=key_cols and [c for c in df.cols if c not in key_cols]
    if upd_cols:
        coalesce=(
            lambda t, c: '{c}=coalesce({t}.{c}, excluded.{c})'.format(t=t, c=c)
            if c in upd_only_none_cols else '{c}=excluded.{c}'.format(c=c)
        )
        conflict = 'on conflict ({}) do update set \n{}'.format(
            '\n,'.join(key_cols), ', '.join([coalesce(table, c) for c in upd_cols])
        )
    else:
        conflict = 'on conflict do nothing'
    values = ', '.join([f"({', '.join(['%s'] * len(df.cols))})"] * len(df))
    sql = f"""
    insert into {table} (
    {', '.join(df.cols)}
    )
    values
    {values}
    {conflict}
    """
    vals = list(collapse(list(df.unwind(*df.cols))))
    try:
        with transaction(cn) as tx:
            rc = tx.execute(sql, *vals)
    finally:
        if reset:
            id_name = kwargs.pop('id_name', 'id')
            reset_sequence(cn, table, id_name)

    logger.info(f'Inserted {rc} rows into {table}')
    if rc != len(df):
        logger.warning(f'{len(df) - rc} rows were skipped due to existing contraints')
    return rc


#
# SQL helpers
#


def insert_row(cn, table, fields, values):
    """Insert a row into a table using the supplied list of fields and values."""
    assert len(fields) == len(values), 'fields must be same length as values'
    return insert_identity(cn, insert_row_sql(table, fields), *values)


def insert_row_sql(table, fields):
    """Generate the SQL to insert a row into a table using the supplied list
    of fields and values.
    """
    cols = ','.join(fields)
    vals = ','.join(['%s'] * len(fields))
    return f'insert into {table} ({cols}) values ({vals})'


def update_row(cn, table, keyfields, keyvalues, datafields, datavalues):
    """Update the specified datafields to the supplied datavalues in a table row
    identified by the keyfields and keyvalues.
    """
    assert len(keyfields) == len(keyvalues), 'keyfields must be same length as keyvalues'
    assert len(datafields) == len(datavalues), 'datafields must be same length as datavalues'
    values = list(datavalues) + list(keyvalues)
    return update(cn, update_row_sql(table, keyfields, datafields), *values)


def update_row_sql(table, keyfields, datafields):
    """Generate the SQL to update the specified datafields in a table row
    identified by the keyfields.
    """
    for kf in keyfields:
        assert kf not in datafields, f'keyfield {kf} cannot be in datafields'
    keycols = ' and '.join([f'{f}=%s' for f in keyfields])
    datacols = ','.join([f'{f}=%s' for f in datafields])
    return f'update {table} set {datacols} where {keycols}'


def reset_sequence(cn, table, identity='id'):
    sql = f"""
select
    setval(pg_get_serial_sequence('{table}', '{identity}'), coalesce(max({identity}),0)+1, false)
from
    {table}
    """
    if isinstance(cn, transaction):
        cn.execute(sql)
    else:
        execute(cn, sql)
    logger.debug(f'Reset sequence for {table=}')


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
    'insert_row_sql',
    'select',
    'select_column',
    'select_column_unique',
    'select_row',
    'select_row_or_none',
    'select_scalar',
    'select_scalar_or_none',
    'update_or_insert',
    'update_row',
    'update_row_sql',
    ]


if __name__ == '__main__':
    def __POSTGRES_TEST_QUERY(numcols=2):
        sql="""
select
    {} {}
from
    pg_index i
join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
where
    i.indisprimary
        """
        if numcols==1:
            return sql.format('a.attname as column', '')
        if numcols==2:
            return sql.format('a.attname as column,', 'format_type(a.atttypid, a.atttypmod) as type')

    def __POSTGRES_CONNECTION(driver='postgresql+psycopg'):
        import os
        param = {
            'drivername': driver,
            'username': os.getenv('PGUSER'),
            'password': os.getenv('PGPASSWORD'),
            'database': os.getenv('PGDATABASE'),
            'hostname': os.getenv('PGHOST'),
            'port': 5432,
            }
        return connect(**param)
    __import__('doctest').testmod(optionflags=4 | 8 | 32)
