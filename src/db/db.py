import atexit
import datetime
import logging
import re
import sqlite3
import time
from collections.abc import Sequence
from dataclasses import dataclass, fields
from functools import wraps
from numbers import Number
from typing import Any

import pandas as pd
import psycopg
import pyarrow as pa
import pymssql
from more_itertools import flatten
from psycopg import ClientCursor
from psycopg.postgres import types
from psycopg.types.datetime import DateLoader, TimestampLoader
from psycopg.types.datetime import TimestamptzLoader

from date import Date, DateTime
from libb import ConfigOptions, load_options, scriptname

logger = logging.getLogger(__name__)

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
    'upsert_rows',
    'select',
    'select_column',
    'select_column_unique',
    'select_row',
    'select_row_or_none',
    'select_scalar',
    'select_scalar_or_none',
    'update_or_insert',
    'update_row',
    'create_dataframe'
    ]


# == psycopg adapters


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


# == sqlite adapter


def adapt_date_iso(val):
    """Adapt datetime.date to ISO 8601 date."""
    return val.isoformat()


def adapt_datetime_iso(val):
    """Adapt datetime.datetime to timezone-naive ISO 8601 date."""
    return val.isoformat()


sqlite3.register_adapter(datetime.date, adapt_date_iso)
sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)
sqlite3.register_adapter(Date, adapt_date_iso)
sqlite3.register_adapter(DateTime, adapt_datetime_iso)


def convert_date(val):
    """Convert ISO 8601 date to datetime.date object."""
    return Date.fromisoformat(val.decode())


def convert_datetime(val):
    """Convert ISO 8601 datetime to datetime.datetime object."""
    return DateTime.fromisoformat(val.decode())


sqlite3.register_converter('date', convert_date)
sqlite3.register_converter('datetime', convert_datetime)


# == psycopg type mapping


oid = lambda x: types.get(x).oid
aoid = lambda x: types.get(x).array_oid

postgres_types = {}
for v in [
    oid('"char"'),
    oid('bpchar'),
    oid('character varying'),
    oid('character'),
    oid('json'),
    oid('name'),
    oid('text'),
    oid('uuid'),
    oid('varchar'),
]:
    postgres_types[v] = str
for v in [
    oid('bigint'),
    oid('int2'),
    oid('int4'),
    oid('int8'),
    oid('integer'),
]:
    postgres_types[v] = int
for v in [
    oid('float4'),
    oid('float8'),
    oid('double precision'),
    oid('numeric'),
]:
    postgres_types[v] = float
for v in [oid('date')]:
    postgres_types[v] = Date
for v in [
    oid('time'),
    oid('time with time zone'),
    oid('time without time zone'),
    oid('timestamp with time zone'),
    oid('timestamp without time zone'),
    oid('timestamptz'),
    oid('timetz'),
    oid('timestamp'),
]:
    postgres_types[v] = DateTime
for v in [oid('bool'), oid('boolean')]:
    postgres_types[v] = bool
for v in [oid('bytea'), oid('jsonb')]:
    postgres_types[v] = bytes
postgres_types[aoid('int2vector')] = list
for k in list(postgres_types):
    postgres_types[aoid(k)] = list


# == defined errors


PgIntegrityError = psycopg.IntegrityError
PgUniqueViolation = psycopg.errors.UniqueViolation
PgProgrammingError = psycopg.ProgrammingError

ProgrammingError = pymssql.DatabaseError
IntegrityError = pymssql.IntegrityError
GeneralError = pymssql.Error
OperationalError = pymssql.OperationalError


# == main


CONNECTIONOBJ = (psycopg.Connection, pymssql.Connection)


def isconnection(cn):
    """Utility to test for the presence of a mock connection

    >>> cn = __POSTGRES_CONNECTION()
    >>> isconnection(cn)
    True
    >>> isconnection('mock')
    False
    """
    try:
        return isinstance(cn.connection, CONNECTIONOBJ)
    except:
        return False


class ConnectionWrapper:
    """Wraps a connection object so we can keep track of the
    calls and execution time of any cursors used by this connection.
      haq from https://groups.google.com/forum/?fromgroups#!topic/pyodbc/BVIZBYGXNsk
    Can be used as a context manager ... with connect(...) as cn: pass
    """

    def __init__(self, connection, cleanup=True):
        self.connection = connection
        self.calls = 0
        self.time = 0
        if cleanup:
            atexit.register(self.cleanup)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.connection.close()

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
        if isinstance(self.connwrapper.connection, pymssql.Connection | sqlite3.Connection):
            logger.debug(f'SQL:\n{sql}\nargs: {str(args)}\nkwargs: {str(kwargs)}')
        self.cursor.execute(sql, *args, **kwargs)
        end = time.time()
        self.connwrapper.addcall(end - start)
        logger.debug('Query time=%f' % (end - start))


def dumpsql(func):
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


def placeholder(func):
    """Handle placeholder by connection type"""
    @wraps(func)
    def wrapper(cn, sql, *args, **kwargs):
        if isinstance(cn.connection, psycopg.Connection):
            sql = sql.replace('?', '%s')
        if isinstance(cn.connection, pymssql.Connection):
            sql = sql.replace('%s', '?')
        if isinstance(cn.connection, sqlite3.Connection):
            sql = sql.replace('%s', '?')
        return func(cn, sql, *args, **kwargs)
    return wrapper


def _page_mssql(sql, order_by, offset, limit):
    """Wrap a MSSQL stmt in sql server windowing notation, strip existing order by"""
    if isinstance(order_by, list | tuple):
        order_by = ','.join(order_by)
    match = re.search('order by', sql, re.IGNORECASE)
    if match:
        sql = sql[: match.start()]
    logger.info(f'Paged MSSQL statement with {order_by} {offset} {limit}')
    return f"""
{sql}
ORDER BY {order_by}
OFFSET {offset} ROWS
FETCH NEXT {limit} ROWS ONLY"""


def _page_pgsql(sql, order_by, offset, limit):
    """Wrap a Postgres SQL stmt in sql server windowing notation, strip existing order by"""
    if isinstance(order_by, list | tuple):
        order_by = ','.join(order_by)
    match = re.search('order by', sql, re.IGNORECASE)
    if match:
        sql = sql[: match.start()]
    logger.info(f'Paged Postgres statement with {order_by} {offset} {limit}')
    return f"""
{sql}
ORDER BY {order_by}
LIMIT {limit} OFFSET {offset}"""


@dataclass
class Options(ConfigOptions):
    """Options

    supported driver names: `postgres`, `sqlserver`, `sqlite`

    """

    drivername: str = 'postgres'
    hostname: str = None
    username: str = None
    password: str = None
    database: str = None
    port: int = 0
    timeout: int = 0
    appname: str = None
    cleanup: bool = True

    def __post_init__(self):
        assert self.drivername in {'postgres', 'sqlserver', 'sqlite'}, \
            'drivername must be `postgres`, `sqlserver`, or `sqlite`'
        self.appname = self.appname or scriptname() or 'python_console'
        if self.drivername in {'postgres', 'sqlserver'}:
            for field in fields(self):
                assert getattr(self, field.name), f'field {field.name} cannot be None or 0'
        if self.drivername == 'sqlite':
            assert self.database, 'field database cannot be None'


class LoggingCursor(ClientCursor):
    """See https://github.com/psycopg/psycopg/discussions/153 if
    considering replacing raw connections with SQLAlchemy
    """
    def execute(self, query, params=None):
        formatted = self.mogrify(query, params)
        logger.debug('SQL:\n' + formatted)
        result = super().execute(query, params)
        return result


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

    >>> cn = __POSTGRES_CONNECTION()
    >>> df = select(cn, __POSTGRES_TEST_QUERY())
    >>> assert len(df.columns) == 2
    >>> assert len(df) > 10
    """
    if isinstance(options, Options):
        for field in fields(options):
            kw.pop(field.name, None)
    conn = None
    if options.drivername == 'sqlite':
        conn = sqlite3.connect(database=options.database)
        conn.row_factory = sqlite3.Row
    if options.drivername == 'postgres':
        conn = psycopg.connect(
            dbname=options.database,
            host=options.hostname,
            user=options.username,
            password=options.password,
            port=options.port,
            connect_timeout=options.timeout,
            cursor_factory=LoggingCursor
        )
    if options.drivername == 'sqlserver':
        conn = pymssql.connect(
            database=options.database,
            user=options.username,
            server=options.hostname,
            password=options.password,
            appname=options.appname,
            timeout=options.timeout,
            port=options.port,
        )
    if not conn:
        raise AttributeError(f'{options.drivername} is not supported, see Options docstring')
    return ConnectionWrapper(conn, options.cleanup)


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


@dumpsql
@placeholder
def select(cn, sql, *args, **kwargs) -> pd.DataFrame:
    cursor = _dict_cur(cn)
    cursor.execute(sql, args)
    return create_dataframe(cursor, **kwargs)


@dumpsql
@placeholder
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


class DictRowFactory:
    """Rough equivalent of psycopg2.extras.RealDictCursor
    """
    def __init__(self, cursor: psycopg.ClientCursor[Any]):
        self.fields = [(c.name, postgres_types.get(c.type_code)) for c in (cursor.description or [])]

    def __call__(self, values: Sequence[Any]) -> dict[str, Any]:
        return {name: cast(value)
                if isinstance(value, Number)
                else value
                for (name, cast), value in zip(self.fields, values)}


def _dict_cur(cn):
    typ = type(cn.connection)
    if typ == psycopg.Connection:
        return cn.cursor(row_factory=DictRowFactory)
    if typ == pymssql.Connection:
        return cn.cursor(as_dict=True)
    if typ == sqlite3.Connection:
        return cn.cursor()
    raise ValueError('Unknown connection type')


def create_dataframe(cursor) -> pd.DataFrame:
    """Patchable function (see `__init__.py`) that wraps raw
    cursor with object
    """
    if isinstance(cursor.connwrapper.connection, psycopg.Connection):
        cols = [c.name for c in cursor.description]
    if isinstance(cursor.connwrapper.connection, pymssql.Connection | sqlite3.Connection):
        cols = [c[0] for c in cursor.description]
    data = cursor.fetchall()  # iterdict (dictcursor)
    dataT = [[row[col] for row in data] for col in cols]  # list of cols
    return pa.table(dataT, names=cols).to_pandas(types_mapper=pd.ArrowDtype)


def select_column(cn, sql, *args):
    """When we query a single select parameter, return just
    that dataframe column.

    >>> cn = __POSTGRES_CONNECTION()
    >>> df = select_column(cn, __POSTGRES_TEST_QUERY(1))
    >>> assert isinstance(df, pd.Series)
    >>> assert len(df) > 10
    """
    obj = select(cn, sql, *args)
    assert len(obj.columns) == 1, 'Expected one col, got %d' % len(obj.columns)
    return obj[obj.columns[0]]


def select_column_unique(cn, sql, *args):
    return set(select_column(cn, sql, *args))


def select_row(cn, sql, *args):
    obj = select(cn, sql, *args)
    assert len(obj) == 1, 'Expected one row, got %d' % len(obj)
    return obj.iloc[0]


def select_row_or_none(cn, sql, *args):
    obj = select(cn, sql, *args)
    if len(obj) == 1:
        return obj.iloc[0]
    return None


def select_scalar(cn, sql, *args):
    obj = select(cn, sql, *args)
    assert len(obj) == 1, 'Expected one col, got %d' % len(obj)
    return obj[obj.columns[0]].iloc[0]


def select_scalar_or_none(cn, sql, *args):
    obj = select_row_or_none(cn, sql, *args)
    if len(obj):
        return obj.iloc[0]
    return None


@dumpsql
@placeholder
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

    >>> cn = __POSTGRES_CONNECTION()
    >>> with transaction(cn) as tx:
    ...     df = tx.select(__POSTGRES_TEST_QUERY())
    >>> assert len(df.columns) == 2
    >>> assert len(df) > 10
    """

    def __init__(self, cn):
        self.connection = cn

    def __enter__(self):
        self.cursor = _dict_cur(self.connection)
        return self

    def __exit__(self, exc_type, value, traceback):
        if exc_type is not None:
            self.connection.rollback()
            logger.warning('Rolling back the current transaction')
        else:
            self.connection.commit()
            logger.debug('Committed transaction.')

    @dumpsql
    @placeholder
    def execute(self, sql, *args, returnid=None):
        self.cursor.execute(sql, args)
        if not returnid:
            return self.cursor.rowcount
        else:
            result = None
            try:
                result = self.cursor.fetchone()
            except:
                logger.warning('No results to return')
            finally:
                if not result:
                    return
            if isinstance(returnid, list | tuple):
                return [result[r] for r in returnid]
            else:
                return result[returnid]

    @dumpsql
    @placeholder
    def select(self, sql, *args, **kwargs) -> pd.DataFrame:
        cursor = self.cursor
        cursor.execute(sql, args)
        return create_dataframe(cursor, **kwargs)


@dumpsql
@placeholder
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
        rc = tx.execute(update_sql, args)
        logger.info(f'Updated {rc} rows')
        if rc:
            return rc
        rc = tx.execute(insert_sql, args)
        logger.info(f'Inserted {rc} rows')
        return rc


def upsert_rows(
    cn,
    table: str,
    rows: list[dict],
    key_cols: list = None,
    upd_only_none_cols: list = None,
    **kw
):
    """One step database insert of iterdict

    :param key_cols: columns to check for conflict
    :param upd_only_none_cols: columns that will update only if null value
    :param reset_sequence: if the table has a sequence generator (autoincrementing id), a failed
                     transaction insert will nonetheless trigger the sequence and increment.
                     This param allows us to reset the sequence after the transaction.
    :param id_name: name of the primary table id (default: 'id') for reset_sequence
    """
    assert isinstance(cn.connection, psycopg.Connection), '`upsert_rows` only supports postgres'
    if upd_only_none_cols is None:
        upd_only_none_cols=[]
    if key_cols is None:
        key_cols=[]
    reset=kw.pop('reset_sequence', False)

    cols = list(rows[0])  # assume consistency

    table_cols_sql=f'select skeys(hstore(null::{table})) as column'
    table_cols={c.lower() for c in select_column(cn, table_cols_sql)}
    table
    for col in cols.copy():
        if col.lower() not in table_cols:
            cols.remove(col)
            for row in rows:
                row.pop(col, None)
            logger.debug(f'Removed {col}, not a column of {table}')

    if key_cols:
        upd_cols=key_cols and [c for c in cols if c not in key_cols]
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
        key_cols=[row['column'] for row in select(cn, sql, table).to_dict('records')]

    upd_cols=key_cols and [c for c in cols if c not in key_cols]
    if upd_cols:
        coalesce= lambda t, c: f'{c}=coalesce({t}.{c}, excluded.{c})' \
            if c in upd_only_none_cols \
            else f'{c}=excluded.{c}'
        _key_cols = '\n'.join(key_cols)
        conflict = f"""
on conflict (
    {_key_cols}
) do update set
    {', '.join([coalesce(table, c) for c in upd_cols])}
""".strip()
    else:
        conflict = 'on conflict do nothing'
    values = ', '.join([f"({', '.join(['%s'] * len(cols))})"] * len(rows))
    table_cols_sql = f"""
insert into {table} (
    {', '.join(cols)}
)
values
{values}
{conflict}
    """.strip()
    vals = list(flatten([list(row.values()) for row in rows]))
    try:
        with transaction(cn) as tx:
            rc = tx.execute(table_cols_sql, *vals)
    finally:
        if reset:
            id_name = kw.pop('id_name', 'id')
            reset_sequence(cn, table, id_name)

    logger.info(f'Inserted {rc} rows into {table}')
    if rc != len(rows):
        logger.warning(f'{len(rows) - rc} rows were skipped due to existing contraints')
    return rc


#
# SQL helpers
#

def insert_rows(cn, table, rows: list[dict]):
    cols = list(rows[0].keys())
    vals = list(flatten([list(row.values()) for row in rows]))

    def genvals(cols, vals):
        this = ','.join(['%s']*len(cols))
        return ','.join([f'({this})']*int(len(vals)/len(cols)))

    sql = f'insert into {table} ({",".join(cols)}) values {genvals(cols, vals)}'
    return insert(cn, sql, *vals)


def insert_row(cn, table, fields, values):
    """Insert a row into a table using the supplied list of fields and values."""
    assert len(fields) == len(values), 'fields must be same length as values'
    return insert(cn, insert_row_sql(table, fields), *values)


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


if __name__ == '__main__':
    import os
    import site

    from libb import expandabspath

    HERE = os.path.dirname(os.path.abspath(__file__))
    TEST = expandabspath(os.path.join(HERE, '../../tests/fixtures'))
    site.addsitedir(TEST)
    from server import psql_docker_container

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

    def __POSTGRES_CONNECTION():
        param = {
            'drivername': 'postgres',
            'username': 'postgres',
            'password': 'password',
            'database': 'test_db',
            'hostname': 'localhost',
            'port': 5432,
            'timeout': 30,
            }
        return connect(**param)

    container = psql_docker_container()
    try:
        cn = __POSTGRES_CONNECTION()
        print(select(cn, 'select now()'))
        import doctest
        doctest.testmod(optionflags=4 | 8 | 32)
    finally:
        container.stop()
