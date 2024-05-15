from dataclasses import dataclass, field

import pandas as pd
import pyarrow as pa

from libb import ConfigOptions, scriptname

__all__ = ['DatabaseOptions', 'data_loader']


def data_loader(backend='numpy'):
    """Custom data loader. Takes list of dictionaries as data input. User
    can overwrite data loader by passing in any similar function.
    """
    assert backend in {'numpy', 'pyarrow'}, \
        'pandas backend must be `numpy` or `pyarrow`'

    def func(data, cols):
        if backend == 'numpy':
            return pd.DataFrame.from_records(list(data), columns=cols)
        if backend == 'pyarrow':
            dataT = [[row[col] for row in data] for col in cols]  # list of cols
            return pa.table(dataT, names=cols).to_pandas(types_mapper=pd.ArrowDtype)
    return func


@dataclass
class DatabaseOptions(ConfigOptions):
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
    check_connection: bool = True
    data_loader: callable = field(default_factory=data_loader)

    def __post_init__(self):
        assert self.drivername in {'postgres', 'sqlserver', 'sqlite'}, \
            'drivername must be `postgres`, `sqlserver`, or `sqlite`'
        self.appname = self.appname or scriptname() or 'python_console'
        if self.drivername in {'postgres', 'sqlserver'}:
            for field in ('hostname', 'username', 'password', 'database',
                          'port', 'timeout'):
                assert getattr(self, field), f'field {field} cannot be None or 0'
        if self.drivername == 'sqlite':
            assert self.database, 'field database cannot be None'
