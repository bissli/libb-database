from dataclasses import dataclass, fields

from libb import ConfigOptions, scriptname

__all__ = ['DatabaseOptions']


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

    def __post_init__(self):
        assert self.drivername in {'postgres', 'sqlserver', 'sqlite'}, \
            'drivername must be `postgres`, `sqlserver`, or `sqlite`'
        self.appname = self.appname or scriptname() or 'python_console'
        if self.drivername in {'postgres', 'sqlserver'}:
            for field in fields(self):
                assert getattr(self, field.name), f'field {field.name} cannot be None or 0'
        if self.drivername == 'sqlite':
            assert self.database, 'field database cannot be None'
