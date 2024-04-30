import datetime
import sqlite3

import psycopg
from psycopg.types.datetime import DateLoader, TimestampLoader
from psycopg.types.datetime import TimestamptzLoader

from date import Date, DateTime

__all__ = ['register_adapters']


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


# == sqlite adapter


def adapt_date_iso(val):
    """Adapt datetime.date to ISO 8601 date."""
    return val.isoformat()


def adapt_datetime_iso(val):
    """Adapt datetime.datetime to timezone-naive ISO 8601 date."""
    return val.isoformat()


def convert_date(val):
    """Convert ISO 8601 date to datetime.date object."""
    return Date.fromisoformat(val.decode())


def convert_datetime(val):
    """Convert ISO 8601 datetime to datetime.datetime object."""
    return DateTime.fromisoformat(val.decode())


def register_adapters():

    psycopg.adapters.register_loader('date', CustomDateLoader)
    psycopg.adapters.register_loader('timestamp', CustomDateTimeLoader)
    psycopg.adapters.register_loader('timestamptz', CustomDateTimeTzLoader)

    sqlite3.register_adapter(datetime.date, adapt_date_iso)
    sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)
    sqlite3.register_adapter(Date, adapt_date_iso)
    sqlite3.register_adapter(DateTime, adapt_datetime_iso)

    sqlite3.register_converter('date', convert_date)
    sqlite3.register_converter('datetime', convert_datetime)
