import logging
import os
import sys
import time

import docker
import pytest

import db

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.append('..')
import config

logger = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def psql_docker():
    client = docker.from_env()
    container = client.containers.run(
        image='postgres:12',
        auto_remove=True,
        environment={
            'POSTGRES_DB': 'test_db',
            'POSTGRES_USER': 'postgres',
            'POSTGRES_PASSWORD': 'postgres',
            'TZ': 'US/Eastern',
            'PGTZ': 'US/Eastern'},
        name='test_postgres',
        ports={'5432/tcp': ('127.0.0.1', 5432)},
        detach=True,
        remove=True,
    )
    time.sleep(5)
    yield
    container.stop()


def stage_test_data(cn):
    drop_table_if_exists = """
DROP TABLE IF EXISTS test_table;
"""
    db.execute(cn, drop_table_if_exists)

    create_and_insert_data = """
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    value INTEGER NOT NULL
);

INSERT INTO test_table (name, value) VALUES
('Alice', 10),
('Bob', 20),
('Charlie', 30),
('Ethan', 50),
('Fiona', 70),
('George', 80);

    """
    db.execute(cn, create_and_insert_data)


def terminate_postgres_connections(cn):
    sql = """
select
    pg_terminate_backend(pg_stat_activity.pid)
from
    pg_stat_activity
where
    pg_stat_activity.datname = current_database()
    and pid <> pg_backend_pid()
    """
    db.execute(cn, sql)


@pytest.fixture(scope='session')
def conn():
    cn = db.connect('postgres', config)
    terminate_postgres_connections(cn)
    stage_test_data(cn)
    try:
        yield cn
    finally:
        terminate_postgres_connections(cn)
        cn.close()
