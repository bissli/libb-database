import logging
import os
import sys
import time
from contextlib import contextmanager

import docker
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.append('..')
import config

logger = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def psql_docker(request):
    client = docker.from_env()
    container = client.containers.run(
        image='postgres:12',
        auto_remove=True,
        environment={
            'POSTGRES_DB': config.sql.pg.test.write.db,
            'POSTGRES_USER': config.sql.pg.test.write.user,
            'POSTGRES_PASSWORD': config.sql.pg.test.write.passwd,
            'TZ': 'US/Eastern',
            'PGTZ': 'US/Eastern'},
        name='test_postgres',
        ports={'5432/tcp': (config.sql.pg.test.write.host,
                            config.sql.pg.test.write.port)},
        detach=True,
        remove=True,
    )
    time.sleep(5)
    request.addfinalizer(container.stop)


@pytest.fixture(scope='session')
def schema(request, logger):
    """Use only for getting schema data"""
    cn = db.connect('Tenor_test')

    def fin():
        logger.info('Tearing down connection to Tenor db')
        cn.close()

    request.addfinalizer(fin)
    return cn


def terminate_postgres_connections(db_url):
    sql = """
select
    pg_terminate_backend(pg_stat_activity.pid)
from
    pg_stat_activity
where
    pg_stat_activity.datname = current_database()
    and pid <> pg_backend_pid()
    """
    db = dataset.connect(db_url)
    db.query(sql)


@contextmanager
def conn(db_url):
    terminate_postgres_connections(db_url)
    db = dataset.connect(db_url)
    schema.create_tables(db)
    try:
        yield db
    finally:
        drop_tables(db)
        db.close()
