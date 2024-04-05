import logging
import os
import site

import pytest
import tests.fixtures

from libb import get_package_paths_in_module

logger = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
site.addsitedir(HERE)


def pytest_addoption(parser):
    parser.addoption(
        '--log',
        action='store',
        default='INFO',
        help='set logging level',
    )


@pytest.fixture(scope='session')
def logger(request):

    loglevel = request.config.getoption('--log')

    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)

    logging.basicConfig()
    logger = logging.getLogger(__name__)
    logger.setLevel(numeric_level)
    return logger


pytest_plugins = [*get_package_paths_in_module(tests.fixtures)]
