import sys

import pytest
from _pytest.config.argparsing import Parser
from psycopg import OperationalError

from common.db_client import DataBaseClient
from common.logger import get_logger
from common.tables import TableManager

logger = get_logger('root conftest.py')


def pytest_addoption(parser: Parser):
    parser.addoption(
        '--ip',
        type=str,
        action='store',
        help='Database host ip.',
        required=True,
    )
    parser.addoption(
        '--port',
        type=str,
        action='store',
        help='Database host port.',
        required=True,
    )
    parser.addoption(
        '--database',
        type=str,
        action='store',
        help='Database name.',
        required=True,
    )
    parser.addoption(
        '--username',
        type=str,
        action='store',
        help='User which connected to database.',
        required=True,
    )

    parser.addoption(
        '--password',
        type=str,
        action='store',
        help='User password for auth.',
        required=True,
    )


@pytest.fixture(scope='session')
def db_client(request) -> DataBaseClient:
    host = request.config.getoption('--ip')
    port = request.config.getoption('--port')
    dbname = request.config.getoption('--database')
    user = request.config.getoption('--username')
    password = request.config.getoption('--password')

    connect_info = f'{host=} {port=} {dbname=} {user=} {password=}'
    # TODO: make db clients factory
    try:
        db_client = DataBaseClient(connect_info)
    except OperationalError:
        logger.error('Check the connection to database.')
        sys.exit(2)
    else:
        yield db_client
        db_client.close()


@pytest.fixture(scope='class')
def table_manager(db_client: DataBaseClient) -> TableManager:
    return TableManager(db_client)


@pytest.fixture(scope='class')
def create_table_persons(
        table_manager: TableManager,
        create_table_persons_row_sql: str,
        delete_table_persons_row_sql: str,
) -> None:
    # TODO: make dependency injection for creating different tables
    table_name = 'persons'
    table_manager.create_table(table_name, create_table_persons_row_sql)
    yield
    table_manager.delete_table(table_name, delete_table_persons_row_sql)


@pytest.fixture(scope='class')
def create_table_better_persons(
        table_manager,
        create_table_better_persons_row_sql,
        delete_table_better_persons_row_sql,
) -> None:
    # TODO: Or create simple factory method (create_table_persons)
    table_name = 'better_persons'
    table_manager.create_table(table_name, create_table_better_persons_row_sql)
    yield
    table_manager.delete_table(table_name, delete_table_better_persons_row_sql)


@pytest.fixture(scope='class')
def create_table_persons_row_sql() -> str:
    return """
    CREATE TABLE persons (
        person_id integer PRIMARY KEY,
        first_name varchar(128) NOT NULL,
        birthday date NOT NULL
    );
    """


@pytest.fixture(scope='class')
def delete_table_persons_row_sql() -> str:
    return """DROP TABLE persons;"""


@pytest.fixture(scope='class')
def create_table_better_persons_row_sql() -> str:
    return """
    CREATE TABLE better_persons (
        person_id integer PRIMARY KEY,
        first_name varchar(128) NOT NULL,
        family_name varchar(128),
        birthday date NOT NULL,
        birthplace varchar(256),
        occupation varchar(256),
        hobby varchar(512)
    );
    """


@pytest.fixture(scope='class')
def delete_table_better_persons_row_sql() -> str:
    return """DROP TABLE better_persons;"""
