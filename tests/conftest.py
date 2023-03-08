import os
import sys

import pytest
from psycopg import OperationalError

from common.db_client import DataBaseClient
from common.logger import get_logger
from common.tables import TableManager

# TODO: is row sql fixtures should be here?
# TODO: add beautiful parameter throwing via class or the other structure
host = os.getenv('POSTGRES_CONTAINER_NAME', 'localhost')
port = '5432'
db_name = os.getenv('POSTGRES_DB', 'test_db')
username = os.getenv('POSTGRES_USER', 'test_user')
password = os.getenv('POSTGRES_PASSWORD', 'test_password')
connection_timeout = 5

connect_info = f'host=localhost ' \
               f'port=5432 ' \
               f'dbname={db_name} ' \
               f'user={username} ' \
               f'connect_timeout=5 ' \
               f'password={password}'

logger = get_logger('root conftest.py')


@pytest.fixture(scope='session')
def db_client() -> DataBaseClient:
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
