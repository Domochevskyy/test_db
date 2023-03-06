import pytest
from psycopg.rows import dict_row

# TODO: move to common module
TABLE_NAMES = ['persons', 'better_persons']
CREATE_TABLE_QUERIES = [
    """
    CREATE TABLE persons (
        person_id integer PRIMARY KEY,
        name varchar(128) NOT NULL,
        birthday date NOT NULL
    );
    """,
    """
    CREATE TABLE better_persons (
        person_id integer PRIMARY KEY,
        first_name varchar(128) NOT NULL,
        family_name varchar(128),
        birthday date NOT NULL,
        birthplace varchar(256),
        occupation varchar(256),
        hobby varchar(512)
    );
    """,
]


class TestTableIsCreated:

    @pytest.fixture
    def delete_table(self, request, table_manager) -> None:
        yield
        table_name = request.node.funcargs['table_name']
        q = f"""DROP TABLE {table_name};"""
        table_manager.delete_table(table_name, q)

    @pytest.mark.parametrize(argnames='table_name, q', argvalues=zip(TABLE_NAMES, CREATE_TABLE_QUERIES))
    def test_table_is_created(self, table_name, q, table_manager, delete_table):
        """
        setup:
        1. Connect to database test_db

        test:
        1. Create table <table_name>
        2. Send query check

        result: query check result is not empty and created database name is <table_name>

        teardown:
        1. Delete table <table_name>
        2. Disconnect from database test_db
        """

        check_table_row_sql = f"""
        SELECT * FROM information_schema.tables
        WHERE table_name = '{table_name}';
        """

        cursor = table_manager.db_client.connection.cursor(row_factory=dict_row)
        table_manager.create_table(table_name, q)
        result = table_manager.execute(cursor, check_table_row_sql, fetch_result=True)

        assert result, f'Cannot create {table_name} table.'
        assert result['table_name'] == table_name, 'Table with wrong name was created.'
