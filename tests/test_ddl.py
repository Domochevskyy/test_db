import pytest

from common.tables import BetterPersons


@pytest.fixture(scope='class')
def better_persons_table(db_client, table_manager):
    q = """
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
    table = BetterPersons(db_client)
    table_manager.create_table(table.table_name, q)
    yield table
    q = f"""DROP TABLE {table.table_name};"""
    table_manager.delete_table(table.table_name, q)


@pytest.fixture
def clear_table_better_persons(db_client):
    yield
    db_client.connection.execute("""TRUNCATE TABLE better_persons;""")


@pytest.fixture
def rename_back_table_name(better_persons_table):
    yield
    name = 'better_persons'
    better_persons_table.rename(name)


@pytest.fixture
def rename_back_column_hobby(better_persons_table):
    yield
    old_column_name = 'bobby'
    new_column_name = 'hobby'
    better_persons_table.rename_column(old_column_name, new_column_name)


@pytest.fixture
def delete_column(better_persons_table):
    yield
    name = 'new_column'
    better_persons_table.delete_column(name)


@pytest.fixture
def add_back_column(better_persons_table):
    column_name = 'occupation'
    column_type = 'varchar(256)'
    better_persons_table.add_column(column_name, column_type)


@pytest.fixture
def create_back_better_persons(table_manager):
    yield
    q = """
    CREATE TABLE IF NOT EXISTS better_persons (
        person_id integer PRIMARY KEY,
        first_name varchar(128) NOT NULL,
        family_name varchar(128),
        birthday date NOT NULL,
        birthplace varchar(256),
        occupation varchar(256),
        hobby varchar(512)
    );
    """
    table_manager.create_table('better_persons', q)


@pytest.mark.usefixtures('better_persons_table')
class TestDDL:
    def test_rename_table(self, better_persons_table, rename_back_table_name):
        """
        setup:
        1. Connect to test_db
        2. Create table better_persons

        test:
        1. Rename table better_persons to armageddon
        2. Get table name

        result: table name is armageddon

        teardown:
        1. Rename armageddon to better_persons
        1. Delete better_persons
        3. Disconnect from test_db
        """
        new_name = 'armageddon'
        better_persons_table.rename(new_name)
        table_name = better_persons_table.get_table_name()
        assert table_name, 'Cannot get table name!'
        assert new_name in table_name, 'Rename table failed!'

    def test_rename_table_empty(self, better_persons_table):
        """
        setup:
        1. Connect to database test_db
        2. Create better_persons

        test:
        1. Rename table better_persons to empty string

        result: table was not renamed

        teardown:
        1. Delete better_persons
        1. Disconnect from database test_db
        """
        new_name = ''
        better_persons_table.rename(new_name)
        result = better_persons_table.get_table_name()
        assert new_name not in result, 'Rename table failed!'

    def test_rename_column(self, better_persons_table, rename_back_column_hobby):
        """
        setup:
        1. Connect to  test_db
        2. Create better_persons

        test:
        1. Rename column `hobby` to `bobby`

        result: column successfully renamed

        teardown:
        1. Rename column `bobby` to `hobby
        2. Delete better_persons
        3. Disconnect from test_db
        """
        old_column_name = 'hobby'
        new_column_name = 'bobby'
        better_persons_table.rename_column(old_column_name, new_column_name)
        assert better_persons_table.column_exists(new_column_name), 'Rename column failed!'

    def test_rename_non_existent_column(self, better_persons_table):
        """
        setup:
        1. Connect to test_db
        2. Create better_persons

        test:
        1. Rename column non-existent column to `bobby`

        result: rename column failed

        teardown:
        1. Delete better_persons
        2. Disconnect from test_db
        """
        old_column_name = 'not_hobby'
        new_column_name = 'bobby'
        better_persons_table.rename_column(old_column_name, new_column_name)
        assert not better_persons_table.column_exists(new_column_name), 'Cannot rename column!'

    def test_add_column(self, better_persons_table, delete_column):
        """
        setup:
        1. Connect to  test_db
        2. Create better_persons

        test:
        1. Add `new_column` with type `text` into better_persons
        2. Select column with name `new_column`

        result: better_persons has new column `new_column`

        teardown:
        1. Delete `new_column` from better_persons
        2. Delete better_persons
        3. Disconnect from test_db
        """
        column_name = 'new_column'
        column_type = 'text'
        better_persons_table.add_column(column_name, column_type)
        result = better_persons_table.get_column(column_name)
        assert result, f'Column {column_name} with type {column_type} is not exist!'
        assert result['column_name'] == column_name, 'Wrong column name!'
        assert result['data_type'] == column_type, 'Wrong column type!'

    def test_add_wrong_type_column(self, better_persons_table):
        """
        setup:
        1. Connect to  test_db
        2. Create better_persons

        test:
        1. Add `new_column` with type `wrong column type` into better_persons
        2. Select column with name `new_column`

        result: column was not added

        teardown:
        1. Delete better_persons
        2. Disconnect from test_db
        """
        column_name = 'new_column'
        column_type = 'wrong column type!'
        better_persons_table.add_column(column_name, column_type)
        result = better_persons_table.get_column(column_name)
        assert not result, f'Create column {column_name} with invalid type {column_type}!'

    def test_delete_column(self, better_persons_table):
        """
        setup:
        1. Connect to  test_db
        2. Create better_persons

        test:
        1. Delete column `occupation`
        2. Select column with name `occupation` from better_persons

        result: cannot get column with name `occupation`

        teardown:
        1. Add column `occupation` with type `varchar(256)`
        2. Delete better_persons
        3. Disconnect from test_db
        """
        column_name = 'occupation'
        better_persons_table.delete_column(column_name)
        result = better_persons_table.get_column(column_name)
        assert not result, f'Delete {column_name} from better_persons failed!'

    def test_delete_non_existent_column(self, better_persons_table):
        """
        setup:
        1. Connect to test_db
        2. Create better_persons

        test:
        1. Delete column `non_existent_column`
        2. Select column with name `non_existent_column` from better_persons

        result: cannot get column with name `occupation`

        teardown:
        1. Delete better_persons
        2. Disconnect from test_db
        """
        column_name = 'non_existent_column'
        better_persons_table.delete_column(column_name)
        assert not better_persons_table.column_exists(column_name)

    def test_delete_table(self, better_persons_table, create_back_better_persons):
        """
        setup:
        1. Connect to test_db
        2. Create better_persons

        test:
        1. Delete table better_persons
        2. Select table better_persons by name

        result: table deleted

        teardown:
        1. Create better_persons
        2. Delete better_persons
        3. Disconnect from test_db
        """
        better_persons_table.delete()
        assert not better_persons_table.is_table_alive()
