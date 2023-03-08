import datetime
from datetime import date

import pytest

from common.models import Person, PersonField
from common.tables import Persons


@pytest.fixture(scope='class')
def persons_table(db_client, table_manager):
    q = """
    CREATE TABLE persons (
        person_id integer PRIMARY KEY,
        first_name varchar(128) NOT NULL,
        birthday date NOT NULL
    );
    """
    table = Persons(db_client)
    table_manager.create_table(table.table_name, q)
    yield table
    q = f"""DROP TABLE {table.table_name};"""
    table_manager.delete_table(table.table_name, q)


@pytest.fixture
def clear_table_persons(persons_table):
    yield
    q = """TRUNCATE TABLE persons;"""
    persons_table.db_client.connection.execute(q)


@pytest.mark.usefixtures('persons_table', 'clear_table_persons')
class TestPersonDML:

    def test_select_person(self, persons_table):
        """
        setup:
        1. Connect to test_db
        2. Create table persons

        test:
        1. Insert Person in persons
        2. Select Person from persons

        result: select response has Person with correct data

        teardown:
        1. Truncate persons
        2. Delete persons
        3. Disconnect from test_db
        """
        person = Person(1, 'Shao Kahn', datetime.date(1998, 11, 5))
        persons_table.insert(person)
        selected_person = persons_table.select(person, by=PersonField.person_id)
        assert person == selected_person, f'Select failed on: {person.compare(selected_person)}'

    def test_select_non_existent_person(self, persons_table):
        """
        setup:
        1. Connect to test_db
        2. Create table persons

        test:
        1. Select Person from persons

        result: select result is None

        teardown:
        1. Truncate persons
        2. Delete persons
        3. Disconnect from test_db
        """
        person_not_in_table = Person(1, 'Sub-Zero', date(2007, 12, 4))
        selected_person = persons_table.select(person_not_in_table, by=PersonField.person_id)
        assert not selected_person, 'Select non-exists person!'

    def test_insert_person(self, persons_table):
        """
        setup:
        1. Connect to test_db
        2. Create table persons

        test:
        1. Insert Person into persons with RETURNING *

        result: Person in table with correct data

        teardown:
        1. Truncate persons
        2. Delete persons
        3. Disconnect from test_db
        """
        person_to_be_inserted = Person(1, 'Smoke', date(1000, 5, 2))
        q_result = persons_table.insert(person_to_be_inserted)
        assert person_to_be_inserted == q_result, f'Insert failed on {person_to_be_inserted.compare(q_result)}!'

    def test_insert_not_uniq_person(self, persons_table):
        """
        setup:
        1. Connect to database test_db
        2. Create table persons

        test:
        1. Insert Person into persons
        2. Insert the same Person into persons

        result: Person was not inserted

        teardown:
        1. Truncate persons
        2. Delete persons
        3. Disconnect from database test_db
        """
        person = Person(1, 'Liu Kang', date(1000, 5, 2))
        persons_table.insert(person)

        result = persons_table.insert(person)
        assert not result, 'Insert into persons duplicate Persons!'

    def test_update_person(self, persons_table):
        """
        setup:
        1. Connect to database test_db
        2. Create table persons

        test:
        1. Insert Person into persons
        2. Update Person by id

        result: Person has a new person_id and a new first_name

        teardown:
        1. Truncate persons
        2. Delete persons
        3. Disconnect from test_db
        """
        person = Person(1, 'Shang Tsung', datetime.date(1000, 3, 8))
        new_id = 2
        new_name = 'Tsung Shang'
        persons_table.insert(person)
        updated_person = persons_table.update(
            person.person_id,
            [PersonField.person_id, PersonField.first_name],
            [new_id, new_name],
        )
        assert updated_person, 'Failed to update Person!'
        assert updated_person.person_id == new_id, 'Update person_id failed.'
        assert updated_person.first_name == new_name, 'Update first_name failed.'

    def test_update_non_existent_person(self, persons_table):
        """
        setup:
        1. Connect to test_db
        2. Create table persons

        test:
        1. update non-existent Person by id

        result: Person was not updated

        teardown:
        1. Truncate persons
        2. Delete persons
        3. Disconnect from test_db
        """
        new_id = 123
        new_first_name = 'Kenshi'
        updated_person = persons_table.update(
            1,
            [PersonField.person_id, PersonField.first_name],
            [new_id, new_first_name],
        )
        assert not updated_person, 'Update non-existent Person!'

    def test_delete_person(self, persons_table):
        """
        setup:
        1. Connect to test_db
        2. Create persons

        test:
        1. Insert Person into persons
        2. Delete Person from persons by id

        result: Person deleted

        teardown:
        1. Truncate persons
        2. Delete persons
        3. Disconnect from test_db
        """
        person = Person(1, 'Kung Lao', datetime.date(999, 12, 12))
        persons_table.insert(person)

        deleted_person = persons_table.delete(person, by=PersonField.person_id)
        assert deleted_person, 'Delete Person failed!'
        assert person == deleted_person, f'Deleted Person had wrong data: {person.compare(deleted_person)}'

    def test_delete_non_existent_person(self, persons_table):
        """
        setup:
        1. Connect to test_db
        2. Create persons

        test:
        1. Delete non-existent Person from persons by id

        result: Person delete failed

        teardown:
        1. Truncate persons
        2. Delete persons
        3. Disconnect from test_db
        """
        person = Person(666, 'Night Wolf', datetime.date(2033, 10, 5))
        deleted_person = persons_table.delete(person, by=PersonField.person_id)
        assert not deleted_person, 'We cannot delete non-existent Person!'
