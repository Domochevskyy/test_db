import datetime
from datetime import date

import pytest
from psycopg.errors import UniqueViolation

from common.logger import get_logger
from common.tables import Persons
from common.models import Person, PersonField

logger = get_logger('test_dml')


@pytest.fixture(scope='class')
def persons_table(db_client):
    return Persons(db_client)


@pytest.fixture
def inserted_person(persons_table) -> Person:
    p = Person(1, 'Scorpion', date(2007, 12, 4))
    persons_table.insert(p)
    yield p
    persons_table.delete(p, by=PersonField.person_id)


@pytest.fixture
def clear_table_persons(db_client):
    yield
    db_client.connection.execute("""TRUNCATE persons""")
    logger.info('Clear persons table.')


@pytest.mark.usefixtures('create_table_persons')
class TestPersonDML:

    def test_select_person(self, inserted_person, persons_table):
        """
        setup:
        1. Connect to database test_db
        2. Create table persons
        3. Insert Person in persons

        test:
        1. Select Person from persons

        result: select response has Person with correct data

        teardown:
        1. Delete Person from persons
        2. Delete table persons
        3. Disconnect from database test_db
        """
        selected_person = persons_table.select(inserted_person, by=PersonField.person_id)
        assert selected_person == inserted_person, f'Select failed on: {selected_person.compare(inserted_person)}'

    def test_select_non_existent_person(self, persons_table):
        """
        setup:
        1. Connect to database test_db
        2. Create table People

        test:
        1. Select Person() from People

        result:
        Person() select failed

        teardown:
        1. Delete table People
        2. Disconnect from database test_db
        """
        person_not_in_table = Person(1, 'Sub-Zero', date(2007, 12, 4))
        selected_person = persons_table.select(person_not_in_table, by=PersonField.person_id)
        assert not selected_person, 'Select non-exists person!'

    def test_insert_person(self, persons_table, clear_table_persons):
        """
        setup:
        1. Connect to database test_db
        2. Create table persons

        test:
        1. Insert Person into persons with RETURNING *

        result: Person in table with correct data

        teardown:
        1. Delete Person from persons
        2. Delete table persons
        3. Disconnect from database test_db
        """
        person_to_be_inserted = Person(1, 'Smoke', date(1000, 5, 2))

        q_result = persons_table.insert(person_to_be_inserted)
        assert person_to_be_inserted == q_result, f'Insert failed on {person_to_be_inserted.compare(q_result)}!'

    def test_insert_not_uniq_person(self, inserted_person, persons_table):
        """
        setup:
        1. Connect to database test_db
        2. Create table persons
        3. Insert Person into persons

        test:
        1. Insert same Person into persons

        result: raise unique violation error

        teardown:
        1. Truncate persons
        2. Delete table persons
        3. Disconnect from database test_db
        """

        person_not_to_be_inserted = Person(1, 'Liu Kang', date(1000, 5, 2))
        with pytest.raises(UniqueViolation) as uniq_error:
            persons_table.insert(person_not_to_be_inserted)
        assert uniq_error.type is UniqueViolation, 'Insert into persons duplicate Persons!'

    def test_update_person(self, inserted_person, persons_table, clear_table_persons):
        """
        setup:
        1. Connect to database test_db
        2. Create table persons
        3. Insert Person into persons

        test:
        1. update Person e by id

        result: Person has new data

        teardown:
        1. Clear table
        2. Delete table People
        3. Disconnect from database test_db
        """
        new_id = 2
        new_first_name = 'Shang Tsung'
        updated_person: Person = persons_table.update(
            inserted_person.person_id,
            [PersonField.person_id, PersonField.first_name],
            [new_id, new_first_name],
        )
        assert updated_person.person_id == new_id, 'Update person_id failed.'
        assert updated_person.first_name == new_first_name, 'Update first_name failed.'

    def test_update_non_existent_person(self, persons_table):
        """
        setup:
        1. Connect to database test_db
        2. Create table People

        test:
        1. update non-existent Person() from People by id

        result:
        Person() select failed

        teardown:
        1. Delete table People
        2. Disconnect from database test_db
        """
        new_id = 123
        new_first_name = 'Kenshi'
        updated_person: Person = persons_table.update(
            1,
            [PersonField.person_id, PersonField.first_name],
            [new_id, new_first_name],
        )
        assert not updated_person, 'Update non-exists Person!'

    def test_delete_person(self, inserted_person, persons_table):
        """
        setup:
        1. Connect to database test_db
        2. Create table persons
        3. Insert Person into persons

        test:
        1. Delete Person from persons by id

        result: Person is not in persons

        teardown:
        1. Delete table People
        2. Disconnect from database test_db
        """
        deleted_person: Person = persons_table.delete(inserted_person, by=PersonField.person_id)
        assert deleted_person == inserted_person, 'Delete Person failed.'

    def test_delete_non_existent_person(self, persons_table):
        """
        setup:
        1. Connect to database test_db
        2. Create table People

        test:
        1. Delete Person() from People by id

        result:
        Person() delete failed

        teardown:
        1. Delete table People
        2. Disconnect from database test_db
        """

        person = Person(666, 'Night wolf', datetime.date(2033, 10, 5))
        deleted_person = persons_table.delete(person, by=PersonField.person_id)
        assert not deleted_person, 'We cannot delete non-existent Person!'
