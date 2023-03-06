import functools
import typing
from dataclasses import astuple

from typing import Optional, Any

from psycopg import Cursor, sql
from psycopg.abc import Params
from psycopg.errors import InFailedSqlTransaction
from psycopg.rows import class_row

from common.db_client import DataBaseClient
from common.logger import get_logger
from common.models import Person, PersonField

logger = get_logger('tables')


# TODO: resolve DRY problem by writing a decorator
class TableManager:
    def __init__(self, db_client: DataBaseClient):
        self.db_client = db_client

    def create_table(self, table_name: str, row_sql: str) -> None:
        """Create table via db_client in its database."""
        # TODO: fix DRY (create_table and delete_table)
        try:
            with self.db_client.connection.cursor() as cursor:
                cursor.execute(row_sql)
        except BaseException as err:
            self.db_client.connection.rollback()
            logger.error(f'Cannot create table {table_name}.')
            logger.error(err)
        else:
            self.db_client.connection.commit()
            logger.info(f'Create table {table_name}.')

    def delete_table(self, table_name: str, row_sql: str) -> None:
        """Delete table via db_client from its database."""
        # TODO: fix DRY (create_table and delete_table)
        try:
            with self.db_client.connection.cursor() as cursor:
                cursor.execute(row_sql)
        except BaseException as err:
            self.db_client.connection.rollback()
            logger.error(f'Cannot delete table {table_name}.')
            logger.error(err)
        else:
            self.db_client.connection.commit()
            logger.info(f'Delete table {table_name}.')

    def execute(self, cursor: Cursor, row_sql: str, params: Optional[Params] = None, fetch_result: bool = False):
        """Execute a query via cursor to the database and return result."""
        # TODO: add typing for this one
        try:
            with cursor:
                result = cursor.execute(row_sql, params)
                # TODO: fix logging message
                logger.info(f'Execute\t{row_sql}\t with {params=}')
                if fetch_result:
                    # TODO: add flexibility for fetching
                    return result.fetchone()
        except BaseException as err:
            logger.error(err)
            self.db_client.connection.rollback()
            raise


def on_transaction_failed(method: typing.Callable):
    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except InFailedSqlTransaction as error:
            logger.warning(f'Transaction failed for {method}.')
            logger.warning(error)
            if isinstance(args[0], Persons):
                persons_object: Persons = args[0]
                persons_object.db_client.connection.rollback()

    return wrapper


class Table:
    # TODO: is Table abstract class which provides interfaces?
    #       Persons and BetterPersons methods have different signatures.
    # TODO: Should we implement specification?
    def __init__(self, db_client: DataBaseClient):
        self.db_client = db_client


class Persons(Table):
    def __init__(self, db_client: DataBaseClient):
        super().__init__(db_client)
        self.table_name = 'persons'

    @on_transaction_failed
    def select(self, person: Person, *, by: PersonField) -> Optional[Person]:
        logger.info(f'Select {person} by {by.name}.')
        q = """SELECT * FROM {} WHERE {} = {value};"""
        query = sql.SQL(q).format(
            sql.Identifier(self.table_name),
            sql.Identifier(by.name),
            value=getattr(person, by.name),
        )

        with self.db_client.connection.cursor(row_factory=class_row(person.__class__)) as cur:
            result = cur.execute(query).fetchone()
        return result

    @on_transaction_failed
    def insert(self, person: Person) -> Optional[Person]:
        logger.info(f'INSERT {person} INTO {self.table_name}.')
        q = """INSERT INTO {} VALUES (%s, %s, %s) RETURNING *;"""
        params = astuple(person)
        query = sql.SQL(q).format(
            sql.Identifier(self.table_name),
        )
        with self.db_client.connection.cursor(row_factory=class_row(person.__class__)) as cur:
            result = cur.execute(query, params).fetchone()
        return result

    @on_transaction_failed
    def update(
            self,
            person_id: int,
            person_fields: list[PersonField],
            person_values: list[Any],
    ) -> Optional[Person | list[Person]]:
        q = """UPDATE {} SET ({}) = ({}) WHERE person_id = {value} RETURNING *;"""

        person_fields_as_str = [field.name for field in person_fields]
        logger.info(f'Update person fields: {person_fields_as_str} by id: {person_id}.')
        query = sql.SQL(q).format(
            sql.Identifier(self.table_name),
            sql.SQL(', ').join(map(sql.Identifier, person_fields_as_str)),
            sql.SQL(', ').join(sql.Placeholder() * len(person_values)),
            value=person_id,
        )
        with self.db_client.connection.cursor(row_factory=class_row(Person)) as cur:
            result = cur.execute(query, params=person_values).fetchone()
        return result

    @on_transaction_failed
    def delete(self, person: Person, *, by: PersonField) -> Optional[Person | list[Person]]:
        logger.info(f'Delete {person} from {self.table_name} by {by.name}.')
        q = """DELETE from {} WHERE {} = {value} RETURNING *;"""
        query = sql.SQL(q).format(
            sql.Identifier(self.table_name),
            sql.Identifier(by.name),
            value=getattr(person, by.name),
        )

        with self.db_client.connection.cursor(row_factory=class_row(person.__class__)) as cur:
            result = cur.execute(query).fetchall()
        if len(result) == 1:
            return result[0]
        return result
