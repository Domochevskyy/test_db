import functools
import typing
from dataclasses import astuple

from psycopg import Cursor, sql
from psycopg.abc import Params
from psycopg.errors import (InFailedSqlTransaction, SyntaxError,
                            UndefinedColumn, UndefinedObject, UniqueViolation)
from psycopg.rows import class_row, dict_row

from common.db_client import DataBaseClient
from common.logger import get_logger
from common.models import Person, PersonField

logger = get_logger('tables')


def on_transaction_failed(method: typing.Callable):
    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except InFailedSqlTransaction as error:
            logger.warning(f'Transaction failed for {method}.')
            logger.warning(error)
            # TODO: add full check for presence db_client in args
            args[0].db_client.connection.rollback()

    return wrapper


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

    def execute(
            self,
            cursor: Cursor,
            row_sql: str,
            params: typing.Optional[Params] = None,
            fetch_result: bool = False,
    ) -> typing.Optional[typing.Any]:
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
    def select(self, person: Person, *, by: PersonField) -> typing.Optional[Person]:
        q = """SELECT * FROM {} WHERE {} = {value};"""
        query = sql.SQL(q).format(
            sql.Identifier(self.table_name),
            sql.Identifier(by.name),
            value=getattr(person, by.name),
        )

        with self.db_client.connection.cursor(row_factory=class_row(person.__class__)) as cur:
            result = cur.execute(query).fetchone()
        logger.info(f'Select {person} by {by.name}.')
        return result

    @on_transaction_failed
    def insert(self, person: Person) -> typing.Optional[Person]:
        q = """INSERT INTO {} VALUES (%s, %s, %s) RETURNING *;"""
        params = astuple(person)
        query = sql.SQL(q).format(
            sql.Identifier(self.table_name),
        )
        try:
            with self.db_client.connection.cursor(row_factory=class_row(person.__class__)) as cur:
                res = cur.execute(query, params).fetchone()
                logger.info(f'INSERT {person} INTO {self.table_name}.')
                return res
        except UniqueViolation:
            logger.warning(f'Failed to insert {person}!')
            self.db_client.connection.rollback()

    @on_transaction_failed
    def update(
            self,
            person_id: int,
            person_fields: list[PersonField],
            person_values: list[typing.Any],
    ) -> typing.Optional[Person | list[Person]]:
        q = """UPDATE {} SET ({}) = ({}) WHERE person_id = {value} RETURNING *;"""

        person_fields_as_str = [field.name for field in person_fields]
        query = sql.SQL(q).format(
            sql.Identifier(self.table_name),
            sql.SQL(', ').join(map(sql.Identifier, person_fields_as_str)),
            sql.SQL(', ').join(sql.Placeholder() * len(person_values)),
            value=person_id,
        )
        with self.db_client.connection.cursor(row_factory=class_row(Person)) as cur:
            result = cur.execute(query, params=person_values).fetchone()
        logger.info(f'Update person fields: {person_fields_as_str} by id: {person_id}.')
        return result

    @on_transaction_failed
    def delete(self, person: Person, *, by: PersonField) -> typing.Optional[Person | list[Person]]:
        q = """DELETE from {} WHERE {} = {value} RETURNING *;"""
        query = sql.SQL(q).format(
            sql.Identifier(self.table_name),
            sql.Identifier(by.name),
            value=getattr(person, by.name),
        )

        with self.db_client.connection.cursor(row_factory=class_row(person.__class__)) as cur:
            result = cur.execute(query).fetchall()
            logger.info(f'Delete {person} from {self.table_name} by {by.name}.')
        if len(result) == 1:
            return result[0]
        return result


class BetterPersons(Table):
    def __init__(self, db_client: DataBaseClient):
        super().__init__(db_client)
        self.table_name = 'better_persons'

    @on_transaction_failed
    def get_table_name(self) -> typing.Optional[str]:
        q = """SELECT table_name from information_schema.tables WHERE table_name = %s;"""
        params = (self.table_name,)

        with self.db_client.connection.cursor() as cur:
            return cur.execute(q, params).fetchone()

    @on_transaction_failed
    def rename(self, name: str) -> None:
        q = """ALTER TABLE {} RENAME TO {};"""
        query = sql.SQL(q).format(
            sql.Identifier(self.table_name),
            sql.Identifier(name),
        )
        with self.db_client.connection.cursor() as cur:
            try:
                cur.execute(query)
                logger.info(f'Rename "{self.table_name}" to "{name}".')
                self.table_name = name
            except SyntaxError as syn_error:
                logger.warning(syn_error)
                self.db_client.connection.rollback()

    @on_transaction_failed
    def rename_column(self, column_name: str, new_column_name: str) -> None:
        q = """ALTER TABLE {} RENAME COLUMN {} TO {};"""
        query = sql.SQL(q).format(
            sql.Identifier(self.table_name),
            sql.Identifier(column_name),
            sql.Identifier(new_column_name),
        )
        with self.db_client.connection.cursor() as cur:
            try:
                cur.execute(query)
                logger.info(f'Rename column {column_name} to {new_column_name}.')
            except UndefinedColumn:
                logger.warning(f'Rename column {column_name} to {new_column_name} failed!')
                self.db_client.connection.rollback()

    @on_transaction_failed
    def column_exists(self, column_name: str) -> typing.Optional[bool]:
        q = """SELECT column_name FROM information_schema.columns WHERE table_name = %s;"""
        params = (self.table_name,)

        with self.db_client.connection.cursor() as cur:
            result = cur.execute(q, params).fetchall()
            if result:
                return (column_name,) in result
            return False

    @on_transaction_failed
    def add_column(self, name: str, column_type: str) -> None:
        q = """ALTER TABLE {} ADD COLUMN {} {};"""
        query = sql.SQL(q).format(
            sql.Identifier(self.table_name),
            sql.Identifier(name),
            sql.Identifier(column_type),
        )
        with self.db_client.connection.cursor() as cur:
            try:
                cur.execute(query)
                logger.info(f'Add column:{name}, type:{column_type} into {self.table_name}')
            except UndefinedObject as und_obj_error:
                logger.warning(*und_obj_error.args)

    @on_transaction_failed
    def get_column(self, name: str) -> typing.Optional[dict]:
        q = """SELECT * FROM information_schema.columns WHERE column_name = %s;"""
        params = (name,)
        with self.db_client.connection.cursor(row_factory=dict_row) as cur:
            return cur.execute(q, params).fetchone()

    @on_transaction_failed
    def delete_column(self, name: str) -> None:
        q = """ALTER TABLE {} DROP COLUMN {};"""
        query = sql.SQL(q).format(
            sql.Identifier(self.table_name),
            sql.Identifier(name),
        )
        with self.db_client.connection.cursor() as cur:
            try:
                cur.execute(query)
                logger.info(f'Delete column: {name} from {self.table_name}')
            except UndefinedColumn:
                logger.warning(f'Cannot delete column: {name} from {self.table_name}.')
                self.db_client.connection.rollback()

    @on_transaction_failed
    def delete(self) -> None:
        q = 'DROP TABLE {};'
        query = sql.SQL(q).format(
            sql.Identifier(self.table_name),
        )
        with self.db_client.connection.cursor() as cur:
            cur.execute(query)
            logger.info(f'Delete table: {self.table_name}.')

    @on_transaction_failed
    def is_table_alive(self) -> bool:
        q = 'SELECT * FROM information_schema.tables WHERE table_name = {};'
        query = sql.SQL(q).format(
            sql.Identifier(self.table_name),
        )
        with self.db_client.connection.cursor() as cur:
            try:
                cur.execute(query)
                return True
            except UndefinedColumn:
                self.db_client.connection.rollback()
                return False
