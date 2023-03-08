import psycopg
from psycopg import Connection, OperationalError

from common.logger import get_logger

logger = get_logger('db_client')


class DataBaseClient:

    def __init__(self, connection_info: str):
        self.connection_info = connection_info
        self.connection = self.connect(connection_info)

    def connect(self, connection_info: str) -> Connection:
        """Connect to a database server and return a new `Connection` instance."""
        # TODO: add check for connection errors for outer operations from other classes
        # TODO: add reconnection logic
        try:
            connect = psycopg.connect(conninfo=self.connection_info)
            logger.info(f'Connect to {self.connection_info}.')
            return connect
        except OperationalError:
            logger.error(f'Cannot connect to {connection_info}.')
            raise

    def close(self) -> None:
        """Close database connection."""
        self.connection.close()
        logger.info(f'Close connection {self.connection_info}.')
