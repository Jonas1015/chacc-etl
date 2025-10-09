import pymysql
import logging
from contextlib import contextmanager
from config import SOURCE_DB_CONFIG, TARGET_DB_CONFIG, DB_POOL_SIZE, DB_CONNECT_TIMEOUT

logger = logging.getLogger(__name__)

class DatabaseConnectionPool:
    """
    Simple database connection pool for MySQL.
    """
    def __init__(self, db_config, pool_size=DB_POOL_SIZE):
        self.db_config = db_config
        self.pool_size = pool_size
        self.connections = []

    def get_connection(self):
        if self.connections:
            return self.connections.pop()
        try:
            conn = pymysql.connect(**self.db_config)
            logger.debug("Created new database connection")
            return conn
        except Exception as e:
            logger.error(f"Failed to create database connection: {e}")
            raise

    def return_connection(self, conn):
        if len(self.connections) < self.pool_size:
            self.connections.append(conn)
        else:
            conn.close()
            logger.debug("Closed excess database connection")

_source_connection_pool = DatabaseConnectionPool(SOURCE_DB_CONFIG)
_target_connection_pool = DatabaseConnectionPool(TARGET_DB_CONFIG)

_mysql_config = TARGET_DB_CONFIG.copy()
_mysql_config['database'] = 'mysql'
_mysql_connection_pool = DatabaseConnectionPool(_mysql_config)

@contextmanager
def get_source_db_connection():
    """
    Context manager for source database connections.
    """
    conn = _source_connection_pool.get_connection()
    try:
        yield conn
    finally:
        _source_connection_pool.return_connection(conn)

@contextmanager
def get_target_db_connection():
    """
    Context manager for target database connections.
    """
    conn = _target_connection_pool.get_connection()
    try:
        yield conn
    finally:
        _target_connection_pool.return_connection(conn)

@contextmanager
def get_mysql_db_connection():
    """
    Context manager for MySQL system database connections (used for creating databases).
    """
    conn = _mysql_connection_pool.get_connection()
    try:
        yield conn
    finally:
        _mysql_connection_pool.return_connection(conn)


def execute_query(conn, query, params=None, fetch=False):
    """
    Execute a database query.
    """
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            if fetch:
                return cursor.fetchall()
            conn.commit()
            return cursor.rowcount
    except Exception as e:
        logger.error(f"Query execution failed: {query} - {e}")
        conn.rollback()
        raise

def create_table_if_not_exists(conn, table_name, schema):
    """
    Create a table if it doesn't exist.
    """
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})"
    execute_query(conn, query)
    logger.info(f"Ensured table {table_name} exists")

def truncate_table(conn, table_name):
    """
    Truncate a table.
    """
    query = f"TRUNCATE TABLE {table_name}"
    execute_query(conn, query)
    logger.info(f"Truncated table {table_name}")

def insert_data(conn, table_name, data, columns=None):
    """
    Insert data into a table.
    """
    if not data:
        return 0

    if columns:
        cols = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    else:
        placeholders = ', '.join(['%s'] * len(data[0]))
        query = f"INSERT INTO {table_name} VALUES ({placeholders})"

    with conn.cursor() as cursor:
        cursor.executemany(query, data)
        conn.commit()
        logger.info(f"Inserted {cursor.rowcount} rows into {table_name}")
        return cursor.rowcount