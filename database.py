import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import os
import logging
from contextlib import contextmanager

logger = logging.getLogger("grs-db")

DATABASE_URL = os.getenv("DATABASE_URL")

class DatabasePool:
    _pool = None

    @classmethod
    def initialize(cls):
        if cls._pool is None:
            try:
                cls._pool = psycopg2.pool.SimpleConnectionPool(
                    1, 20,
                    dsn=DATABASE_URL,
                    cursor_factory=RealDictCursor
                )
                logger.info("Database connection pool initialized.")
            except Exception as e:
                logger.error(f"Error initializing connection pool: {e}")
                raise

    @classmethod
    @contextmanager
    def get_connection(cls):
        if cls._pool is None:
            cls.initialize()
        
        conn = cls._pool.getconn()
        try:
            yield conn
        finally:
            cls._pool.putconn(conn)

def get_db_connection():
    return DatabasePool.get_connection()
