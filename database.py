import logging
import os
from contextlib import contextmanager

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

logger = logging.getLogger("grs-db")

load_dotenv()


def get_database_url():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")
    return database_url

class DatabasePool:
    _pool = None

    @classmethod
    def initialize(cls):
        if cls._pool is None:
            try:
                cls._pool = ThreadedConnectionPool(
                    1, 20,
                    dsn=get_database_url(),
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
        except Exception:
            conn.rollback()
            raise
        finally:
            cls._pool.putconn(conn)

def get_db_connection():
    return DatabasePool.get_connection()
