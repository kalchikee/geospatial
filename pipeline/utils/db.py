"""
Database connection utilities.
Provides a context manager for psycopg2 connections and a SQLAlchemy engine.
"""
import contextlib
from typing import Generator

import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, Engine

from config.settings import DATABASE_URL, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS


def get_dsn() -> str:
    return f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASS}"


@contextlib.contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """Yield a psycopg2 connection; commit on success, rollback on error."""
    conn = psycopg2.connect(get_dsn())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextlib.contextmanager
def get_cursor(
    connection: psycopg2.extensions.connection,
    cursor_factory=psycopg2.extras.RealDictCursor,
):
    """Yield a cursor from an existing connection."""
    cur = connection.cursor(cursor_factory=cursor_factory)
    try:
        yield cur
    finally:
        cur.close()


def get_engine() -> Engine:
    """Return a SQLAlchemy engine for geopandas.to_postgis() calls."""
    # Build URL from individual settings (DATABASE_URL may use legacy 'postgres://' scheme)
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)
