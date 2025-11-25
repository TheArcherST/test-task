from sqlalchemy.dialects.sqlite.aiosqlite import (
    AsyncAdapt_aiosqlite_connection)
from sqlalchemy.engine import Engine
from sqlalchemy import event


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection: AsyncAdapt_aiosqlite_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()
