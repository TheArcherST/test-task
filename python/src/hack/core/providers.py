from collections.abc import AsyncGenerator, Iterable
from pathlib import Path

from dishka import Provider, Scope, provide
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import event, Engine
from sqlalchemy.dialects.sqlite.aiosqlite import \
    AsyncAdapt_aiosqlite_connection
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    create_async_engine,
)


class ConfigSQLite(BaseModel):
    path: str | None = None
    test_path: str | None = None
    use_test_by_default: bool = False

    def get_sqlalchemy_url(
        self,
        driver: str = "aiosqlite",
        *,
        is_test_database: bool | None = None,
    ) -> str:
        if is_test_database is None:
            is_test_database = self.use_test_by_default

        db_path = self.path
        if is_test_database:
            if self.test_path is None:
                raise ValueError("Test database not specified")
            db_path = self.test_path

        if db_path is None or db_path == ":memory:":
            return f"sqlite+{driver}:///:memory:"

        # ensure directory exists for database file
        db_file = Path(db_path)
        if not db_file.parent.exists():
            db_file.parent.mkdir(parents=True, exist_ok=True)

        return f"sqlite+{driver}:///{db_file}"


class ConfigHack(BaseSettings):
    model_config = SettingsConfigDict(
        env_nested_delimiter="__",
        env_file=".env",
        env_prefix="HACK__",
    )

    sqlite: ConfigSQLite


class ProviderConfig(Provider):
    @provide(scope=Scope.APP)
    def get_config_hack(self) -> ConfigHack:
        return ConfigHack()  # type: ignore

    @provide(scope=Scope.APP)
    def get_config_sqlite(
            self,
            config: ConfigHack,
    ) -> ConfigSQLite:
        return config.sqlite


class ProviderDatabase(Provider):
    @provide(scope=Scope.APP)
    def get_database_engine(
            self,
            config: ConfigSQLite,
    ) -> AsyncEngine:

        engine = create_async_engine(
            config.get_sqlalchemy_url("aiosqlite"),
        )

        # see https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#transactions-with-sqlite-and-the-sqlite3-driver

        @event.listens_for(engine.sync_engine, "connect")
        def do_connect(dbapi_connection, connection_record):
            # disable aiosqlite's emitting of the BEGIN statement entirely.
            dbapi_connection.isolation_level = None

        @event.listens_for(engine.sync_engine, "begin")
        def do_begin(conn):
            # emit our own BEGIN.  aiosqlite still emits COMMIT/ROLLBACK correctly
            conn.exec_driver_sql("BEGIN")

        @event.listens_for(Engine, "connect")
        def set_sqlite_pragma(
                dbapi_connection: AsyncAdapt_aiosqlite_connection,
                connection_record,
        ):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

        return engine

    @provide(scope=Scope.SESSION)
    async def get_database_session(
            self,
            engine: AsyncEngine,
    ) -> AsyncGenerator[AsyncSession, None]:
        async with AsyncSession(
            engine,
            expire_on_commit=False,
        ) as session:
            yield session
