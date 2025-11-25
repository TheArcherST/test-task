from collections.abc import AsyncGenerator, Iterable
from pathlib import Path

from dishka import Provider, Scope, provide
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    create_async_engine,
)
from sqlalchemy.orm import Session


class EnsureDatabaseFKsSentinel:
    pass


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
        return create_async_engine(
            config.get_sqlalchemy_url("aiosqlite"),
        )

    @provide(scope=Scope.SESSION)
    async def get_database_session(
            self,
            engine: AsyncEngine,
            database_fks_sentinel: EnsureDatabaseFKsSentinel,
    ) -> AsyncGenerator[AsyncSession, None]:
        assert database_fks_sentinel
        async with AsyncSession(
            engine,
            expire_on_commit=False,
        ) as session:
            yield session

    @provide(scope=Scope.APP)
    async def ensure_database_fks(
            self,
    ) -> EnsureDatabaseFKsSentinel:
            from . import sqlalchemy_events
            assert sqlalchemy_events
            return EnsureDatabaseFKsSentinel()


class ProviderTestDatabase(Provider):
    def get_database_engine(
            self,
            config: ConfigSQLite,
    ) -> Engine:
        return create_engine(
            config.get_sqlalchemy_url("aiosqlite"),
        )

    def get_database_session(
            self,
            engine: Engine,
    ) -> Iterable[Session]:
        with Session(engine) as session:
            yield session
