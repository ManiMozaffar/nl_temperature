from typing import Type

from sqlalchemy import URL
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from .models import Base


class Database:
    def __init__(
        self,
        model_base: Type[DeclarativeBase],
        database_url: str | URL,
    ) -> None:
        self.model_base = model_base
        self.database_url = database_url
        self.engine = create_async_engine(
            database_url,
        )
        self.sessionmaker = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
        )

    async def drop_tables(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(self.model_base.metadata.drop_all)

    async def create_tables(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(self.model_base.metadata.create_all)

    def begin(self):
        return self.sessionmaker.begin()


DB = Database(
    Base, "sqlite+aiosqlite:///:memory:"
)


def get_db():
    return DB
