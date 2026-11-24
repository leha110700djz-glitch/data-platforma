"""
Custom SQLAlchemyAsyncHook — async DB extraction for Airflow.
Demonstrates: ООП, async/await, типизация, контекстные менеджеры.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from airflow.hooks.base import BaseHook
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine


class SQLAlchemyAsyncHook(BaseHook):
    """Async SQLAlchemy hook with connection pooling and typed results."""

    conn_name_attr = "postgres_conn_id"
    default_conn_name = "sqla_default"
    hook_name = "SQLAlchemy Async Hook"

    def __init__(
        self,
        postgres_conn_id: str,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_pre_ping: bool = True,
    ) -> None:
        super().__init__()
        self.postgres_conn_id = postgres_conn_id
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_pre_ping = pool_pre_ping
        self._engine: AsyncEngine | None = None

    def _get_conn(self) -> dict[str, Any]:  # type: ignore[override]
        """Read Airflow Connection and return kwargs for create_async_engine."""
        conn = self.get_connection(self.postgres_conn_id)
        return {
            "url": f"postgresql+asyncpg://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}",
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "pool_pre_ping": self.pool_pre_ping,
        }

    @property
    def engine(self) -> AsyncEngine:
        if self._engine is None:
            self._engine = create_async_engine(**self._get_conn())
        return self._engine

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        async with AsyncSession(self.engine) as sess:
            yield sess

    async def _extract_async(self, sql: str, parameters: dict[str, Any]) -> list[dict[str, Any]]:
        from sqlalchemy import text
        async with self.session() as sess:
            result = await sess.execute(text(sql), parameters)
            return [dict(row._mapping) for row in result.fetchall()]

    def extract(self, sql: str, parameters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Sync facade — runs async extraction in event loop."""
        return asyncio.get_event_loop().run_until_complete(
            self._extract_async(sql, parameters or {})
        )

    def on_disconnect(self) -> None:
        if self._engine is not None:
            asyncio.get_event_loop().run_until_complete(self._engine.dispose())
            self._engine = None
