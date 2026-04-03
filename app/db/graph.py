"""
Delllo — Memgraph Graph DB Connection (neo4j driver)
Memgraph is Bolt-compatible so we use the neo4j Python driver.
"""

from neo4j import AsyncGraphDatabase, AsyncDriver
from app.config import settings

_driver: AsyncDriver | None = None


async def init_graph():
    global _driver
    _driver = AsyncGraphDatabase.driver(
        f"bolt://{settings.memgraph_host}:{settings.memgraph_port}",
        auth=(settings.memgraph_user, settings.memgraph_password),
    )
    await _driver.verify_connectivity()


async def close_graph():
    global _driver
    if _driver:
        await _driver.close()
        _driver = None


def get_driver() -> AsyncDriver:
    if _driver is None:
        raise RuntimeError("Graph driver not initialised. Call init_graph() first.")
    return _driver


async def get_graph_session():
    """FastAPI dependency — yields an async graph session."""
    async with get_driver().session() as session:
        yield session
