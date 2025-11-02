import os
import asyncpg
from contextlib import asynccontextmanager
from typing import Optional
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

DWH_HOST = os.getenv("DWH_HOST")
DWH_PORT = os.getenv("DWH_PORT", "5432")
DWH_DB = os.getenv("DWH_DB")
DWH_USER = os.getenv("DWH_USER")
DWH_PASSWORD = os.getenv("DWH_PASSWORD")

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    """Obtiene el pool de conexiones al DWH"""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            host=DWH_HOST,
            port=DWH_PORT,
            database=DWH_DB,
            user=DWH_USER,
            password=DWH_PASSWORD,
            min_size=2,
            max_size=10,
            command_timeout=60
        )
    return _pool


async def close_pool():
    """Cierra el pool de conexiones"""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


@asynccontextmanager
async def get_connection():
    """Context manager para obtener una conexión del pool"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        yield conn
