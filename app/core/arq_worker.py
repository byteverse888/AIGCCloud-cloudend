"""
ARQ 异步任务队列配置
"""
import asyncio
from typing import Optional
from arq import create_pool
from arq.connections import RedisSettings, ArqRedis
from app.core.config import settings
from app.core.logger import logger


# ARQ 队列名称（与 worker.py 保持一致）
ARQ_QUEUE_NAME = 'arq:cloudend'

# ARQ Redis 连接池
_arq_pool: Optional[ArqRedis] = None


def get_redis_settings() -> RedisSettings:
    """获取 ARQ Redis 配置"""
    return RedisSettings(
        host=settings.redis_host,
        port=settings.redis_port,
        database=settings.redis_db,
        password=settings.redis_password or None,
    )


async def get_arq_pool() -> ArqRedis:
    """获取 ARQ 连接池"""
    global _arq_pool
    if _arq_pool is None:
        _arq_pool = await create_pool(get_redis_settings())
    return _arq_pool


async def close_arq_pool():
    """关闭 ARQ 连接池"""
    global _arq_pool
    if _arq_pool:
        await _arq_pool.close()
        _arq_pool = None


async def enqueue_task(func_name: str, *args, **kwargs):
    """
    添加任务到队列
    
    Args:
        func_name: 任务函数名
        *args: 位置参数
        **kwargs: 关键字参数
    """
    pool = await get_arq_pool()
    job = await pool.enqueue_job(func_name, *args, _queue_name=ARQ_QUEUE_NAME, **kwargs)
    logger.info(f"[ARQ] 任务入队: {func_name}, job_id: {job.job_id}")
    return job
