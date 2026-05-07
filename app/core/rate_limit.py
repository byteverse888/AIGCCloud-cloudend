"""
基于 Redis 的接口限流工具
"""
from fastapi import HTTPException, Request
from app.core.redis_client import redis_client
from app.core.logger import logger


async def rate_limit(
    request: Request,
    key_prefix: str = "rl",
    max_requests: int = 10,
    window_seconds: int = 60,
):
    """
    通用限流函数（基于 Redis INCR + EXPIRE）
    
    Args:
        request: FastAPI Request 对象
        key_prefix: Redis key 前缀
        max_requests: 窗口内最大请求数
        window_seconds: 时间窗口（秒）
    
    Raises:
        HTTPException 429: 超过限流阈值
    """
    # 获取客户端 IP
    client_ip = request.client.host if request.client else "unknown"
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        client_ip = forwarded.split(",")[0].strip()
    
    key = f"{key_prefix}:{client_ip}"
    
    try:
        current = await redis_client.client.incr(key)
        if current == 1:
            await redis_client.client.expire(key, window_seconds)
        
        if current > max_requests:
            logger.warning(f"[RateLimit] 限流触发: {key} ({current}/{max_requests})")
            raise HTTPException(
                status_code=429,
                detail=f"请求过于频繁，请 {window_seconds} 秒后再试"
            )
    except HTTPException:
        raise
    except Exception as e:
        # Redis 不可用时放行（降级策略）
        logger.warning(f"[RateLimit] Redis 异常，跳过限流: {e}")


async def login_rate_limit(request: Request):
    """登录接口限流：每 IP 每分钟最多 10 次"""
    await rate_limit(request, key_prefix="rl:login", max_requests=10, window_seconds=60)


async def register_rate_limit(request: Request):
    """注册接口限流：每 IP 每分钟最多 5 次"""
    await rate_limit(request, key_prefix="rl:register", max_requests=5, window_seconds=60)


async def sms_rate_limit(request: Request):
    """短信发送限流：每 IP 每分钟最多 3 次"""
    await rate_limit(request, key_prefix="rl:sms", max_requests=3, window_seconds=60)
