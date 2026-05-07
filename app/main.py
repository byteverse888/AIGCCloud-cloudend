""" 
AIGC Cloud Platform API
主入口文件
"""
import os
import signal
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import httpx

from app.core.config import settings
from app.core.redis_client import redis_client
from app.core.logger import logger
from app.core.arq_worker import get_arq_pool, close_arq_pool
from app.core.parse_client import parse_client
from app.api.v1 import router as api_v1_router

# ARQ Worker 实例
_arq_worker = None
_arq_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # Startup
    logger.info("Starting up CloudendAPI...")
    
    # 初始化 Redis 连接
    try:
        await redis_client.connect()
        logger.info("Redis connected successfully")
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
    
    # 初始化 Parse Schema（确保所有业务类存在）
    try:
        await parse_client.ensure_schema()
        logger.info("Parse Schema 初始化完成")
    except Exception as e:
        logger.error(f"Parse Schema 初始化失败: {e}")
    
    # 创建默认管理员/运营用户
    try:
        await parse_client.ensure_default_users()
        logger.info("默认用户初始化完成")
    except Exception as e:
        logger.error(f"默认用户初始化失败: {e}")
    
    # 初始化 ARQ 连接池
    try:
        await get_arq_pool()
        logger.info("ARQ 连接池初始化成功")
    except Exception as e:
        logger.error(f"ARQ 连接失败: {e}")
    
    # 启动 ARQ Worker
    global _arq_worker, _arq_task
    try:
        import asyncio
        from arq import Worker
        from app.tasks.worker import WorkerSettings
        
        _arq_worker = Worker(
            functions=WorkerSettings.functions,
            cron_jobs=WorkerSettings.cron_jobs,
            redis_settings=WorkerSettings.redis_settings,
            max_jobs=WorkerSettings.max_jobs,
            job_timeout=WorkerSettings.job_timeout,
            handle_signals=False,
        )
        _arq_task = asyncio.create_task(_arq_worker.async_run())
        logger.info("ARQ Worker 已启动")
    except Exception as e:
        logger.error(f"ARQ Worker 启动失败: {e}")
    
    yield
    
    # Shutdown
    logger.info("Shutting down CloudendAPI...")
    
    # 先取消 ARQ 任务，再关闭 Worker
    try:
        if _arq_task and not _arq_task.done():
            _arq_task.cancel()
        if _arq_worker:
            await _arq_worker.close()
        await close_arq_pool()
        logger.info("ARQ 已关闭")
    except Exception:
        pass
    
    # 关闭 Redis 连接
    try:
        await redis_client.disconnect()
        logger.info("Redis disconnected")
    except Exception:
        pass
    
    # 关闭 Parse 连接池
    try:
        await parse_client.close()
        logger.info("Parse client closed")
    except Exception:
        pass


app = FastAPI(
    title="AIGC Cloud Platform API",
    description="""
## CloudendAPI - AIGC云平台后端服务

### 功能模块

- **用户管理** `/api/v1/users` - 注册、激活、Web3绑定
- **支付管理** `/api/v1/payment` - 订单创建、微信支付回调
- **任务管理** `/api/v1/tasks` - AI任务提交、状态查询
- **激励系统** `/api/v1/incentive` - 每日奖励、金币管理
- **推广系统** `/api/v1/promotion` - 邀请统计、推广记录
- **商品管理** `/api/v1/products` - 审核、举报处理

### 认证方式

大部分接口需要在 Header 中携带 JWT Token:
```
Authorization: Bearer <token>
```
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# CORS 配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.frontend_url.split(",") if not settings.debug else ["*"],  # 生产环境限制具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(api_v1_router, prefix="/api/v1")


# ============ 全局异常处理器 ============

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """捕获所有未处理的异常，返回统一 JSON 格式"""
    logger.error(f"[Unhandled] {request.method} {request.url.path} -> {type(exc).__name__}: {exc}")
    return JSONResponse(
        status_code=500,
        content={"success": False, "detail": "服务器内部错误，请稍后重试"},
    )


@app.exception_handler(httpx.ConnectError)
async def httpx_connect_error_handler(request: Request, exc: httpx.ConnectError):
    """Parse Server 连接失败时的友好响应"""
    logger.error(f"[ConnectError] {request.method} {request.url.path} -> {exc}")
    return JSONResponse(
        status_code=502,
        content={"success": False, "detail": "上游服务暂不可用，请稍后重试"},
    )


@app.exception_handler(httpx.TimeoutException)
async def httpx_timeout_handler(request: Request, exc: httpx.TimeoutException):
    """上游超时的友好响应"""
    logger.error(f"[Timeout] {request.method} {request.url.path} -> {exc}")
    return JSONResponse(
        status_code=504,
        content={"success": False, "detail": "请求超时，请稍后重试"},
    )


@app.get("/", tags=["Root"])
async def root():
    """API 根路径"""
    return {
        "message": "Welcome to AIGC Cloud Platform API",
        "version": "1.0.0",
        "docs": "/docs",
        "redoc": "/redoc",
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """健康检查"""
    health_status = {
        "status": "healthy",
        "services": {
            "api": "up",
        }
    }
    
    # 检查 Redis
    try:
        if redis_client._client:
            await redis_client.client.ping()
            health_status["services"]["redis"] = "up"
        else:
            health_status["services"]["redis"] = "not connected"
    except Exception:
        health_status["services"]["redis"] = "down"
    
    return health_status


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug,
    )
