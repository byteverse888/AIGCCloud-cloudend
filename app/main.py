""" 
AIGC Cloud Platform API
主入口文件
"""
import signal
import sys
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.core.config import settings
from app.core.redis_client import redis_client
from app.core.logger import logger
from app.core.arq_worker import get_arq_pool, close_arq_pool
from app.core.parse_client import parse_client
from app.api.v1 import router as api_v1_router

# ARQ Worker 实例
_arq_worker = None

# Windows 下强制终止的信号处理
def _force_exit(sig, frame):
    logger.info(f"\n收到信号 {sig}，强制退出...")
    sys.exit(0)

# 注册 SIGINT (Ctrl+C)
signal.signal(signal.SIGINT, _force_exit)


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
    
    # 初始化 ARQ 连接池
    try:
        await get_arq_pool()
        logger.info("ARQ 连接池初始化成功")
    except Exception as e:
        logger.error(f"ARQ 连接失败: {e}")
    
    # 启动 ARQ Worker
    global _arq_worker
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
            handle_signals=False,  # 不让 ARQ 接管信号处理，避免和 uvicorn 冲突
        )
        asyncio.create_task(_arq_worker.async_run())
        logger.info("ARQ Worker 已启动")
    except Exception as e:
        logger.error(f"ARQ Worker 启动失败: {e}")
    
    yield
    
    # Shutdown
    logger.info("Shutting down CloudendAPI...")
    
    # 关闭 ARQ
    try:
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
    allow_origins=["*"],  # 生产环境应限制为具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(api_v1_router, prefix="/api/v1")


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
