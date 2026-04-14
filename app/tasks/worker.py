"""
ARQ Worker 配置 - 用于独立运行 worker
运行: arq app.tasks.worker.WorkerSettings
"""
from arq.connections import RedisSettings
from arq.cron import cron
from app.core.config import settings
from app.tasks.arq_tasks import (
    process_pending_orders,
    process_paid_order,
    process_paid_tx_orders,
    execute_ai_task,
    check_timeout_tasks,
    settle_pending_incentives,
)


class WorkerSettings:
    """ARQ Worker 配置"""
    
    # 任务函数列表
    functions = [
        process_pending_orders,
        process_paid_order,
        process_paid_tx_orders,
        execute_ai_task,
        check_timeout_tasks,
        settle_pending_incentives,
    ]
    
    # 定时任务
    cron_jobs = [
        # 每5分钟处理待支付订单
        cron(process_pending_orders, minute={0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55}),
        # 每5分钟处理支付中订单
        cron(process_paid_tx_orders, minute={2, 7, 12, 17, 22, 27, 32, 37, 42, 47, 52, 57}),
        # 每10分钟检查超时任务
        cron(check_timeout_tasks, minute={0, 10, 20, 30, 40, 50}),
        # 每天24小时清算待结算激励积分（凌晨2点执行）
        cron(settle_pending_incentives, hour={2}, minute={0}),
    ]
    
    # Redis 配置
    redis_settings = RedisSettings(
        host=settings.redis_host,
        port=settings.redis_port,
        database=settings.redis_db,
        password=settings.redis_password or None,
    )
    
    # 队列名称前缀（隔离不同服务的 ARQ 任务）
    queue_name = 'arq:cloudend'
    
    # Worker 配置
    max_jobs = 10
    job_timeout = 300  # 5分钟超时
    keep_result = 3600  # 结果保留1小时
    retry_jobs = True
    max_tries = 3
