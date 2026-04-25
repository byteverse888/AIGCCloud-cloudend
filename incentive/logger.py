"""
日志配置
支持轮转文件日志 + 控制台输出，按大小轮转，保留可配置天数。
"""
import logging
import os
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

from incentive.config import config


def _clean_old_logs():
    """清理超过保留天数的日志文件"""
    log_dir = Path(config.log_dir)
    if not log_dir.exists():
        return
    cutoff = time.time() - config.log_retention_days * 86400
    for f in log_dir.glob("*.log*"):
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink()
        except OSError:
            pass


def setup_logger(name: str = "incentive") -> logging.Logger:
    """创建并配置 logger"""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, config.log_level.upper(), logging.INFO))

    fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 控制台
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    # 文件（轮转）
    log_dir = Path(config.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        str(log_dir / config.log_file),
        maxBytes=config.log_max_bytes,
        backupCount=config.log_backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    # 启动时清理过期日志
    _clean_old_logs()

    return logger


# 全局 logger
logger = setup_logger()
