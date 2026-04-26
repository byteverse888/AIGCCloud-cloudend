"""
日志配置模块
- 文件日志：轮转保留5个文件，每个最大10MB
- 终端日志：打印关键信息
"""
import logging
import os
from logging.handlers import RotatingFileHandler
from app.core.config import settings


def setup_logging():
    """配置日志系统"""
    # 创建日志目录
    log_dir = settings.log_dir
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_file = os.path.join(log_dir, settings.log_file)
    
    # 解析日志级别
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    
    # 日志格式
    file_format = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_format = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # 文件 Handler - 轮转日志
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,               # 保留5个文件
        encoding='utf-8'
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(file_format)
    
    # 终端 Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_format)
    
    # 根 Logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # 应用 Logger
    app_logger = logging.getLogger('aigccloud')
    app_logger.setLevel(log_level)
    
    # 降低第三方库日志级别
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('httpcore').setLevel(logging.WARNING)
    logging.getLogger('uvicorn.access').setLevel(logging.INFO)
    
    return app_logger


# 全局 logger 实例
logger = setup_logging()
