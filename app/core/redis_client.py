"""
Redis 客户端
"""
import redis.asyncio as redis
from typing import Optional, Any
from app.core.config import settings
from app.core.logger import logger


class RedisClient:
    """Redis 异步客户端"""
    
    def __init__(self):
        self._client: Optional[redis.Redis] = None
    
    async def connect(self):
        """建立连接"""
        if self._client is None:
            redis_url = settings.redis_url
            logger.info(f"[Redis] 连接URL: {redis_url}")
            self._client = redis.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=True
            )
            # 立即测试连接
            try:
                await self._client.ping()
                logger.info("[Redis] Ping 成功")
            except Exception as e:
                logger.error(f"[Redis] Ping 失败: {e}")
                raise
    
    async def disconnect(self):
        """关闭连接"""
        if self._client:
            await self._client.close()
            self._client = None
    
    @property
    def client(self) -> redis.Redis:
        if self._client is None:
            raise RuntimeError("Redis client not connected. Call connect() first.")
        return self._client
    
    # ============ 基础操作 ============
    
    async def get(self, key: str) -> Optional[str]:
        """获取值"""
        return await self.client.get(key)
    
    async def set(self, key: str, value: Any, ex: Optional[int] = None) -> bool:
        """设置值，ex为过期时间(秒)"""
        return await self.client.set(key, value, ex=ex)
    
    async def setnx(self, key: str, value: Any, ex: Optional[int] = None) -> bool:
        """原子操作: 仅当键不存在时设置值，返回是否设置成功"""
        result = await self.client.setnx(key, value)
        if result and ex:
            await self.client.expire(key, ex)
        return result
    
    async def delete(self, key: str) -> int:
        """删除键"""
        return await self.client.delete(key)
    
    async def exists(self, key: str) -> bool:
        """检查键是否存在"""
        return await self.client.exists(key) > 0
    
    async def expire(self, key: str, seconds: int) -> bool:
        """设置过期时间"""
        return await self.client.expire(key, seconds)
    
    async def ttl(self, key: str) -> int:
        """获取剩余过期时间"""
        return await self.client.ttl(key)
    
    # ============ Hash 操作 ============
    
    async def hget(self, name: str, key: str) -> Optional[str]:
        """获取哈希字段值"""
        return await self.client.hget(name, key)
    
    async def hset(self, name: str, key: str, value: Any) -> int:
        """设置哈希字段值"""
        return await self.client.hset(name, key, value)
    
    async def hgetall(self, name: str) -> dict:
        """获取哈希所有字段"""
        return await self.client.hgetall(name)
    
    async def hdel(self, name: str, *keys: str) -> int:
        """删除哈希字段"""
        return await self.client.hdel(name, *keys)
    
    # ============ List 操作 ============
    
    async def lpush(self, name: str, *values: Any) -> int:
        """从左侧插入"""
        return await self.client.lpush(name, *values)
    
    async def rpush(self, name: str, *values: Any) -> int:
        """从右侧插入"""
        return await self.client.rpush(name, *values)
    
    async def lpop(self, name: str) -> Optional[str]:
        """从左侧弹出"""
        return await self.client.lpop(name)
    
    async def rpop(self, name: str) -> Optional[str]:
        """从右侧弹出"""
        return await self.client.rpop(name)
    
    async def lrange(self, name: str, start: int, end: int) -> list:
        """获取列表范围元素"""
        return await self.client.lrange(name, start, end)
    
    async def llen(self, name: str) -> int:
        """获取列表长度"""
        return await self.client.llen(name)
    
    # ============ Set 操作 ============
    
    async def sadd(self, name: str, *values: Any) -> int:
        """添加集合成员"""
        return await self.client.sadd(name, *values)
    
    async def srem(self, name: str, *values: Any) -> int:
        """移除集合成员"""
        return await self.client.srem(name, *values)
    
    async def sismember(self, name: str, value: Any) -> bool:
        """检查是否为集合成员"""
        return await self.client.sismember(name, value)
    
    async def smembers(self, name: str) -> set:
        """获取集合所有成员"""
        return await self.client.smembers(name)
    
    # ============ 计数器操作 ============
    
    async def incr(self, key: str, amount: int = 1) -> int:
        """自增"""
        return await self.client.incrby(key, amount)
    
    async def decr(self, key: str, amount: int = 1) -> int:
        """自减"""
        return await self.client.decrby(key, amount)
    
    # ============ 业务方法 ============
    
    async def set_daily_claim_flag(self, user_id: str) -> bool:
        """设置每日领取标记(24h过期)"""
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        key = f"daily_claim:{today}:{user_id}"
        return await self.set(key, "1", ex=86400)  # 24小时
    
    async def check_daily_claim(self, user_id: str) -> bool:
        """检查今日是否已领取"""
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        key = f"daily_claim:{today}:{user_id}"
        return await self.exists(key)
    
    async def set_activation_token(self, token: str, user_data: dict, ex: int = 86400) -> bool:
        """存储激活Token(默认24h过期)"""
        import json
        key = f"activation:{token}"
        return await self.set(key, json.dumps(user_data), ex=ex)
    
    async def get_activation_token(self, token: str) -> Optional[dict]:
        """获取激活Token对应的用户数据"""
        import json
        key = f"activation:{token}"
        data = await self.get(key)
        if data:
            return json.loads(data)
        return None
    
    async def delete_activation_token(self, token: str) -> int:
        """删除激活Token"""
        key = f"activation:{token}"
        return await self.delete(key)
    
    async def set_reset_password_token(self, token: str, user_id: str, ex: int = 3600) -> bool:
        """存储重置密码Token(默认1h过期)"""
        key = f"reset_pwd:{token}"
        return await self.set(key, user_id, ex=ex)
    
    async def get_reset_password_token(self, token: str) -> Optional[str]:
        """获取重置密码Token对应的用户ID"""
        key = f"reset_pwd:{token}"
        return await self.get(key)


# 全局单例
redis_client = RedisClient()
