"""
激励服务 - DB累积模式
激励积分先记录DB并累积待结算积分，达到阈值后由定时任务批量清算上链
"""
from typing import Optional
from enum import Enum
from datetime import datetime
from app.core.config import settings
from app.core.parse_client import parse_client
from app.core.logger import logger


class IncentiveType(str, Enum):
    """激励类型"""
    REGISTER = "register"           # 注册奖励
    DAILY_LOGIN = "daily_login"     # 每日登录（暂不实现）
    INVITE = "invite"               # 邀请奖励（暂不实现）
    INVITE_RECHARGE = "invite_recharge"  # 邀请首充返利（暂不实现）
    TASK = "task"                   # 任务奖励
    RECHARGE = "recharge"           # 充值奖励（暂不实现）
    ACTIVITY = "activity"           # 活动奖励（暂不实现）
    MEMBER_SUBSCRIBE = "member_subscribe"  # 会员订阅奖励


# 激励配置
INCENTIVE_CONFIG = {
    "register": 100,                    # 注册奖励
    "daily_login_normal": 5,            # 普通用户每日登录（暂不实现）
    "daily_login_paid": 10,             # 付费用户每日登录（暂不实现）
    "invite_register": 100,             # 邀请注册奖励（暂不实现）
    "invite_first_recharge_rate": 0.1,  # 邀请首充返利比例（暂不实现）
    "task_complete": 1,                 # 任务完成奖励
    "recharge_rate": 0.05,              # 充值奖励比例（暂不实现）
    "member_subscribe_vip": 50,         # VIP订阅奖励
    "member_subscribe_svip": 100,       # SVIP订阅奖励
}

# 清算阈值
SETTLEMENT_THRESHOLD = 1000  # 待结算积分达到此值后才进行链上清算


class IncentiveService:
    """激励服务 - DB累积模式"""
    
    async def grant_incentive(
        self,
        user_id: str,
        web3_address: str,
        incentive_type: IncentiveType,
        amount: float,
        description: str,
        related_id: Optional[str] = None
    ) -> dict:
        """
        发放激励 - 核心公共函数（DB累积模式）
        
        流程：
        1. 创建 IncentiveLog 记录（settlementStatus=pending）
        2. 原子递增用户的 pendingCoins
        
        Args:
            user_id: 用户ID
            web3_address: 用户Web3地址
            incentive_type: 激励类型
            amount: 激励金额（金币数量）
            description: 描述
            related_id: 关联ID（如任务ID、订单ID等）
            
        Returns:
            发放结果
        """
        if not web3_address:
            return {"success": False, "error": "用户未绑定Web3地址"}
        
        if amount <= 0:
            return {"success": False, "error": "激励金额必须大于0"}
        
        logger.info(f"[激励服务] 发放激励: user={user_id}, amount={amount}, type={incentive_type.value}")
        
        try:
            # 创建 IncentiveLog 记录（待结算状态）
            log_data = {
                "userId": user_id,
                "web3Address": web3_address,
                "type": incentive_type.value,
                "amount": amount,
                "description": description,
                "status": "success",
                "settlementStatus": "pending",
            }
            if related_id:
                log_data["relatedId"] = related_id
            
            await parse_client.create_object("IncentiveLog", log_data)
            
            logger.info(f"[激励服务] 激励发放成功: user={user_id}, +{amount} 金币 (待结算)")
            return {
                "success": True,
                "amount": amount,
                "message": f"成功发放 {amount} 金币（待结算）"
            }
            
        except Exception as e:
            logger.error(f"[激励服务] 激励发放失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def grant_register_reward(self, user_id: str) -> dict:
        """
        发放注册奖励
        
        Args:
            user_id: 新注册用户ID
        """
        try:
            user = await parse_client.get_user(user_id)
        except Exception:
            return {"success": False, "error": "用户不存在"}
        
        web3_address = user.get("web3Address")
        if not web3_address:
            return {"success": False, "error": "用户未绑定Web3地址"}
        
        amount = INCENTIVE_CONFIG["register"]
        
        return await self.grant_incentive(
            user_id=user_id,
            web3_address=web3_address,
            incentive_type=IncentiveType.REGISTER,
            amount=amount,
            description="新用户注册奖励"
        )
    
    async def grant_member_subscribe_reward(
        self,
        user_id: str,
        plan_name: str,
        member_level: str,
        order_id: Optional[str] = None,
        bonus: Optional[int] = None
    ) -> dict:
        """
        发放会员订阅奖励
        
        Args:
            user_id: 用户ID
            plan_name: 套餐名称
            member_level: 会员等级 (vip/svip)
            order_id: 订单ID
            bonus: 自定义奖励金额（从套餐配置获取），为空则使用默认配置
        """
        try:
            user = await parse_client.get_user(user_id)
        except Exception:
            return {"success": False, "error": "用户不存在"}
        
        web3_address = user.get("web3Address")
        if not web3_address:
            return {"success": False, "error": "用户未绑定Web3地址"}
        
        # 使用传入的 bonus 或默认配置
        if bonus is not None and bonus > 0:
            amount = float(bonus)
        else:
            config_key = f"member_subscribe_{member_level}"
            amount = float(INCENTIVE_CONFIG.get(config_key, 50))
        
        return await self.grant_incentive(
            user_id=user_id,
            web3_address=web3_address,
            incentive_type=IncentiveType.MEMBER_SUBSCRIBE,
            amount=amount,
            description=f"会员订阅奖励 - {plan_name}",
            related_id=order_id
        )
    
    async def grant_task_reward(
        self, 
        user_id: str, 
        task_id: str, 
        task_type: str,
        amount: Optional[float] = None
    ) -> dict:
        """
        发放任务完成奖励
        
        Args:
            user_id: 用户ID
            task_id: 任务ID
            task_type: 任务类型
            amount: 奖励金额（为空则使用默认配置）
        """
        try:
            user = await parse_client.get_user(user_id)
        except Exception:
            return {"success": False, "error": "用户不存在"}
        
        web3_address = user.get("web3Address")
        if not web3_address:
            return {"success": False, "error": "用户未绑定Web3地址"}
        
        reward_amount = amount or INCENTIVE_CONFIG["task_complete"]
        
        return await self.grant_incentive(
            user_id=user_id,
            web3_address=web3_address,
            incentive_type=IncentiveType.TASK,
            amount=reward_amount,
            description=f"完成 {task_type} 任务奖励",
            related_id=task_id
        )


# 全局单例
incentive_service = IncentiveService()
