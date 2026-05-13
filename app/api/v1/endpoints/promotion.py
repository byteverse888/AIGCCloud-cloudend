"""
推广系统端点
"""
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional
from datetime import datetime

from app.core.parse_client import parse_client
from app.core.web3_client import web3_client
from app.core.deps import get_current_user_id, get_current_user_id_compat
from app.core.config import settings
from app.core.incentive_service import incentive_service, INCENTIVE_CONFIG

router = APIRouter()


# ============ 模型 ============

class PromotionStats(BaseModel):
    invite_count: int
    success_reg_count: int
    total_invite_reward: float  # 邀请奖励总额（从日志查询）
    invite_link: str
    invite_code: str
    web3_address: Optional[str] = None


class InviteRecord(BaseModel):
    id: str
    invitee_id: str
    invitee_name: str
    status: str  # registered, first_recharged
    reward: float
    created_at: datetime


# ============ 端点 ============

@router.get("/link")
async def get_promotion_link(user_id: str = Depends(get_current_user_id_compat)):
    """
    获取用户的推广链接
    """
    # 生成邀请码 (使用用户ID前8位)
    invite_code = user_id[:8]
    
    # 获取前端基础URL
    base_url = settings.frontend_url.rstrip("/")
    invite_link = f"{base_url}/register?ref={invite_code}"
    
    return {
        "invite_link": invite_link,
        "invite_code": invite_code,
        "qr_code": f"https://api.qrserver.com/v1/create-qr-code/?size=200x200&data={invite_link}",
    }


@router.get("/stats", response_model=PromotionStats)
async def get_promotion_stats(user_id: str = Depends(get_current_user_id_compat)):
    """
    获取用户的推广统计
    """
    try:
        user = await parse_client.get_user(user_id)
    except Exception:
        raise HTTPException(status_code=404, detail="用户不存在")
    
    invite_count = user.get("inviteCount", 0)
    success_reg_count = user.get("successRegCount", 0)
    
    # 统计邀请奖励总额（从日志表查询）
    result = await parse_client.query_objects(
        "IncentiveLog",
        where={"userId": user_id, "type": "invite", "amount": {"$gt": 0}},
        limit=1000
    )
    total_invite_reward = sum(item.get("amount", 0) for item in result.get("results", []))
    
    invite_code = user_id[:8]
    base_url = settings.frontend_url.rstrip("/")
    
    return PromotionStats(
        invite_count=invite_count,
        success_reg_count=success_reg_count,
        total_invite_reward=total_invite_reward,
        invite_link=f"{base_url}/register?ref={invite_code}",
        invite_code=invite_code,
        web3_address=user.get("web3Address"),
    )


@router.get("/records")
async def get_promotion_records(
    page: int = 1,
    limit: int = 20,
    user_id: str = Depends(get_current_user_id_compat)
):
    """
    获取推广记录列表
    """
    # 查询被邀请的用户
    skip = (page - 1) * limit
    result = await parse_client.query_users(
        where={"inviterId": user_id},
        order="-createdAt",
        limit=limit,
        skip=skip
    )
    
    total = await parse_client.count_objects("_User", {"inviterId": user_id})
    
    records = []
    for invitee in result.get("results", []):
        # 查询该邀请人带来的奖励（从日志表）
        rewards = await parse_client.query_objects(
            "IncentiveLog",
            where={
                "userId": user_id,
                "type": "invite",
                "description": {"$regex": invitee["username"]}
            }
        )
        total_reward = sum(r.get("amount", 0) for r in rewards.get("results", []))
        
        status = "first_recharged" if invitee.get("firstRechargeRewarded") else "registered"
        
        records.append({
            "id": invitee["objectId"],
            "invitee_id": invitee["objectId"],
            "invitee_name": invitee["username"],
            "status": status,
            "reward": total_reward,
            "created_at": invitee["createdAt"],
        })
    
    return {
        "data": records,
        "total": total,
        "page": page,
        "limit": limit,
    }


@router.get("/leaderboard")
async def get_promotion_leaderboard(limit: int = 10):
    """
    获取推广排行榜
    """
    # 按邀请成功人数排序
    result = await parse_client.query_users(
        where={"successRegCount": {"$gt": 0}},
        order="-successRegCount",
        limit=limit
    )
    
    leaderboard = []
    rank = 1
    for user in result.get("results", []):
        leaderboard.append({
            "rank": rank,
            "user_id": user["objectId"],
            "username": user["username"],
            "invite_count": user.get("inviteCount", 0),
            "success_reg_count": user.get("successRegCount", 0),
        })
        rank += 1
    
    return {"leaderboard": leaderboard}


@router.post("/bind-inviter")
async def bind_inviter(
    invite_code: str,
    user_id: str = Depends(get_current_user_id_compat)
):
    """
    绑定邀请人(注册后补绑定)
    """
    # 获取当前用户
    try:
        user = await parse_client.get_user(user_id)
    except Exception:
        raise HTTPException(status_code=404, detail="用户不存在")
    
    # 检查是否已有邀请人
    if user.get("inviterId"):
        raise HTTPException(status_code=400, detail="已绑定邀请人")
    
    # 查找邀请人
    inviters = await parse_client.query_users(
        where={"objectId": invite_code}
    )
    
    if not inviters.get("results"):
        raise HTTPException(status_code=404, detail="邀请码无效")
    
    inviter = inviters["results"][0]
    
    # 不能自己邀请自己
    if inviter["objectId"] == user_id:
        raise HTTPException(status_code=400, detail="不能使用自己的邀请码")
    
    # 绑定邀请人
    await parse_client.update_user(user_id, {"inviterId": inviter["objectId"]})
    
    # 更新邀请人统计
    await parse_client.update_user(inviter["objectId"], {
        "inviteCount": parse_client.increment(1),
        "successRegCount": parse_client.increment(1)
    })
    
    # 发放邀请奖励（通过激励服务）
    reward_result = await incentive_service.grant_invite_register_reward(
        inviter_id=inviter["objectId"],
        invitee_name=user.get("username", "新用户")
    )
    
    return {
        "success": True,
        "message": "邀请人绑定成功",
        "inviter_name": inviter["username"],
        "reward": {
            "granted": reward_result.get("success"),
            "amount": reward_result.get("amount") if reward_result.get("success") else None
        }
    }


@router.get("/rewards-config")
async def get_rewards_config():
    """
    获取推广奖励配置
    """
    return {
        "register_reward": 100,  # 邀请注册奖励
        "first_recharge_rate": 0.1,  # 首充返利比例
        "rules": [
            "成功邀请一位好友注册，即可获得100金币奖励",
            "好友首次充值，您将获得充值金额10%的返利",
            "邀请越多，奖励越多，上不封顶",
        ]
    }
