"""
激励系统端点
"""
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional
from datetime import datetime

from app.core.parse_client import parse_client
from app.core.redis_client import redis_client
from app.core.web3_client import web3_client
from app.core.deps import get_current_user_id
from app.core.incentive_service import incentive_service, IncentiveType, INCENTIVE_CONFIG

router = APIRouter()


# ============ 模型 ============

class GrantIncentiveRequest(BaseModel):
    user_id: str
    type: str  # IncentiveType 字符串
    amount: float
    description: str


# ============ 端点 ============

@router.get("/history")
async def get_incentive_history(
    page: int = 1,
    limit: int = 20,
    type: Optional[str] = None,
    user_id: str = Depends(get_current_user_id)
):
    """
    获取用户激励历史
    返回精简字段：type、amount、description、status、settlementStatus、created_at
    """
    where = {"userId": user_id}
    if type:
        where["type"] = type
    
    skip = (page - 1) * limit
    result = await parse_client.query_objects(
        "IncentiveLog",
        where=where,
        order="-createdAt",
        limit=limit,
        skip=skip
    )
    
    total = await parse_client.count_objects("IncentiveLog", where)
    
    records = []
    for item in result.get("results", []):
        records.append({
            "id": item["objectId"],
            "type": item.get("type", ""),
            "amount": item.get("amount", 0),
            "description": item.get("description", ""),
            "status": item.get("status", ""),
            "settlementStatus": item.get("settlementStatus", "pending"),
            "created_at": item.get("createdAt", ""),
        })
    
    return {
        "data": records,
        "total": total,
        "page": page,
        "limit": limit,
    }


@router.get("/balance")
async def get_balance(user_id: str = Depends(get_current_user_id)):
    """
    获取用户金币余额
    返回：链上余额 + 待结算积分
    """
    try:
        user = await parse_client.get_user(user_id)
        web3_address = user.get("web3Address")
        
        # 链上余额
        coins = 0
        if web3_address:
            coins = await web3_client.get_balance(web3_address)
        
        # 待结算积分（从 IncentiveLog 实时计算，避免计数器不一致）
        pending_result = await parse_client.query_objects(
            "IncentiveLog",
            where={"userId": user_id, "settlementStatus": "pending"},
            limit=1000
        )
        pending_coins = sum(item.get("amount", 0) for item in pending_result.get("results", []))
        
        return {
            "coins": coins,
            "pending_coins": pending_coins,
            "web3_address": web3_address,
            "member_level": user.get("memberLevel", "normal"),
        }
    except Exception:
        raise HTTPException(status_code=404, detail="用户不存在")


@router.get("/stats")
async def get_incentive_stats(user_id: str = Depends(get_current_user_id)):
    """
    获取激励统计
    """
    try:
        user = await parse_client.get_user(user_id)
    except Exception:
        raise HTTPException(status_code=404, detail="用户不存在")
    
    # 链上余额
    web3_address = user.get("web3Address")
    coins = 0
    if web3_address:
        coins = await web3_client.get_balance(web3_address)
    
    # 待结算积分（从 IncentiveLog 实时计算）
    pending_result = await parse_client.query_objects(
        "IncentiveLog",
        where={"userId": user_id, "settlementStatus": "pending"},
        limit=1000
    )
    pending_coins = sum(item.get("amount", 0) for item in pending_result.get("results", []))
    
    # 统计各类型奖励
    stats = {}
    for itype in IncentiveType:
        count = await parse_client.count_objects("IncentiveLog", {
            "userId": user_id,
            "type": itype.value
        })
        stats[itype.value] = count
    
    # 计算总获得金币
    result = await parse_client.query_objects(
        "IncentiveLog",
        where={"userId": user_id, "amount": {"$gt": 0}},
        limit=1000
    )
    total_earned = sum(item.get("amount", 0) for item in result.get("results", []))
    
    return {
        "coins": coins,
        "pending_coins": pending_coins,
        "web3_address": web3_address,
        "total_earned": total_earned,
        "by_type": stats,
    }


@router.post("/grant")
async def grant_incentive(request: GrantIncentiveRequest):
    """
    发放激励(内部接口) - 通过Web3接口铸造金币到联盟链
    """
    try:
        user = await parse_client.get_user(request.user_id)
    except Exception:
        raise HTTPException(status_code=404, detail="用户不存在")
    
    web3_address = user.get("web3Address")
    if not web3_address:
        raise HTTPException(status_code=400, detail="用户未绑定Web3地址")
    
    # 通过Web3接口铸造金币
    mint_result = await web3_client.mint(web3_address, int(request.amount))
    if not mint_result.get("success"):
        raise HTTPException(status_code=500, detail="发放奖励失败: " + mint_result.get("error", ""))
    
    # 记录激励日志
    await parse_client.create_object("IncentiveLog", {
        "userId": request.user_id,
        "web3Address": web3_address,
        "type": request.type,
        "amount": request.amount,
        "txHash": mint_result.get("tx_hash"),
        "description": request.description,
        "status": "success",
        "settlementStatus": "settled",
    })
    
    # 获取新余额
    new_balance = await web3_client.get_balance(web3_address)
    
    return {
        "success": True,
        "user_id": request.user_id,
        "amount": request.amount,
        "tx_hash": mint_result.get("tx_hash"),
        "new_coins": new_balance
    }


@router.post("/consume")
async def consume_coins(
    amount: float,
    description: str = "金币消费",
    user_id: str = Depends(get_current_user_id)
):
    """
    消费金币 - 通过Web3接口销毁金币
    """
    try:
        user = await parse_client.get_user(user_id)
    except Exception:
        raise HTTPException(status_code=404, detail="用户不存在")
    
    web3_address = user.get("web3Address")
    if not web3_address:
        raise HTTPException(status_code=400, detail="用户未绑定Web3地址")
    
    # 检查联盟链上的余额
    balance = await web3_client.get_balance(web3_address)
    if balance < amount:
        raise HTTPException(status_code=400, detail="余额不足")
    
    # 通过Web3接口销毁金币
    burn_result = await web3_client.burn(web3_address, int(amount))
    if not burn_result.get("success"):
        raise HTTPException(status_code=500, detail="消费失败: " + burn_result.get("error", ""))
    
    # 记录消费日志
    await parse_client.create_object("IncentiveLog", {
        "userId": user_id,
        "web3Address": web3_address,
        "type": "consume",
        "amount": -amount,
        "txHash": burn_result.get("tx_hash"),
        "description": description,
        "status": "success",
        "settlementStatus": "settled",
    })
    
    # 获取新余额
    new_balance = await web3_client.get_balance(web3_address)
    
    return {
        "success": True,
        "consumed": amount,
        "tx_hash": burn_result.get("tx_hash"),
        "new_coins": new_balance
    }
