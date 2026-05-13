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
from app.core.deps import get_current_user_id, get_current_user_id_compat
from app.core.incentive_service import incentive_service
from app.core.logger import logger

router = APIRouter()


# ============ 模型 ============

class ExchangeRequest(BaseModel):
    amount: float


# ============ 端点 ============

@router.get("/history")
async def get_incentive_history(
    page: int = 1,
    limit: int = 20,
    type: Optional[str] = None,
    exclude_type: Optional[str] = None,
    category: Optional[str] = None,
    user_id: str = Depends(get_current_user_id_compat)
):
    """
    获取用户账户积分流水（AccountRecord 统一账本）
    返回字段：type、category、amount、balance_after、description、created_at
    可选过滤：type (recharge/purchase/refund/reward/exchange/consume)、category、exclude_type
    """
    where = {"userId": user_id, "status": "success"}
    if type:
        where["type"] = type
    if exclude_type:
        where["type"] = {"$nin": exclude_type.split(",")}
    if category:
        where["category"] = category
    
    skip = (page - 1) * limit
    result = await parse_client.query_objects(
        "AccountRecord",
        where=where,
        order="-createdAt",
        limit=limit,
        skip=skip
    )
    
    total = await parse_client.count_objects("AccountRecord", where)
    
    records = []
    for item in result.get("results", []):
        records.append({
            "id": item["objectId"],
            "type": item.get("type", ""),
            "category": item.get("category", ""),
            "amount": item.get("amount", 0),
            "balance_after": item.get("balance_after", 0),
            "description": item.get("description", ""),
            "relatedId": item.get("relatedId", ""),
            "relatedOrderNo": item.get("relatedOrderNo", ""),
            "created_at": item.get("createdAt", ""),
        })
    
    return {
        "data": records,
        "total": total,
        "page": page,
        "limit": limit,
    }


@router.get("/balance")
async def get_balance(user_id: str = Depends(get_current_user_id_compat)):
    """
    获取用户余额
    - 账户积分（totalIncentive）始终返回
    - 链上金币：仅在用户绑定了 web3Address 时查询并返回
    """
    try:
        user = await parse_client.get_user(user_id)
        web3_address = user.get("web3Address")
        balance = incentive_service._read_balance(user)

        # 未绑定钱包 → 不返回链上相关字段
        if not web3_address:
            return {
                "balance": balance,
                "coins": None,
                "web3_address": None,
                "member_level": user.get("memberLevel", "normal"),
            }

        # 已绑定 → 查询链上余额
        try:
            coins = await web3_client.get_balance(web3_address)
        except Exception as e:
            logger.warning(f"[余额] 查询链上余额失败: {e}")
            coins = 0

        return {
            "balance": balance,
            "coins": coins,
            "web3_address": web3_address,
            "member_level": user.get("memberLevel", "normal"),
        }
    except Exception:
        raise HTTPException(status_code=404, detail="用户不存在")


@router.get("/account-balance")
async def get_account_balance(user_id: str = Depends(get_current_user_id_compat)):
    """获取用户当前账户积分余额（totalIncentive）"""
    try:
        user = await parse_client.get_user(user_id)
    except Exception:
        raise HTTPException(status_code=404, detail="用户不存在")
    balance = incentive_service._read_balance(user)
    return {"balance": balance, "user_id": user_id}


# ============ 每日签到 ============

@router.post("/daily-sign")
async def daily_sign(user_id: str = Depends(get_current_user_id_compat)):
    """每日签到，发放账户积分（日级幂等）"""
    result = await incentive_service.grant_daily_sign_reward(user_id)
    if not result.get("success"):
        if result.get("signed"):
            return {"success": False, "signed": True, "message": result.get("error", "今日已签到")}
        raise HTTPException(status_code=400, detail=result.get("error", "签到失败"))
    return result


@router.get("/daily-sign/status")
async def daily_sign_status(user_id: str = Depends(get_current_user_id_compat)):
    """查询今日是否已签到 + 连续签到天数"""
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        r = await parse_client.query_objects(
            "DailySign",
            where={"userId": user_id, "signDate": today},
            limit=1,
        )
        items = r.get("results", [])
        signed = bool(items)
        continuous = int(items[0].get("continuousDays", 0)) if signed else 0
        if not signed:
            # 查昨天的连续天数预览
            from datetime import timedelta
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            y = await parse_client.query_objects(
                "DailySign", where={"userId": user_id, "signDate": yesterday}, limit=1
            )
            if y.get("results"):
                continuous = int(y["results"][0].get("continuousDays", 0))
        return {"signed": signed, "continuousDays": continuous, "date": today}
    except Exception as e:
        logger.warning(f"[签到状态] 查询失败: {e}")
        return {"signed": False, "continuousDays": 0, "date": today}


# ============ 积分兑换 ============

@router.get("/exchange-rate")
async def get_exchange_rate():
    """当前兑换比例：{points} 账户积分 = {coins} 链上金币"""
    points, coins = await incentive_service._get_exchange_rate()
    return {"points": points, "coins": coins}


@router.post("/exchange-to-web3")
async def exchange_to_web3(
    request: ExchangeRequest,
    user_id: str = Depends(get_current_user_id_compat),
):
    """账户积分 → 链上金币"""
    result = await incentive_service.exchange_to_web3(user_id, float(request.amount))
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error", "兑换失败"))
    return result


@router.post("/exchange-to-balance")
async def exchange_to_balance(
    request: ExchangeRequest,
    user_id: str = Depends(get_current_user_id_compat),
):
    """链上金币 → 账户积分"""
    result = await incentive_service.exchange_to_balance(user_id, float(request.amount))
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error", "兑换失败"))
    return result


@router.get("/stats")
async def get_incentive_stats(user_id: str = Depends(get_current_user_id_compat)):
    """
    获取账户积分统计
    - coins / web3_address：链上余额
    - total_earned：累计所获得的账户积分（AccountRecord.amount > 0 累加）
    - by_type：各类型流水计数
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
    
    # 按 type 统计流水计数（AccountRecord）
    account_types = ["recharge", "purchase", "refund", "reward", "exchange", "consume"]
    stats = {}
    for t in account_types:
        count = await parse_client.count_objects("AccountRecord", {
            "userId": user_id,
            "type": t,
            "status": "success",
        })
        stats[t] = count
    
    # 累计所获得的账户积分（所有正值）
    earned_result = await parse_client.query_objects(
        "AccountRecord",
        where={"userId": user_id, "status": "success", "amount": {"$gt": 0}},
        limit=1000,
    )
    total_earned = sum(item.get("amount", 0) for item in earned_result.get("results", []))
    
    return {
        "balance": incentive_service._read_balance(user),
        "coins": coins,
        "web3_address": web3_address,
        "total_earned": total_earned,
        "by_type": stats,
    }
