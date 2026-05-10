"""
运营端订单处理接口
"""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone

from app.core.parse_client import parse_client
from app.core.deps import get_current_user_id, get_operator_user_id
from app.core.logger import logger

router = APIRouter()


@router.get("/orders")
async def get_orders(
    page: int = 1,
    limit: int = 20,
    status: Optional[str] = None,
    search: Optional[str] = None,
    buyer_user_id: Optional[str] = None,
    operator_id: str = Depends(get_operator_user_id)
):
    """获取订单列表（运营人员，支持按购买者 userId 过滤）"""
    where = {}
    if status:
        where["status"] = status
    if search:
        where["$or"] = [
            {"orderNo": {"$regex": search, "$options": "i"}},
            {"userId": {"$regex": search, "$options": "i"}},
        ]
    if buyer_user_id:
        # 支持按 userId 精确 或 用户名 模糊搜索
        user_kw = buyer_user_id.strip()
        candidate_ids = {user_kw}
        try:
            u_res = await parse_client.query_users(
                where={"username": {"$regex": user_kw, "$options": "i"}}, limit=50
            )
            for u in u_res.get("results", []):
                if u.get("objectId"):
                    candidate_ids.add(u["objectId"])
        except Exception:
            pass
        where["userId"] = {"$in": list(candidate_ids)} if len(candidate_ids) > 1 else user_kw
    
    skip = (page - 1) * limit
    result = await parse_client.query_objects(
        "Order",
        where=where if where else None,
        order="-createdAt",
        limit=limit,
        skip=skip,
    )
    total = await parse_client.count_objects("Order", where if where else None)
    
    orders = []
    for order in result.get("results", []):
        user_id = order.get("userId")
        username = ""
        if user_id:
            try:
                user = await parse_client.get_user(user_id)
                username = user.get("username", "")
            except Exception:
                pass
        orders.append({
            "id": order["objectId"],
            "orderNo": order.get("orderNo", ""),
            "user": username,
            "userId": user_id,
            "amount": order.get("amount", 0),
            "status": order.get("status", ""),
            "type": order.get("type", ""),
            "paymentMethod": order.get("paymentMethod", "-"),
            "createdAt": order.get("createdAt", ""),
        })
    
    return {"data": orders, "total": total, "page": page, "limit": limit}


@router.get("/orders/{order_id}")
async def get_order_detail(
    order_id: str,
    operator_id: str = Depends(get_operator_user_id)
):
    """获取订单详情"""
    try:
        order = await parse_client.get_object("Order", order_id)
    except Exception:
        raise HTTPException(status_code=404, detail="订单不存在")
    
    user_id = order.get("userId")
    username = ""
    if user_id:
        try:
            user = await parse_client.get_user(user_id)
            username = user.get("username", "")
        except Exception:
            pass
    
    product_name = ""
    product_id = order.get("productId")
    if product_id:
        try:
            product = await parse_client.get_object("Product", product_id)
            product_name = product.get("name", "")
        except Exception:
            pass
    
    return {
        "id": order["objectId"],
        "orderNo": order.get("orderNo", ""),
        "userId": user_id,
        "username": username,
        "productId": product_id,
        "productName": product_name,
        "amount": order.get("amount", 0),
        "status": order.get("status", ""),
        "type": order.get("type", ""),
        "paymentMethod": order.get("paymentMethod", "-"),
        "txHash": order.get("txHash", ""),
        "createdAt": order.get("createdAt", ""),
        "paidAt": order.get("paidAt"),
        "completedAt": order.get("completedAt"),
    }


class RefundRequest(BaseModel):
    reason: str = ""


@router.post("/orders/{order_id}/refund")
async def refund_order(
    order_id: str,
    request: RefundRequest,
    operator_id: str = Depends(get_operator_user_id)
):
    """退款订单"""
    try:
        order = await parse_client.get_object("Order", order_id)
    except Exception:
        raise HTTPException(status_code=404, detail="订单不存在")
    
    if order.get("status") not in ["pending", "paid", "completed"]:
        raise HTTPException(status_code=400, detail="该订单状态不允许退款")
    
    refund_amount = float(order.get("amount", 0))
    
    await parse_client.update_object("Order", order_id, {
        "status": "refunded",
        "refundAmount": refund_amount,
        "refundReason": request.reason,
        "refundBy": operator_id,
        "refundedAt": datetime.now(timezone.utc).isoformat(),
    })
    
    logger.info(f"[订单退款] order_id={order_id}, amount={refund_amount}, operator={operator_id}")
    
    return {"success": True, "message": "退款成功", "refund_amount": refund_amount}