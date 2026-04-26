"""
支付管理端点 - Web3转账版本
"""
import asyncio
import logging
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional
from enum import Enum
from datetime import datetime, timedelta

from app.core.parse_client import parse_client
from app.core.web3_client import web3_client
from app.core.security import generate_order_no
from app.core.deps import get_current_user_id, get_optional_parse_user
from app.core.config import settings
from app.core.incentive_service import incentive_service
from eth_account import Account

logger = logging.getLogger(__name__)
router = APIRouter()


# ============ 枚举与模型 ============

class OrderType(str, Enum):
    SUBSCRIPTION = "subscription"  # 订阅会员
    RECHARGE = "recharge"  # 充值金币
    PURCHASE = "purchase"  # 购买商品


class OrderStatus(str, Enum):
    PENDING = "pending"              # 待支付
    PAID = "paid"                    # 支付中（转账待确认）
    PAYMENT_FAILED = "payment_failed"  # 支付失败
    COMPLETED = "completed"          # 已完成
    CANCELLED = "cancelled"          # 已取消
    REFUNDED = "refunded"            # 已退款


class SubscriptionPlan(str, Enum):
    TRIAL = "trial"
    MONTHLY = "monthly"
    HALFYEAR = "halfyear"
    YEARLY = "yearly"
    THREEYEAR = "threeyear"


# 订阅计划配置
SUBSCRIPTION_PLANS = {
    "trial": {"name": "试用会员", "price": 0.1, "days": 3, "description": "3天体验会员权益", "coins": 100},
    "monthly": {"name": "月度会员", "price": 29, "days": 30, "description": "基础会员权益", "coins": 2900},
    "halfyear": {"name": "半年会员", "price": 99, "days": 180, "description": "半年超值套餐", "coins": 9900},
    "yearly": {"name": "一年会员", "price": 139, "days": 365, "description": "年度优惠套餐", "coins": 13900},
    "threeyear": {"name": "三年会员", "price": 299, "days": 1095, "description": "三年长期套餐", "coins": 29900},
}


class VerifyTransferRequest(BaseModel):
    order_id: str
    tx_hash: str


class CreateOrderRequest(BaseModel):
    user_id: str
    amount: float
    type: str  # subscription, recharge, purchase
    plan: Optional[str] = None
    product_id: Optional[str] = None
    payment_method: Optional[str] = "web3"


class OrderResponse(BaseModel):
    order_id: str
    order_no: str
    amount: float
    status: str


# ============ 创建订单 ============

@router.post("/create-order")
async def create_order(
    request: CreateOrderRequest,
    parse_user: Optional[dict] = Depends(get_optional_parse_user)
):
    """
    创建订单（订阅/充值/购买）
    """
    logger.info(f"[创建订单] 请求参数: user_id={request.user_id}, type={request.type}, amount={request.amount}")
    
    # 验证用户身份
    user = parse_user
    if user:
        # 有 session token，验证用户 ID 是否匹配
        session_user_id = user.get("objectId")
        if session_user_id != request.user_id:
            logger.warning(f"[创建订单] 用户ID不匹配: session={session_user_id}, request={request.user_id}")
            raise HTTPException(status_code=401, detail="用户身份不匹配")
        logger.info(f"[创建订单] Session验证成功: {user.get('username')}")
    else:
        # 无 session token，向后兼容
        logger.info(f"[创建订单] 无Session，使用 user_id 创建订单")
    
    # 获取订单金额和详情
    amount = request.amount
    description = ""
    coins = 0
    plan_key = None
    
    if request.type == "subscription" and request.plan:
        plan = SUBSCRIPTION_PLANS.get(request.plan)
        if plan:
            amount = plan["price"]
            description = f"订阅{plan['name']}"
            coins = plan["coins"]
            plan_key = request.plan
    elif request.type == "recharge":
        description = f"充值{amount}元"
        coins = int(amount * 100)  # 1元 = 100金币
    elif request.type == "purchase":
        description = "购买商品"
    
    # 生成订单号
    order_no = generate_order_no()
    
    # 创建订单
    order_data = {
        "orderNo": order_no,
        "userId": request.user_id,
        "type": request.type,
        "amount": amount,
        "coins": coins,
        "status": OrderStatus.PENDING.value,
        "description": description,
        "paymentMethod": request.payment_method,
    }
    
    if plan_key:
        order_data["plan"] = plan_key
    if request.product_id:
        order_data["productId"] = request.product_id
    
    result = await parse_client.create_object("Order", order_data)
    order_id = result.get("objectId")
    
    logger.info(f"[创建订单] 订单创建成功: {order_id}, 类型: {request.type}, 金额: {amount}")
    
    return {
        "success": True,
        "order_id": order_id,
        "order_no": order_no,
        "amount": amount,
        "coins": coins,
        "status": OrderStatus.PENDING.value,
        "description": description,
        # Web3支付信息
        "web3_payment": {
            "to_address": Account.from_key(settings.web3_private_key).address if settings.web3_private_key else "",  # 从私钥派生运营账户地址
            "amount_eth": amount,  # 转账金额（ETH）
        }
    }


# ============ 订阅计划 ============

@router.get("/plans")
async def get_subscription_plans():
    """获取订阅计划列表"""
    return {"plans": [{"id": key, **plan} for key, plan in SUBSCRIPTION_PLANS.items()]}


# ============ 订单查询 ============

@router.get("/order/{order_id}")
async def get_order(order_id: str):
    """查询订单详情"""
    try:
        order = await parse_client.get_object("Order", order_id)
    except Exception:
        raise HTTPException(status_code=404, detail="订单不存在")
    
    return {
        "success": True,
        "order_id": order["objectId"],
        "order_no": order.get("orderNo"),
        "type": order.get("type"),
        "amount": order.get("amount"),
        "status": order.get("status"),
        "product_id": order.get("productId"),
        "product_name": order.get("productName"),
        "buyer_address": order.get("buyerAddress"),
        "seller_address": order.get("sellerAddress"),
        "tx_hash": order.get("txHash"),
        "created_at": order.get("createdAt"),
        "paid_at": order.get("paidAt"),
        "completed_at": order.get("completedAt"),
    }


@router.get("/order/{order_id}/status")
async def get_order_status(order_id: str):
    """查询订单状态"""
    try:
        order = await parse_client.get_object("Order", order_id)
    except Exception:
        raise HTTPException(status_code=404, detail="订单不存在")
    
    return {
        "success": True,
        "order_id": order_id,
        "status": order.get("status"),
        "tx_hash": order.get("txHash")
    }


@router.get("/orders")
async def get_user_orders(
    page: int = 1,
    limit: int = 20,
    status: Optional[str] = None,
    user_id: str = Depends(get_current_user_id)
):
    """获取用户订单列表"""
    where = {"userId": user_id}
    if status:
        where["status"] = status
    
    skip = (page - 1) * limit
    result = await parse_client.query_objects("Order", where=where, order="-createdAt", limit=limit, skip=skip)
    total = await parse_client.count_objects("Order", where)
    
    return {"data": result.get("results", []), "total": total, "page": page, "limit": limit}


# ============ 订单操作 ============

@router.post("/order/{order_id}/cancel")
async def cancel_order(order_id: str):
    """取消订单"""
    try:
        order = await parse_client.get_object("Order", order_id)
    except Exception:
        raise HTTPException(status_code=404, detail="订单不存在")
    
    if order.get("status") != OrderStatus.PENDING:
        raise HTTPException(status_code=400, detail="只有待支付订单可以取消")
    
    await parse_client.update_object("Order", order_id, {"status": OrderStatus.CANCELLED})
    return {"success": True, "message": "订单已取消"}


# ============ Web3转账验证（核心） ============

async def _verify_tx_status(tx_hash: str, buyer_address: str, seller_address: str, amount: int) -> dict:
    """查询交易状态"""
    return await web3_client.verify_transfer(
        tx_hash=tx_hash,
        from_address=buyer_address,
        to_address=seller_address,
        amount=amount
    )


async def _poll_tx_until_confirmed(tx_hash: str, buyer_address: str, seller_address: str, amount: int, max_retries: int = 3, interval: int = 10) -> dict:
    """
    轮询交易状态直到确认
    
    Args:
        tx_hash: 交易hash
        max_retries: 最大重试次数（默认3次）
        interval: 每次等待秒数（默认10秒）
    
    Returns:
        最终的验证结果
    """
    for i in range(max_retries):
        logger.info(f"[轮询txHash] 第{i+1}次查询: {tx_hash[:16]}...")
        
        verify_result = await _verify_tx_status(tx_hash, buyer_address, seller_address, amount)
        tx_status = verify_result.get("tx_status", "error")
        
        if tx_status == "confirmed":
            logger.info(f"[轮询txHash] 交易已确认: {tx_hash[:16]}...")
            return verify_result
        
        if tx_status == "failed":
            logger.warning(f"[轮询txHash] 交易失败: {tx_hash[:16]}...")
            return verify_result
        
        if tx_status == "not_found":
            logger.warning(f"[轮询txHash] 交易不存在: {tx_hash[:16]}...")
            return verify_result
        
        # pending 状态，等待后继续轮询
        if i < max_retries - 1:
            logger.info(f"[轮询txHash] 交易待确认，等待{interval}秒后重试...")
            await asyncio.sleep(interval)
    
    # 轮询结束仍未确认，返回最后一次结果
    logger.info(f"[轮询txHash] 轮询结束，交易仍待确认: {tx_hash[:16]}...")
    return verify_result


@router.post("/verify-transfer")
async def verify_web3_transfer(request: VerifyTransferRequest):
    """
    验证Web3转账并更新订单状态
    
    状态流转：
    - txHash 待确认 → 订单状态: paid（支付中），然后轮询3次每次10秒
    - txHash 已确认 → 更新商品所有权，成功后订单状态: completed
    - txHash 失败 → 订单状态: payment_failed
    """
    logger.info(f"[验证转账] 开始验证订单: {request.order_id}, txHash: {request.tx_hash[:16]}...")
    
    # 1. 查询订单
    try:
        order = await parse_client.get_object("Order", request.order_id)
    except Exception:
        raise HTTPException(status_code=404, detail="订单不存在")
    
    if order.get("status") == "completed":
        return {"success": True, "message": "订单已完成", "order_status": "completed", "tx_status": "confirmed"}
    
    if order.get("status") not in ["pending", "paid"]:
        raise HTTPException(status_code=400, detail="订单状态异常")
    
    buyer_address = order.get("buyerAddress")
    seller_address = order.get("sellerAddress")
    amount = int(order.get("amount", 0))
    product_id = order.get("productId")
    
    # 2. 首次查询交易状态
    verify_result = await _verify_tx_status(request.tx_hash, buyer_address, seller_address, amount)
    tx_status = verify_result.get("tx_status", "error")
    
    # 3. 交易不存在
    if tx_status == "not_found":
        raise HTTPException(status_code=400, detail="交易不存在，请检查txHash")
    
    # 4. 交易失败 → 更新为支付失败
    if tx_status == "failed":
        await parse_client.update_object("Order", request.order_id, {
            "txHash": request.tx_hash,
            "status": "payment_failed"
        })
        logger.warning(f"[验证转账] 交易失败，订单更新为支付失败: {request.order_id}")
        return {
            "success": False,
            "message": "交易执行失败",
            "order_id": request.order_id,
            "order_status": "payment_failed",
            "tx_status": "failed",
            "tx_hash": request.tx_hash
        }
    
    # 5. 交易待确认 → 先更新为支付中，然后轮询
    if tx_status == "pending":
        # 更新订单状态为支付中
        await parse_client.update_object("Order", request.order_id, {
            "txHash": request.tx_hash,
            "status": "paid",
            "paidAt": datetime.now().isoformat()
        })
        logger.info(f"[验证转账] 交易待确认，订单更新为支付中，开始轮询: {request.order_id}")
        
        # 轮询3次，每次10秒
        verify_result = await _poll_tx_until_confirmed(
            request.tx_hash, buyer_address, seller_address, amount,
            max_retries=3, interval=10
        )
        tx_status = verify_result.get("tx_status", "error")
    
    # 6. 轮询后仍是 pending，返回支付中状态
    if tx_status == "pending":
        return {
            "success": True,
            "message": "交易待确认，请稍后查看订单状态",
            "order_id": request.order_id,
            "order_status": "paid",
            "tx_status": "pending",
            "tx_hash": request.tx_hash
        }
    
    # 7. 轮询后变为 failed
    if tx_status == "failed":
        await parse_client.update_object("Order", request.order_id, {"status": "payment_failed"})
        return {
            "success": False,
            "message": "交易执行失败",
            "order_id": request.order_id,
            "order_status": "payment_failed",
            "tx_status": "failed",
            "tx_hash": request.tx_hash
        }
    
    # 8. 交易已确认但验证失败（地址或金额不匹配）
    if not verify_result.get("verified"):
        await parse_client.update_object("Order", request.order_id, {
            "status": "payment_failed",
            "failReason": verify_result.get("error", "转账验证失败")
        })
        return {
            "success": False,
            "message": verify_result.get("error", "转账验证失败"),
            "order_id": request.order_id,
            "order_status": "payment_failed",
            "tx_status": "confirmed",
            "tx_hash": request.tx_hash
        }
    
    # 9. 交易已确认且验证通过 → 更新商品所有权
    logger.info(f"[验证转账] 交易已确认，开始转移商品所有权: {request.order_id}")
    
    ownership_transferred = False
    if product_id:
        try:
            await parse_client.update_object("Product", product_id, {
                "owner": buyer_address,
                "sales": {"__op": "Increment", "amount": 1}
            })
            ownership_transferred = True
            logger.info(f"[验证转账] 商品所有权转移成功: {product_id} -> {buyer_address}")
        except Exception as e:
            logger.error(f"[验证转账] 商品所有权转移失败: {e}")
    else:
        ownership_transferred = True  # 无商品时视为成功
    
    # 10. 更新订单状态为已完成
    if ownership_transferred:
        await parse_client.update_object("Order", request.order_id, {
            "txHash": request.tx_hash,
            "status": "completed",
            "completedAt": datetime.now().isoformat()
        })
        logger.info(f"[验证转账] 订单已完成: {request.order_id}, txHash: {request.tx_hash[:16]}...")
        
        # 11. 发放充值奖励（如果是购买订单）
        try:
            user_id = order.get("userId")
            order_amount = float(order.get("amount", 0))
            if user_id and order_amount > 0:
                reward_result = await incentive_service.grant_recharge_reward(
                    user_id=user_id,
                    recharge_amount=order_amount,
                    order_id=request.order_id
                )
                if reward_result.get("success"):
                    logger.info(f"[验证转账] 充值奖励已发放: {reward_result.get('amount')} 金币")
                else:
                    logger.warning(f"[验证转账] 充值奖励发放失败: {reward_result.get('error')}")
        except Exception as e:
            logger.error(f"[验证转账] 发放充值奖励异常: {e}")
        
        return {
            "success": True,
            "message": "订单已完成",
            "order_id": request.order_id,
            "order_status": "completed",
            "tx_status": "confirmed",
            "tx_hash": request.tx_hash
        }
    else:
        # 所有权转移失败，保持 paid 状态，等待手动处理
        return {
            "success": False,
            "message": "商品所有权转移失败，请联系客服",
            "order_id": request.order_id,
            "order_status": "paid",
            "tx_status": "confirmed",
            "tx_hash": request.tx_hash
        }


# ============ 模拟支付（仅测试环境） ============

@router.post("/order/{order_id}/mock-pay")
async def mock_pay_order(order_id: str):
    """
    模拟支付成功（仅测试环境可用）
    跳过真实链上验证，直接完成订单
    """
    if not getattr(settings, 'debug', False) and not getattr(settings, 'wechat_test_mode', False):
        raise HTTPException(status_code=403, detail="仅测试环境可用")
    
    try:
        order = await parse_client.get_object("Order", order_id)
    except Exception:
        raise HTTPException(status_code=404, detail="订单不存在")
    
    if order.get("status") not in ["pending", "paid"]:
        raise HTTPException(status_code=400, detail="订单状态异常")
    
    product_id = order.get("productId")
    buyer_address = order.get("buyerAddress")
    amount = order.get("amount", 0)
    mock_tx_hash = f"mock_tx_{order_id}_{datetime.now().timestamp()}"
    
    # 更新订单状态
    await parse_client.update_object("Order", order_id, {
        "txHash": mock_tx_hash,
        "status": "completed",
        "completedAt": datetime.now().isoformat()
    })
    
    # 更新商品owner
    if product_id:
        await parse_client.update_object("Product", product_id, {
            "owner": buyer_address,
            "sales": {"__op": "Increment", "amount": 1}
        })
    
    # 创建交易记录
    await parse_client.create_object("Transaction", {
        "userId": order.get("userId"),
        "type": "consume",
        "amount": -amount,
        "description": f"购买商品: {order.get('productName')}",
        "status": "completed",
        "txHash": mock_tx_hash
    })
    
    return {"success": True, "message": "模拟支付成功", "order_id": order_id, "status": "completed"}


# ============ 订阅处理（已迁移到 member.py） ============

# 注意: 订阅处理逻辑已迁移到 member.py
# 请使用 /api/v1/member/subscribe 接口


# ============ 后台协程：处理支付中订单 ============

_background_task_running = False

async def process_pending_paid_orders():
    """
    处理处于支付中(paid)状态的订单
    循环查询并验证订单的 txHash 状态
    """
    logger.info("[后台任务] 开始处理支付中订单...")
    
    try:
        # 查询所有支付中的订单
        result = await parse_client.query_objects(
            "Order",
            where={"status": "paid"},
            order="-createdAt",
            limit=100
        )
        orders = result.get("results", [])
        
        if not orders:
            logger.info("[后台任务] 无支付中订单")
            return
        
        logger.info(f"[后台任务] 找到 {len(orders)} 个支付中订单")
        
        for order in orders:
            order_id = order.get("objectId")
            tx_hash = order.get("txHash")
            
            if not tx_hash:
                logger.warning(f"[后台任务] 订单 {order_id} 无 txHash，跳过")
                continue
            
            try:
                logger.info(f"[后台任务] 处理订单: {order_id}")
                
                # 调用 verify-transfer 逻辑
                buyer_address = order.get("buyerAddress")
                seller_address = order.get("sellerAddress")
                amount = int(order.get("amount", 0))
                product_id = order.get("productId")
                
                # 查询交易状态
                verify_result = await _verify_tx_status(tx_hash, buyer_address, seller_address, amount)
                tx_status = verify_result.get("tx_status", "error")
                
                if tx_status == "confirmed" and verify_result.get("verified"):
                    # 交易已确认，更新商品所有权和订单状态
                    if product_id:
                        try:
                            await parse_client.update_object("Product", product_id, {
                                "owner": buyer_address,
                                "sales": {"__op": "Increment", "amount": 1}
                            })
                        except Exception as e:
                            logger.error(f"[后台任务] 更新商品所有权失败: {e}")
                            continue
                    
                    await parse_client.update_object("Order", order_id, {
                        "status": "completed",
                        "completedAt": datetime.now().isoformat()
                    })
                    logger.info(f"[后台任务] 订单已完成: {order_id}")
                
                elif tx_status == "failed":
                    # 交易失败
                    await parse_client.update_object("Order", order_id, {
                        "status": "payment_failed"
                    })
                    logger.warning(f"[后台任务] 订单支付失败: {order_id}")
                
                elif tx_status == "confirmed" and not verify_result.get("verified"):
                    # 交易确认但验证失败
                    await parse_client.update_object("Order", order_id, {
                        "status": "payment_failed",
                        "failReason": verify_result.get("error", "转账验证失败")
                    })
                    logger.warning(f"[后台任务] 订单验证失败: {order_id}")
                
                # pending 状态保持不变，等待下次处理
                
            except Exception as e:
                logger.error(f"[后台任务] 处理订单 {order_id} 失败: {e}")
                continue
    
    except Exception as e:
        logger.error(f"[后台任务] 查询订单失败: {e}")


async def background_order_processor(interval: int = 60):
    """
    后台订单处理器（长期运行的协程）
    
    Args:
        interval: 每次处理间隔（秒），默认60秒
    """
    global _background_task_running
    
    if _background_task_running:
        logger.warning("[后台任务] 已经在运行，跳过")
        return
    
    _background_task_running = True
    logger.info(f"[后台任务] 启动订单处理器，间隔: {interval}秒")
    
    try:
        while True:
            await process_pending_paid_orders()
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("[后台任务] 订单处理器已停止")
    finally:
        _background_task_running = False


def start_background_order_processor():
    """启动后台订单处理器"""
    asyncio.create_task(background_order_processor(interval=300))  # 5分钟间隔
    logger.info("[后台任务] 订单处理器已加入任务队列")
