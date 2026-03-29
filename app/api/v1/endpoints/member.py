"""
会员订阅接口
"""
import uuid
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel

from app.core.logger import logger
from app.core.parse_client import parse_client
from app.core.wechat_pay import wechat_pay, MEMBER_PLANS
from app.core.incentive_service import incentive_service

router = APIRouter()


# ============ 请求/响应模型 ============

class SubscribeRequest(BaseModel):
    """订阅请求"""
    user_id: str
    plan_id: str  # 套餐ID: vip_month, vip_year, svip_month 等
    openid: Optional[str] = None  # 微信openid（JSAPI支付需要）
    session_token: Optional[str] = None  # Parse session token，用于更新用户信息


class SubscribeResponse(BaseModel):
    """订阅响应"""
    success: bool
    order_id: Optional[str] = None
    pay_params: Optional[dict] = None  # 前端调起支付的参数
    message: Optional[str] = None


class SimulatePayRequest(BaseModel):
    """模拟支付请求（测试模式）"""
    order_id: str
    session_token: Optional[str] = None  # 用于更新用户信息


class MemberStatusResponse(BaseModel):
    """会员状态响应"""
    member_level: str  # normal, vip, svip
    member_expire_at: Optional[str] = None
    is_expired: bool = False


class PlanInfo(BaseModel):
    """套餐信息"""
    plan_id: str
    name: str
    level: str
    days: int
    price: float
    original_price: float
    discount: int  # 折扣百分比，如 90 表示 9 折
    bonus: int


# ============ 接口 ============

@router.get("/plans", response_model=list[PlanInfo])
async def get_member_plans():
    """获取会员套餐列表"""
    plans = []
    for plan_id, plan in MEMBER_PLANS.items():
        plans.append(PlanInfo(
            plan_id=plan_id,
            name=plan["name"],
            level=plan["level"],
            days=plan["days"],
            price=plan["price"],
            original_price=plan.get("original_price", plan["price"]),
            discount=plan.get("discount", 100),
            bonus=plan["bonus"],
        ))
    return plans


@router.post("/subscribe", response_model=SubscribeResponse)
async def subscribe_member(request: SubscribeRequest):
    """
    创建会员订阅订单
    
    流程:
    1. 验证套餐
    2. 检查是否允许购买（禁止降级）
    3. 创建订单记录
    4. 调用微信支付
    5. 返回支付参数
    """
    # 1. 验证套餐
    plan = MEMBER_PLANS.get(request.plan_id)
    if not plan:
        raise HTTPException(status_code=400, detail="无效的套餐ID")
    
    # 2. 通过 session token 验证用户
    logger.info(f"[会员订阅] user_id={request.user_id}, session_token={request.session_token[:20] if request.session_token else 'None'}...")
    
    if not request.session_token:
        raise HTTPException(status_code=401, detail="未提供会话令牌")
    
    try:
        user = await parse_client.get_current_user(request.session_token)
        logger.info(f"[会员订阅] 获取用户成功: {user.get('objectId')}")
        if user.get("objectId") != request.user_id:
            raise HTTPException(status_code=403, detail="用户身份不匹配")
    except Exception as e:
        logger.error(f"[会员订阅] 验证用户失败: {e}")
        raise HTTPException(status_code=401, detail="会话已过期，请重新登录")
    
    # 3. 检查是否允许购买（禁止活跃会员降级购买）
    current_level = user.get("memberLevel", "normal")
    current_expire = user.get("memberExpireAt")
    new_level = plan.get("level", "vip")
    
    # 等级优先级: svip > vip > normal
    LEVEL_PRIORITY = {"normal": 0, "vip": 1, "svip": 2}
    current_priority = LEVEL_PRIORITY.get(current_level, 0)
    new_priority = LEVEL_PRIORITY.get(new_level, 0)
    
    # 检查当前会员是否有效
    is_active = False
    if current_expire:
        try:
            expire_dt = datetime.fromisoformat(current_expire.replace("Z", "+00:00"))
            if expire_dt.tzinfo:
                expire_dt = expire_dt.replace(tzinfo=None)
            is_active = expire_dt > datetime.now()
        except Exception as e:
            logger.warning(f"[会员订阅] 解析到期时间失败: {e}")
    
    # 禁止降级购买
    if is_active and new_priority < current_priority:
        logger.warning(f"[会员订阅] 禁止降级: 当前{current_level}未过期，不能购买{new_level}")
        return SubscribeResponse(
            success=False,
            message=f"您当前是{current_level.upper()}会员，有效期至{expire_dt.date()}，请等到期后再购买{new_level.upper()}"
        )
    
    # 4. 创建订单
    order_id = f"MO{datetime.now().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:8]}"
    order_data = {
        "orderId": order_id,
        "userId": request.user_id,
        "planId": request.plan_id,
        "planName": plan["name"],
        "level": plan["level"],
        "days": plan["days"],
        "amount": plan["price"],
        "bonus": plan["bonus"],
        "status": "pending",  # pending, paid, failed, cancelled
    }
    
    try:
        await parse_client.create_object("MemberOrder", order_data)
        logger.info(f"[会员订阅] 创建订单成功: {order_id}")
    except Exception as e:
        logger.error(f"[会员订阅] 创建订单失败: {e}")
        raise HTTPException(status_code=500, detail="创建订单失败")
    
    # 5. 创建微信支付
    total_fee = int(plan["price"] * 100)  # 转为分
    pay_result = await wechat_pay.create_order(
        out_trade_no=order_id,
        total_fee=total_fee,
        body=plan["name"],
        openid=request.openid or "",
        trade_type="NATIVE",  # 扫码支付
    )
    
    if not pay_result.get("success"):
        # 更新订单状态
        await parse_client.query_and_update(
            "MemberOrder",
            {"orderId": order_id},
            {"status": "failed", "failReason": pay_result.get("error")},
        )
        return SubscribeResponse(
            success=False,
            message=pay_result.get("error", "支付创建失败"),
        )
    
    # 6. 更新订单支付信息
    await parse_client.query_and_update(
        "MemberOrder",
        {"orderId": order_id},
        {
            "prepayId": pay_result.get("prepay_id"),
            "codeUrl": pay_result.get("code_url"),
        },
    )
    
    return SubscribeResponse(
        success=True,
        order_id=order_id,
        pay_params={
            "prepay_id": pay_result.get("prepay_id"),
            "code_url": pay_result.get("code_url"),  # 扫码支付的二维码内容
            "test_mode": pay_result.get("test_mode", False),
        },
    )


@router.post("/simulate-pay", response_model=SubscribeResponse)
async def simulate_pay(request: SimulatePayRequest):
    """
    模拟支付成功（仅测试模式可用）
    
    用于开发测试，模拟支付成功后的处理流程
    """
    if not wechat_pay.test_mode:
        raise HTTPException(status_code=403, detail="非测试模式不可用")
    
    # 查询订单
    orders = await parse_client.query("MemberOrder", {"orderId": request.order_id})
    if not orders:
        raise HTTPException(status_code=404, detail="订单不存在")
    
    order = orders[0]
    if order.get("status") == "paid":
        return SubscribeResponse(success=True, message="订单已支付")
    
    # 执行订单完成逻辑（传递 session_token）
    result = await complete_member_order(request.order_id, order, request.session_token)
    return result


@router.post("/callback/wechat")
async def wechat_callback(request_body: str):
    """
    微信支付回调
    
    微信服务器通知支付结果
    """
    # 验证回调
    verify_result = wechat_pay.verify_callback(request_body)
    if not verify_result.get("success"):
        return "<xml><return_code>FAIL</return_code><return_msg>签名失败</return_msg></xml>"
    
    data = verify_result.get("data", {})
    out_trade_no = data.get("out_trade_no")
    result_code = data.get("result_code")
    
    if result_code != "SUCCESS":
        logger.warning(f"[微信回调] 支付失败: {out_trade_no}")
        return "<xml><return_code>SUCCESS</return_code></xml>"
    
    # 查询订单
    orders = await parse_client.query("MemberOrder", {"orderId": out_trade_no})
    if not orders:
        logger.error(f"[微信回调] 订单不存在: {out_trade_no}")
        return "<xml><return_code>SUCCESS</return_code></xml>"
    
    order = orders[0]
    if order.get("status") == "paid":
        return "<xml><return_code>SUCCESS</return_code></xml>"
    
    # 完成订单
    await complete_member_order(out_trade_no, order)
    
    return "<xml><return_code>SUCCESS</return_code></xml>"


@router.get("/status/{user_id}", response_model=MemberStatusResponse)
async def get_member_status(
    user_id: str,
    x_parse_session_token: Optional[str] = Header(None, alias="X-Parse-Session-Token")
):
    """获取用户会员状态"""
    if not x_parse_session_token:
        raise HTTPException(status_code=401, detail="未提供会话令牌")
    
    try:
        user = await parse_client.get_current_user(x_parse_session_token)
        if user.get("objectId") != user_id:
            raise HTTPException(status_code=403, detail="用户身份不匹配")
    except Exception as e:
        logger.error(f"[会员状态] 验证用户失败: {e}")
        raise HTTPException(status_code=401, detail="会话已过期，请重新登录")
    
    member_level = user.get("memberLevel", "normal")
    member_expire_at = user.get("memberExpireAt")
    
    is_expired = False
    if member_expire_at:
        expire_dt = datetime.fromisoformat(member_expire_at.replace("Z", "+00:00"))
        is_expired = expire_dt < datetime.now(expire_dt.tzinfo)
        # 会员过期后自动降级为普通用户
        if is_expired:
            member_level = "normal"
    
    return MemberStatusResponse(
        member_level=member_level,
        member_expire_at=member_expire_at,
        is_expired=is_expired,
    )


@router.get("/orders/{user_id}")
async def get_member_orders(
    user_id: str, 
    limit: int = 20, 
    skip: int = 0,
    x_parse_session_token: Optional[str] = Header(None, alias="X-Parse-Session-Token")
):
    """获取用户会员订单列表"""
    # 验证用户身份
    if not x_parse_session_token:
        raise HTTPException(status_code=401, detail="未提供会话令牌")
    
    try:
        user = await parse_client.get_current_user(x_parse_session_token)
        if user.get("objectId") != user_id:
            raise HTTPException(status_code=403, detail="用户身份不匹配")
    except Exception as e:
        logger.error(f"[会员订单] 验证用户失败: {e}")
        raise HTTPException(status_code=401, detail="会话已过期，请重新登录")
    
    result = await parse_client.query_objects(
        "MemberOrder",
        where={"userId": user_id},
        order="-createdAt",
        limit=limit,
        skip=skip,
    )
    orders = result.get("results", [])
    return {"orders": orders, "total": len(orders)}


@router.get("/order-status/{order_id}")
async def get_order_status(
    order_id: str,
    x_parse_session_token: Optional[str] = Header(None, alias="X-Parse-Session-Token")
):
    """
    查询订单支付状态（用于支付后轮询）
    
    真实支付流程中，前端展示二维码后需要轮询此接口检查支付结果
    """
    if not x_parse_session_token:
        raise HTTPException(status_code=401, detail="未提供会话令牌")
    
    try:
        user = await parse_client.get_current_user(x_parse_session_token)
    except Exception as e:
        logger.error(f"[订单状态] 验证用户失败: {e}")
        raise HTTPException(status_code=401, detail="会话已过期，请重新登录")
    
    # 查询订单
    result = await parse_client.query_objects(
        "MemberOrder",
        where={"orderId": order_id},
        limit=1,
    )
    orders = result.get("results", [])
    
    if not orders:
        raise HTTPException(status_code=404, detail="订单不存在")
    
    order = orders[0]
    
    # 验证订单属于当前用户
    if order.get("userId") != user.get("objectId"):
        raise HTTPException(status_code=403, detail="无权访问此订单")
    
    # 如果订单状态是 pending，调用微信支付查询接口确认真实状态
    if order.get("status") == "pending":
        try:
            pay_result = await wechat_pay.query_order(order_id)
            if pay_result.get("trade_state") == "SUCCESS":
                # 支付成功，更新订单并完成订阅
                await parse_client.query_and_update(
                    "MemberOrder",
                    {"orderId": order_id},
                    {"status": "paid", "paidAt": datetime.now().isoformat()},
                )
                # 完成会员订阅
                await complete_member_order(order_id, order)
                return {
                    "order_id": order_id,
                    "status": "paid",
                    "paid_at": datetime.now().isoformat(),
                }
        except Exception as e:
            logger.error(f"[订单状态] 查询微信支付失败: {e}")
    
    return {
        "order_id": order_id,
        "status": order.get("status", "pending"),
        "paid_at": order.get("paidAt"),
    }


# ============ 内部函数 ============

async def complete_member_order(order_id: str, order: dict, session_token: Optional[str] = None) -> SubscribeResponse:
    """
    完成会员订单（幂等）
    
    1. 检查订单是否已处理（防止重复发放）
    2. 更新订单状态
    3. 更新用户会员等级和到期时间
    4. 发放积分奖励
    
    Args:
        order_id: 订单ID
        order: 订单数据
        session_token: Parse session token，用于更新用户信息
    """
    user_id = order.get("userId")
    plan_id = order.get("planId")
    plan = MEMBER_PLANS.get(plan_id, {})
    
    logger.info(f"[会员订单] 开始处理: order_id={order_id}, user_id={user_id}, plan_id={plan_id}, has_session={bool(session_token)}")
    
    # 0. 幂等性检查：重新查询订单状态，防止并发重复处理
    try:
        fresh_result = await parse_client.query_objects(
            "MemberOrder",
            where={"orderId": order_id},
            limit=1,
        )
        fresh_orders = fresh_result.get("results", [])
        if fresh_orders and fresh_orders[0].get("status") == "paid":
            logger.info(f"[会员订单] 订单已处理，跳过: {order_id}")
            return SubscribeResponse(success=True, order_id=order_id, message="订单已处理")
    except Exception as e:
        logger.warning(f"[会员订单] 幂等性检查失败，继续处理: {e}")
    try:
        await parse_client.query_and_update(
            "MemberOrder",
            {"orderId": order_id},
            {
                "status": "paid",
                "paidAt": datetime.now().isoformat(),
            },
        )
        logger.info(f"[会员订单] 订单状态已更新为 paid")
    except Exception as e:
        logger.error(f"[会员订单] 更新订单状态失败: {e}")
        return SubscribeResponse(success=False, message="更新订单失败")
    
    # 2. 获取用户当前状态
    try:
        if session_token:
            user = await parse_client.get_current_user(session_token)
            logger.info(f"[会员订单] 通过session获取用户成功: {user.get('username')}")
        else:
            logger.warning(f"[会员订单] 无session_token，尝试直接获取用户")
            user = await parse_client.get_user(user_id)
    except Exception as e:
        logger.error(f"[会员订单] 获取用户失败: {e}")
        return SubscribeResponse(success=False, message="用户不存在")
    if not user:
        logger.error(f"[会员订阅] 用户不存在: {user_id}")
        return SubscribeResponse(success=False, message="用户不存在")
    
    # 3. 计算新的到期时间（简化方案）
    # 规则：
    # - 同等级续费：时间累加
    # - 升级(VIP→SVIP)：折算剩余时间 + 新购时间
    # - 降级(SVIP→VIP)：当前会员未过期时，禁止降级购买
    # - 首次开通/已过期：直接购买
    
    current_expire = user.get("memberExpireAt")
    current_level = user.get("memberLevel", "normal")
    new_level = plan.get("level", "vip")
    days = plan.get("days", 30)
    
    # 等级优先级: svip > vip > normal
    LEVEL_PRIORITY = {"normal": 0, "vip": 1, "svip": 2}
    current_priority = LEVEL_PRIORITY.get(current_level, 0)
    new_priority = LEVEL_PRIORITY.get(new_level, 0)
    
    now = datetime.now()
    is_active = False  # 当前会员是否有效
    
    if current_expire:
        expire_dt = datetime.fromisoformat(current_expire.replace("Z", "+00:00"))
        if expire_dt.tzinfo:
            expire_dt = expire_dt.replace(tzinfo=None)
        is_active = expire_dt > now
    
    # 检查是否为降级购买
    if is_active and new_priority < current_priority:
        # 高等级会员未过期，禁止购买低等级
        logger.warning(f"[会员订阅] 禁止降级: 当前{current_level}未过期({expire_dt.date()})，不能购买{new_level}")
        return SubscribeResponse(
            success=False, 
            message=f"您当前是{current_level.upper()}会员，有效期至{expire_dt.date()}，请等到期后再购买{new_level.upper()}"
        )
    
    # 计算新到期时间
    if is_active:
        remaining_days = (expire_dt - now).days
        
        if current_level == new_level:
            # 同等级续费，时间直接累加
            new_expire = expire_dt + timedelta(days=days)
            logger.info(f"[会员订阅] 同等级续费: 剩余{remaining_days}天 + 新购{days}天")
        else:
            # 升级：折算剩余时间 + 新购时间
            # VIP→SVIP: VIP剩余时间按1:2折算（因为SVIP是VIP 2倍价格）
            # 例: 30天VIP = 15天SVIP
            PRICE_RATIO = {"vip": 1.0, "svip": 2.0}
            current_ratio = PRICE_RATIO.get(current_level, 1.0)
            new_ratio = PRICE_RATIO.get(new_level, 1.0)
            converted_days = int(remaining_days * current_ratio / new_ratio)
            new_expire = now + timedelta(days=converted_days + days)
            logger.info(f"[会员订阅] 升级: {current_level}->{new_level}, "
                       f"剩余{remaining_days}天折算为{converted_days}天 + 新购{days}天")
    else:
        # 首次开通或已过期，从现在开始
        new_expire = now + timedelta(days=days)
        logger.info(f"[会员订阅] {'首次开通' if not current_expire else '会员已过期'}，{days}天")
    
    # 4. 更新用户会员状态
    update_data = {
        "memberLevel": new_level,
        "memberExpireAt": new_expire.isoformat(),
    }
    
    try:
        if session_token:
            await parse_client.update_user_with_session(user_id, update_data, session_token)
            logger.info(f"[会员订单] 使用session更新用户成功")
        else:
            await parse_client.update_user_with_master_key(user_id, update_data)
            logger.info(f"[会员订单] 使用MasterKey更新用户成功")
    except Exception as e:
        logger.error(f"[会员订单] 更新用户会员状态失败: {e}")
        return SubscribeResponse(success=False, message="更新会员状态失败")
    
    logger.info(f"[会员订阅] 用户 {user_id} 会员等级: {new_level}，到期时间: {new_expire}")
    
    # 5. 发放积分奖励
    bonus = plan.get("bonus", 0)
    if bonus > 0:
        try:
            reward_result = await incentive_service.grant_member_subscribe_reward(
                user_id=user_id,
                plan_name=plan.get('name', plan_id),
                member_level=new_level,
                order_id=order_id,
                bonus=bonus,
            )
            if reward_result.get("success"):
                logger.info(f"[会员订阅] 发放积分奖励成功: {user_id}, {bonus}积分")
            else:
                logger.warning(f"[会员订阅] 发放积分奖励失败: {user_id}, 原因: {reward_result.get('error')}")
        except Exception as e:
            logger.error(f"[会员订阅] 发放积分异常: {e}")
    
    return SubscribeResponse(
        success=True,
        order_id=order_id,
        message=f"订阅成功，已升级为{new_level}会员",
    )
