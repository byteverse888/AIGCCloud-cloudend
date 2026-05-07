"""
管理端统计接口 & 系统设置接口
"""
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.core.deps import get_admin_user_id
from app.core.parse_client import parse_client
from app.core.logger import logger

router = APIRouter()


def _today_start() -> str:
    """今天 UTC 0点 ISO 格式"""
    now = datetime.now(timezone.utc)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return start.isoformat()


def _month_start(offset: int = 0) -> str:
    """本月/上月 UTC 1号 0点"""
    now = datetime.now(timezone.utc)
    month = now.month + offset
    year = now.year
    while month <= 0:
        month += 12
        year -= 1
    while month > 12:
        month -= 12
        year += 1
    start = now.replace(year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0)
    return start.isoformat()


def _week_start() -> str:
    """本周一 UTC 0点"""
    now = datetime.now(timezone.utc)
    monday = now - timedelta(days=now.weekday())
    start = monday.replace(hour=0, minute=0, second=0, microsecond=0)
    return start.isoformat()


# ==================== 用户统计 ====================

@router.get("/stats/users")
async def stats_users(admin_id: str = Depends(get_admin_user_id)):
    """用户统计：今日新增、总数、角色分布"""
    today = _today_start()

    total, today_new = await asyncio.gather(
        parse_client.count_objects("_User"),
        parse_client.count_objects("_User", {"createdAt": {"$gte": {"__type": "Date", "iso": today}}}),
    )

    # 角色分布
    roles = ["user", "operator", "channel", "admin"]
    role_counts = await asyncio.gather(
        *[parse_client.count_objects("_User", {"role": r}) for r in roles]
    )
    role_distribution = {r: c for r, c in zip(roles, role_counts)}

    return {
        "total_users": total,
        "today_new": today_new,
        "role_distribution": role_distribution,
    }


# ==================== 订单统计 ====================

@router.get("/stats/orders")
async def stats_orders(admin_id: str = Depends(get_admin_user_id)):
    """订单统计：今日订单、总数、各状态数、本周趋势、客单价"""
    today = _today_start()
    week_start = _week_start()

    statuses = ["pending", "paid", "completed", "cancelled", "payment_failed"]

    total, today_count, *status_counts = await asyncio.gather(
        parse_client.count_objects("Order"),
        parse_client.count_objects("Order", {"createdAt": {"$gte": {"__type": "Date", "iso": today}}}),
        *[parse_client.count_objects("Order", {"status": s}) for s in statuses],
    )

    status_distribution = {s: c for s, c in zip(statuses, status_counts)}

    # 本周每日订单（简化：查本周总订单按天分桶太复杂，返回7天各日count）
    now = datetime.now(timezone.utc)
    monday = now - timedelta(days=now.weekday())
    daily_trend = []
    for i in range(7):
        day_start = monday.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=i)
        day_end = day_start + timedelta(days=1)
        if day_start > now:
            daily_trend.append(0)
        else:
            count = await parse_client.count_objects("Order", {
                "createdAt": {
                    "$gte": {"__type": "Date", "iso": day_start.isoformat()},
                    "$lt": {"__type": "Date", "iso": day_end.isoformat()},
                }
            })
            daily_trend.append(count)

    # 客单价（已完成订单平均金额）
    completed_orders = await parse_client.query_objects(
        "Order", where={"status": "completed"}, limit=1000, keys="amount"
    )
    amounts = [o.get("amount", 0) for o in completed_orders.get("results", [])]
    avg_order_value = round(sum(amounts) / len(amounts), 2) if amounts else 0

    return {
        "total": total,
        "today_count": today_count,
        "status_distribution": status_distribution,
        "daily_trend": daily_trend,
        "average_order_value": avg_order_value,
    }


# ==================== 商品统计 ====================

@router.get("/stats/products")
async def stats_products(admin_id: str = Depends(get_admin_user_id)):
    """商品统计：各状态数、分类分布、热销Top5"""
    statuses = ["draft", "pending", "approved", "rejected", "offline"]
    categories = ["image", "audio", "video", "comic", "music", "digital-human", "model", "other"]

    status_counts = await asyncio.gather(
        *[parse_client.count_objects("Product", {"status": s}) for s in statuses]
    )
    status_distribution = {s: c for s, c in zip(statuses, status_counts)}
    total = sum(status_counts)

    # 分类分布
    cat_counts = await asyncio.gather(
        *[parse_client.count_objects("Product", {"category": c}) for c in categories]
    )
    category_distribution = [
        {"category": c, "count": cnt}
        for c, cnt in zip(categories, cat_counts) if cnt > 0
    ]

    # 热销 Top 5
    top_result = await parse_client.query_objects(
        "Product",
        where={"status": "approved"},
        order="-sales",
        limit=5,
        keys="name,category,sales,price"
    )
    top_products = []
    for p in top_result.get("results", []):
        top_products.append({
            "id": p["objectId"],
            "name": p.get("name", ""),
            "category": p.get("category", ""),
            "sales": p.get("sales", 0),
            "revenue": p.get("sales", 0) * p.get("price", 0),
        })

    # 待处理举报数
    pending_reports = await parse_client.count_objects("ProductReport", {"status": "pending"})

    return {
        "total": total,
        "status_distribution": status_distribution,
        "category_distribution": category_distribution,
        "top_products": top_products,
        "pending_reports": pending_reports,
    }


# ==================== 收入统计 ====================

@router.get("/stats/revenue")
async def stats_revenue(admin_id: str = Depends(get_admin_user_id)):
    """收入统计：今日/本月/上月收入、总营收、支付方式分布、本周趋势"""
    today = _today_start()
    this_month = _month_start(0)
    last_month_start = _month_start(-1)

    # 查询已完成订单的金额
    async def sum_revenue(where: dict) -> float:
        result = await parse_client.query_objects("Order", where=where, limit=1000, keys="amount")
        return sum(o.get("amount", 0) for o in result.get("results", []))

    base_where = {"status": "completed"}

    total_revenue, this_month_revenue, last_month_revenue, today_revenue = await asyncio.gather(
        sum_revenue(base_where),
        sum_revenue({**base_where, "completedAt": {"$gte": {"__type": "Date", "iso": this_month}}}),
        sum_revenue({**base_where, "completedAt": {
            "$gte": {"__type": "Date", "iso": last_month_start},
            "$lt": {"__type": "Date", "iso": this_month},
        }}),
        sum_revenue({**base_where, "completedAt": {"$gte": {"__type": "Date", "iso": today}}}),
    )

    # 支付方式分布
    methods = ["web3", "wechat", "alipay"]
    method_revenues = await asyncio.gather(
        *[sum_revenue({**base_where, "paymentMethod": m}) for m in methods]
    )
    total_for_pct = sum(method_revenues) or 1
    payment_methods = [
        {
            "method": m,
            "amount": round(r, 2),
            "percentage": round(r / total_for_pct * 100, 1),
        }
        for m, r in zip(methods, method_revenues) if r > 0
    ]

    # 本周每日收入
    now = datetime.now(timezone.utc)
    monday = now - timedelta(days=now.weekday())
    daily_trend = []
    for i in range(7):
        day_start = monday.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=i)
        day_end = day_start + timedelta(days=1)
        if day_start > now:
            daily_trend.append(0)
        else:
            rev = await sum_revenue({
                **base_where,
                "completedAt": {
                    "$gte": {"__type": "Date", "iso": day_start.isoformat()},
                    "$lt": {"__type": "Date", "iso": day_end.isoformat()},
                }
            })
            daily_trend.append(round(rev, 2))

    return {
        "total_revenue": round(total_revenue, 2),
        "this_month": round(this_month_revenue, 2),
        "last_month": round(last_month_revenue, 2),
        "today": round(today_revenue, 2),
        "payment_methods": payment_methods,
        "daily_trend": daily_trend,
    }


# ==================== 管理员订单列表 ====================

@router.get("/orders")
async def admin_list_orders(
    page: int = 1,
    limit: int = 20,
    status: Optional[str] = None,
    search: Optional[str] = None,
    admin_id: str = Depends(get_admin_user_id),
):
    """管理员查看全部订单（支持状态筛选和订单号搜索）"""
    where: dict = {}
    if status:
        where["status"] = status
    if search:
        where["orderNo"] = {"$regex": search, "$options": "i"}

    skip = (page - 1) * limit
    result = await parse_client.query_objects(
        "Order",
        where=where if where else None,
        order="-createdAt",
        limit=limit,
        skip=skip,
    )
    total = await parse_client.count_objects("Order", where if where else None)

    # 丰富订单数据（附带用户名）
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
            "paidAt": order.get("paidAt"),
            "completedAt": order.get("completedAt"),
        })

    return {
        "data": orders,
        "total": total,
        "page": page,
        "limit": limit,
    }


# ==================== 系统设置 ====================

class SystemSettingsUpdate(BaseModel):
    category: str  # payment, notification, security, general, email, credits
    settings: dict


@router.get("/settings")
async def get_system_settings(admin_id: str = Depends(get_admin_user_id)):
    """获取系统设置"""
    result = await parse_client.query_objects("SystemConfig", limit=100)
    configs = {}
    for item in result.get("results", []):
        configs[item.get("category", "")] = item.get("settings", {})
    return {"success": True, "data": configs}


@router.put("/settings")
async def update_system_settings(
    request: SystemSettingsUpdate,
    admin_id: str = Depends(get_admin_user_id),
):
    """更新系统设置（按类别）"""
    # 查找已有配置
    existing = await parse_client.query_objects(
        "SystemConfig",
        where={"category": request.category},
        limit=1,
    )
    results = existing.get("results", [])

    if results:
        # 更新
        config_id = results[0]["objectId"]
        await parse_client.update_object("SystemConfig", config_id, {
            "settings": request.settings,
            "updatedBy": admin_id,
        })
    else:
        # 新建
        await parse_client.create_object("SystemConfig", {
            "category": request.category,
            "settings": request.settings,
            "updatedBy": admin_id,
        })

    return {"success": True, "category": request.category}


# ==================== 角色管理 ====================

class RoleUpdateRequest(BaseModel):
    permissions: list[str]


class RoleCreateRequest(BaseModel):
    name: str
    label: str
    description: str = ""
    permissions: list[str] = []


@router.get("/roles")
async def list_roles(admin_id: str = Depends(get_admin_user_id)):
    """获取所有角色及其用户数"""
    roles_resp = await parse_client.query_objects("_Role", limit=100)
    roles = roles_resp.get("results", [])

    result = []
    for role in roles:
        # 获取该角色下的用户数
        user_count = 0
        if "users" in role:
            # Parse Server _Role 有 users relation
            try:
                users_resp = await parse_client.query_objects(
                    "_User",
                    where={"$relatedTo": {"object": {"__type": "Pointer", "className": "_Role", "objectId": role["objectId"]}, "key": "users"}},
                    count=1,
                    limit=0,
                )
                user_count = users_resp.get("count", 0)
            except Exception:
                pass

        result.append({
            "objectId": role["objectId"],
            "name": role.get("name", ""),
            "label": role.get("label", role.get("name", "")),
            "description": role.get("description", ""),
            "permissions": role.get("permissions", []),
            "userCount": user_count,
            "createdAt": role.get("createdAt", ""),
        })

    return {"roles": result}


@router.put("/roles/{role_id}")
async def update_role(
    role_id: str,
    request: RoleUpdateRequest,
    admin_id: str = Depends(get_admin_user_id),
):
    """更新角色权限"""
    await parse_client.update_object("_Role", role_id, {
        "permissions": request.permissions,
    })
    return {"success": True}


@router.post("/roles")
async def create_role(
    request: RoleCreateRequest,
    admin_id: str = Depends(get_admin_user_id),
):
    """创建新角色"""
    role = await parse_client.create_object("_Role", {
        "name": request.name,
        "label": request.label,
        "description": request.description,
        "permissions": request.permissions,
        "ACL": {"*": {"read": True}},
    })
    return {"success": True, "objectId": role.get("objectId")}


@router.delete("/roles/{role_id}")
async def delete_role(
    role_id: str,
    admin_id: str = Depends(get_admin_user_id),
):
    """删除角色"""
    await parse_client.delete_object("_Role", role_id)
    return {"success": True}


# ==================== 券码管理 ====================

class CouponCreateRequest(BaseModel):
    code: str
    type: str  # fixed, percent, threshold
    value: float
    min_amount: Optional[float] = None
    scope: str = "all"  # all, category, product
    scope_detail: Optional[str] = None
    start_date: str
    end_date: str
    total_count: int = 1000


@router.get("/coupons")
async def list_coupons(admin_id: str = Depends(get_admin_user_id)):
    """获取券码列表"""
    result = await parse_client.query_objects("Coupon", order="-createdAt", limit=100)
    coupons = []
    for item in result.get("results", []):
        coupons.append({
            "id": item["objectId"],
            "code": item.get("code", ""),
            "type": item.get("type", "fixed"),
            "value": item.get("value", 0),
            "minAmount": item.get("minAmount"),
            "scope": item.get("scope", "all"),
            "scopeDetail": item.get("scopeDetail"),
            "startDate": item.get("startDate", ""),
            "endDate": item.get("endDate", ""),
            "totalCount": item.get("totalCount", 0),
            "usedCount": item.get("usedCount", 0),
            "status": item.get("status", "active"),
            "createdAt": item.get("createdAt", ""),
        })
    return {"coupons": coupons}


@router.post("/coupons")
async def create_coupon(
    request: CouponCreateRequest,
    admin_id: str = Depends(get_admin_user_id),
):
    """创建券码"""
    coupon = await parse_client.create_object("Coupon", {
        "code": request.code,
        "type": request.type,
        "value": request.value,
        "minAmount": request.min_amount,
        "scope": request.scope,
        "scopeDetail": request.scope_detail,
        "startDate": request.start_date,
        "endDate": request.end_date,
        "totalCount": request.total_count,
        "usedCount": 0,
        "status": "active",
        "createdBy": admin_id,
    })
    return {"success": True, "id": coupon.get("objectId")}


@router.delete("/coupons/{coupon_id}")
async def delete_coupon(coupon_id: str, admin_id: str = Depends(get_admin_user_id)):
    """删除券码"""
    await parse_client.delete_object("Coupon", coupon_id)
    return {"success": True}


@router.put("/coupons/{coupon_id}/status")
async def update_coupon_status(coupon_id: str, status: str, admin_id: str = Depends(get_admin_user_id)):
    """禁用/启用券码"""
    await parse_client.update_object("Coupon", coupon_id, {"status": status})
    return {"success": True}


# ==================== 促销管理 ====================

class PromotionCreateRequest(BaseModel):
    name: str
    type: str  # discount, threshold, gift
    discount: Optional[float] = None
    min_amount: Optional[float] = None
    gift_product: Optional[str] = None
    start_date: str
    end_date: str


@router.get("/promotions")
async def list_promotions(admin_id: str = Depends(get_admin_user_id)):
    """获取促销活动列表"""
    result = await parse_client.query_objects("Promotion", order="-createdAt", limit=100)
    promotions = []
    for item in result.get("results", []):
        promotions.append({
            "id": item["objectId"],
            "name": item.get("name", ""),
            "type": item.get("type", "discount"),
            "status": item.get("status", "draft"),
            "discount": item.get("discount"),
            "minAmount": item.get("minAmount"),
            "giftProduct": item.get("giftProduct"),
            "startDate": item.get("startDate", ""),
            "endDate": item.get("endDate", ""),
            "productCount": item.get("productCount", 0),
            "orderCount": item.get("orderCount", 0),
            "revenue": item.get("revenue", 0),
            "createdAt": item.get("createdAt", ""),
        })
    return {"promotions": promotions}


@router.post("/promotions")
async def create_promotion(
    request: PromotionCreateRequest,
    admin_id: str = Depends(get_admin_user_id),
):
    """创建促销活动"""
    promo = await parse_client.create_object("Promotion", {
        "name": request.name,
        "type": request.type,
        "discount": request.discount,
        "minAmount": request.min_amount,
        "giftProduct": request.gift_product,
        "startDate": request.start_date,
        "endDate": request.end_date,
        "status": "draft",
        "productCount": 0,
        "orderCount": 0,
        "revenue": 0,
        "createdBy": admin_id,
    })
    return {"success": True, "id": promo.get("objectId")}


@router.put("/promotions/{promo_id}/status")
async def update_promotion_status(promo_id: str, status: str, admin_id: str = Depends(get_admin_user_id)):
    """更新促销状态 (active/paused/ended)"""
    await parse_client.update_object("Promotion", promo_id, {"status": status})
    return {"success": True}


@router.delete("/promotions/{promo_id}")
async def delete_promotion(promo_id: str, admin_id: str = Depends(get_admin_user_id)):
    """删除促销活动"""
    await parse_client.delete_object("Promotion", promo_id)
    return {"success": True}


# ==================== 充值管理 ====================

class RechargePlanRequest(BaseModel):
    amount: float
    bonus: float = 0
    enabled: bool = True


@router.get("/recharge/records")
async def list_recharge_records(
    page: int = 1,
    limit: int = 20,
    admin_id: str = Depends(get_admin_user_id),
):
    """获取充值记录"""
    skip = (page - 1) * limit
    result = await parse_client.query_objects(
        "RechargeRecord", order="-createdAt", limit=limit, skip=skip
    )
    total = await parse_client.count_objects("RechargeRecord")
    records = []
    for item in result.get("results", []):
        records.append({
            "id": item["objectId"],
            "userId": item.get("userId", ""),
            "username": item.get("username", ""),
            "amount": item.get("amount", 0),
            "bonus": item.get("bonus", 0),
            "method": item.get("method", ""),
            "status": item.get("status", ""),
            "createdAt": item.get("createdAt", ""),
        })
    return {"records": records, "total": total}


@router.get("/recharge/plans")
async def list_recharge_plans(admin_id: str = Depends(get_admin_user_id)):
    """获取充值方案"""
    result = await parse_client.query_objects("RechargePlan", order="amount", limit=20)
    plans = []
    for item in result.get("results", []):
        plans.append({
            "id": item["objectId"],
            "amount": item.get("amount", 0),
            "bonus": item.get("bonus", 0),
            "enabled": item.get("enabled", True),
        })
    return {"plans": plans}


@router.post("/recharge/plans")
async def create_recharge_plan(
    request: RechargePlanRequest,
    admin_id: str = Depends(get_admin_user_id),
):
    """创建充值方案"""
    plan = await parse_client.create_object("RechargePlan", {
        "amount": request.amount,
        "bonus": request.bonus,
        "enabled": request.enabled,
    })
    return {"success": True, "id": plan.get("objectId")}


@router.put("/recharge/plans/{plan_id}")
async def update_recharge_plan(
    plan_id: str,
    request: RechargePlanRequest,
    admin_id: str = Depends(get_admin_user_id),
):
    """更新充值方案"""
    await parse_client.update_object("RechargePlan", plan_id, {
        "amount": request.amount,
        "bonus": request.bonus,
        "enabled": request.enabled,
    })
    return {"success": True}


# ==================== 账户明细 ====================

@router.get("/accounts/records")
async def list_account_records(
    page: int = 1,
    limit: int = 50,
    admin_id: str = Depends(get_admin_user_id),
):
    """获取平台账户资金明细"""
    skip = (page - 1) * limit
    result = await parse_client.query_objects(
        "AccountRecord", order="-createdAt", limit=limit, skip=skip
    )
    total = await parse_client.count_objects("AccountRecord")
    records = []
    for item in result.get("results", []):
        records.append({
            "id": item["objectId"],
            "type": item.get("type", ""),
            "category": item.get("category", ""),
            "amount": item.get("amount", 0),
            "balance": item.get("balance", 0),
            "description": item.get("description", ""),
            "relatedOrderNo": item.get("relatedOrderNo"),
            "createdAt": item.get("createdAt", ""),
        })
    return {"records": records, "total": total}


@router.get("/accounts/summary")
async def account_summary(admin_id: str = Depends(get_admin_user_id)):
    """获取平台账户汇总"""
    # 获取最近一条记录的余额
    latest = await parse_client.query_objects(
        "AccountRecord", order="-createdAt", limit=1, keys="balance"
    )
    balance = 0
    if latest.get("results"):
        balance = latest["results"][0].get("balance", 0)

    # 收入和支出汇总
    all_records = await parse_client.query_objects(
        "AccountRecord", limit=1000, keys="type,amount"
    )
    total_income = 0
    total_expense = 0
    for r in all_records.get("results", []):
        amt = r.get("amount", 0)
        if r.get("type") == "income":
            total_income += amt
        elif r.get("type") in ("expense", "fee"):
            total_expense += abs(amt)

    return {
        "balance": round(balance, 2),
        "totalIncome": round(total_income, 2),
        "totalExpense": round(total_expense, 2),
    }
