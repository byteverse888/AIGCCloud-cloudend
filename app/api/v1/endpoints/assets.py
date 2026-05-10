"""
AI资产发布与购买接口
"""
from fastapi import APIRouter, HTTPException, Depends, Request
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timezone
import uuid

from app.core.parse_client import parse_client
from app.core.redis_client import redis_client
from app.core.deps import get_current_user_id, get_operator_user_id
from app.core.security import generate_order_no, verify_password
from app.core.logger import logger
from app.core.operation_log import log_operation
from app.core.incentive_service import incentive_service

router = APIRouter()

CART_TTL = 7 * 24 * 3600  # 7 days


class AssetPublishRequest(BaseModel):
    name: str
    description: Optional[str] = None
    category: str
    price: float = 0
    cover_key: Optional[str] = None


class AssetPurchaseRequest(BaseModel):
    asset_id: str


class AssetUpdateRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    cover_key: Optional[str] = None
    copyright: Optional[str] = None
    license: Optional[str] = None
    tags: Optional[List[str]] = None
    price: Optional[float] = None


class BatchSubmitItem(BaseModel):
    asset_id: str
    name: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    price: float = 0


class BatchSubmitRequest(BaseModel):
    items: List[BatchSubmitItem]


class BalancePayRequest(BaseModel):
    """账户积分余额支付请求"""
    payment_password: str


def _require_payment_password(user: dict, input_password: str) -> None:
    """校验用户已设置且输入正确的支付密码。失败时直接抛 HTTPException。"""
    pwd_hash = (user.get("paymentPassword") or "") if user else ""
    if not pwd_hash:
        raise HTTPException(status_code=400, detail="请先设置支付密码")
    if not input_password:
        raise HTTPException(status_code=400, detail="请输入支付密码")
    try:
        ok = verify_password(input_password, pwd_hash)
    except Exception:
        ok = False
    if not ok:
        raise HTTPException(status_code=400, detail="支付密码错误")

@router.post("/publish")
async def publish_asset(
    request: AssetPublishRequest,
    user_id: str = Depends(get_current_user_id)
):
    """发布资产到商城"""
    logger.info(f"[资产发布] user_id={user_id}, name={request.name}")
    
    if not request.name or len(request.name) < 2:
        raise HTTPException(status_code=400, detail="资产名称至少2个字符")
    
    if not request.category:
        raise HTTPException(status_code=400, detail="请选择资产分类")
    
    valid_categories = ["image", "audio", "video", "model", "music", "digital-human", "comic", "other"]
    if request.category not in valid_categories:
        raise HTTPException(status_code=400, detail="无效的资产分类")
    
    if request.price < 0:
        raise HTTPException(status_code=400, detail="价格不能为负数")
    
    asset_data = {
        "name": request.name,
        "description": request.description or "",
        "category": request.category,
        "price": request.price,
        "creatorId": user_id,
        "owner": user_id,
        "status": "draft",
        "sales": 0,
        "coverKey": request.cover_key or "",
    }
    
    try:
        result = await parse_client.create_object("Product", asset_data)
        asset_id = result.get("objectId")
        logger.info(f"[资产发布] 成功: asset_id={asset_id}")
        return {"success": True, "id": asset_id, "message": "资产已发布，待审核后上架"}
    except Exception as e:
        logger.error(f"[资产发布] 失败: {e}")
        raise HTTPException(status_code=500, detail="发布失败")


@router.get("/my")
async def get_my_assets(
    page: int = 1,
    limit: int = 20,
    status: Optional[str] = None,
    user_id: str = Depends(get_current_user_id)
):
    """获取我的资产列表"""
    where = {"creatorId": user_id}
    if status:
        where["status"] = status
    
    skip = (page - 1) * limit
    result = await parse_client.query_objects("Product", where=where, order="-createdAt", limit=limit, skip=skip)
    total = await parse_client.count_objects("Product", where)
    
    assets = []
    for item in result.get("results", []):
        assets.append({
            "id": item.get("objectId"),
            "name": item.get("name"),
            "description": item.get("description"),
            "category": item.get("category"),
            "price": item.get("price", 0),
            "status": item.get("status"),
            "sales": item.get("sales", 0),
            "coverKey": item.get("coverKey"),
            "createdAt": item.get("createdAt"),
            # 审核相关字段：用于展示驳回/下架原因
            "reviewNote": item.get("reviewNote"),
            "offlineReason": item.get("offlineReason"),
            "reviewedAt": item.get("reviewedAt"),
        })
    
    return {"data": assets, "total": total, "page": page, "limit": limit}


@router.get("/purchases")
async def get_purchased_assets(
    page: int = 1,
    limit: int = 20,
    user_id: str = Depends(get_current_user_id)
):
    """获取我购买的资产"""
    skip = (page - 1) * limit
    result = await parse_client.query_objects(
        "Order",
        where={"userId": user_id, "type": "purchase"},
        order="-createdAt",
        limit=limit,
        skip=skip
    )
    total = await parse_client.count_objects("Order", {"userId": user_id, "type": "purchase"})
    
    purchases = []
    for item in result.get("results", []):
        product_id = item.get("productId")
        if product_id:
            try:
                product = await parse_client.get_object("Product", product_id)
                purchases.append({
                    "order_id": item.get("objectId"),
                    "order_no": item.get("orderNo"),
                    "asset": {
                        "id": product.get("objectId"),
                        "name": product.get("name"),
                        "coverKey": product.get("coverKey"),
                    },
                    "amount": item.get("amount", 0),
                    "status": item.get("status"),
                    "createdAt": item.get("createdAt"),
                })
            except:
                pass
    
    return {"data": purchases, "total": total, "page": page, "limit": limit}


# ============ 购物车接口（必须在 /{asset_id} 通配路由之前注册）============

@router.get("/cart")
async def get_cart(user_id: str = Depends(get_current_user_id)):
    """获取购物车（容错：清理已删/下架的无效项）"""
    cart_key = f"cart:{user_id}"
    cart_data = await redis_client.get(cart_key)
    if not cart_data:
        return {"data": [], "total": 0}

    import json
    try:
        cart_items = json.loads(cart_data)
    except Exception:
        cart_items = []

    result = []
    valid_raw_items = []  # 保留有效购物车原始项（用于回写 Redis）
    has_invalid = False
    for item in cart_items:
        asset_id = item.get("asset_id") if isinstance(item, dict) else None
        if not asset_id:
            has_invalid = True
            continue
        try:
            product = await parse_client.get_object("Product", asset_id)
        except Exception as e:
            # Product 已删除或查询失败 → 跳过，标记需清理
            logger.warning(f"[Cart] 购物车项失效 asset_id={asset_id}: {e}")
            has_invalid = True
            continue
        if not product or product.get("status") != "approved":
            # 下架/未审核 → 清理
            has_invalid = True
            continue
        result.append({
            "asset_id": asset_id,
            "name": product.get("name"),
            "price": product.get("price", 0),
            "coverKey": product.get("coverKey"),
            "addedAt": item.get("addedAt"),
        })
        valid_raw_items.append(item)

    # 有无效项 → 回写 Redis，避免下次反复报错
    if has_invalid:
        try:
            if valid_raw_items:
                try:
                    ttl = await redis_client.client.ttl(cart_key)
                except Exception:
                    ttl = 0
                ttl = ttl if (isinstance(ttl, int) and ttl > 0) else CART_TTL
                await redis_client.set(cart_key, json.dumps(valid_raw_items), ex=ttl)
            else:
                await redis_client.delete(cart_key)
        except Exception as e:
            logger.warning(f"[Cart] 清理无效项回写 Redis 失败: {e}")

    total = sum(item["price"] for item in result)
    return {"data": result, "total": total}


@router.post("/cart")
async def add_to_cart(
    request: AssetPurchaseRequest,
    user_id: str = Depends(get_current_user_id)
):
    """添加资产到购物车"""
    asset_id = request.asset_id
    
    try:
        asset = await parse_client.get_object("Product", asset_id)
    except Exception:
        raise HTTPException(status_code=404, detail="资产不存在")
    
    if asset.get("status") != "approved":
        raise HTTPException(status_code=400, detail="该资产暂不可购买")
    
    if asset.get("creatorId") == user_id:
        raise HTTPException(status_code=400, detail="不能购买自己发布的资产")
    
    cart_key = f"cart:{user_id}"
    cart_data = await redis_client.get(cart_key)
    
    import json
    cart_items = json.loads(cart_data) if cart_data else []
    
    for item in cart_items:
        if item["asset_id"] == asset_id:
            raise HTTPException(status_code=400, detail="该资产已在购物车中")
    
    cart_items.append({
        "asset_id": asset_id,
        "addedAt": datetime.now(timezone.utc).isoformat()
    })
    
    await redis_client.set(cart_key, json.dumps(cart_items), ex=CART_TTL)
    
    return {"success": True, "message": "已添加到购物车", "count": len(cart_items)}


@router.delete("/cart/{asset_id}")
async def remove_from_cart(asset_id: str, user_id: str = Depends(get_current_user_id)):
    """从购物车移除（幂等：购物车为空或项不存在也返回成功）"""
    cart_key = f"cart:{user_id}"
    cart_data = await redis_client.get(cart_key)
    if not cart_data:
        return {"success": True, "message": "购物车为空", "count": 0}

    import json
    try:
        cart_items = json.loads(cart_data)
    except Exception:
        cart_items = []
    cart_items = [item for item in cart_items if isinstance(item, dict) and item.get("asset_id") != asset_id]

    if cart_items:
        await redis_client.set(cart_key, json.dumps(cart_items), ex=CART_TTL)
    else:
        await redis_client.delete(cart_key)

    return {"success": True, "message": "已从购物车移除", "count": len(cart_items)}


@router.get("/{asset_id}")
async def get_asset(asset_id: str, user_id: str = Depends(get_current_user_id)):
    """获取资产详情"""
    try:
        asset = await parse_client.get_object("Product", asset_id)
    except Exception:
        raise HTTPException(status_code=404, detail="资产不存在")
    
    creator_id = asset.get("creatorId")
    owner = asset.get("owner")
    is_public = asset.get("status") == "approved"
    is_owner = creator_id == user_id or owner == user_id
    
    if not is_public and not is_owner:
        return {
            "id": asset.get("objectId"),
            "name": asset.get("name"),
            "category": asset.get("category"),
            "status": asset.get("status"),
        }
    
    return {
        "id": asset.get("objectId"),
        "name": asset.get("name"),
        "description": asset.get("description"),
        "category": asset.get("category"),
        "price": asset.get("price", 0),
        "status": asset.get("status"),
        "sales": asset.get("sales", 0),
        "coverKey": asset.get("coverKey"),
        "creatorId": creator_id,
        "owner": owner,
        "createdAt": asset.get("createdAt"),
    }


@router.put("/{asset_id}")
async def update_asset(
    asset_id: str,
    request: AssetUpdateRequest,
    user_id: str = Depends(get_current_user_id)
):
    """编辑AI资产（兼容 AIIPAsset / Product 两类 ID）"""
    # 先当作 AIIPAsset 查，回落 Product
    asset_cls = ""
    asset = None
    try:
        asset = await parse_client.get_object("AIIPAsset", asset_id)
        asset_cls = "AIIPAsset"
    except Exception:
        try:
            asset = await parse_client.get_object("Product", asset_id)
            asset_cls = "Product"
        except Exception:
            raise HTTPException(status_code=404, detail="资产不存在")

    creator_id = asset.get("creatorId") or asset.get("ownerId")
    owner = asset.get("owner")
    if creator_id != user_id and owner != user_id:
        raise HTTPException(status_code=403, detail="无权编辑")

    # 可编辑状态：draft / offline / rejected / 空
    current_status = asset.get("status") or ""
    if current_status not in ["draft", "offline", "rejected", ""]:
        raise HTTPException(
            status_code=400,
            detail=f"当前状态({current_status})不可编辑，请先下架或等待审核结果"
        )

    update_data = {k: v for k, v in request.dict(exclude_unset=True).items() if v is not None}
    # cover_key → cover 字段映射
    if "cover_key" in update_data:
        update_data["cover"] = update_data.pop("cover_key")
    if request.tags is not None:
        update_data["tags"] = request.tags

    if not update_data:
        return {"success": True, "message": "无更新"}

    try:
        await parse_client.update_object(asset_cls, asset_id, update_data)
    except Exception as e:
        logger.error(f"[UpdateAsset] {asset_cls} 更新失败 id={asset_id}: {e}")
        raise HTTPException(status_code=500, detail="更新失败")

    # 若编辑的是 AIIPAsset 且已关联 Product（已提交/已上架过），同步关键展示字段到 Product
    if asset_cls == "AIIPAsset":
        listed_product_id = asset.get("listedProductId")
        if listed_product_id:
            product_update: dict = {}
            for k in ("name", "description", "category", "price", "copyright", "license", "tags"):
                if k in update_data:
                    product_update[k] = update_data[k]
            if "cover" in update_data:
                product_update["cover"] = update_data["cover"]
            if product_update:
                try:
                    await parse_client.update_object("Product", listed_product_id, product_update)
                except Exception as e:
                    logger.warning(f"[UpdateAsset] 同步 Product 失败 product_id={listed_product_id}: {e}")

    return {"success": True, "message": "资产已更新"}


@router.post("/{asset_id}/submit")
async def submit_for_review(
    asset_id: str,
    user_id: str = Depends(get_current_user_id)
):
    """提交AI资产审核"""
    try:
        asset = await parse_client.get_object("AIIPAsset", asset_id)
    except Exception:
        try:
            asset = await parse_client.get_object("Product", asset_id)
        except Exception:
            raise HTTPException(status_code=404, detail="资产不存在")
    
    owner_id = asset.get("ownerId") or asset.get("creatorId")
    if owner_id != user_id:
        raise HTTPException(status_code=403, detail="无权操作")
    
    current_status = asset.get("status", "")
    if current_status not in ["draft", ""]:
        raise HTTPException(status_code=400, detail="只有草稿可提交审核")
    
    product_data = {
        "name": asset.get("name"),
        "description": asset.get("description") or "",
        "cover": asset.get("cover") or asset.get("coverKey", ""),
        "category": asset.get("category"),
        "status": "pending",
        "creatorId": user_id,
        "owner": user_id,
        "copyright": asset.get("copyright", ""),
        "license": asset.get("license", "CC-BY-NC-ND"),
        "sales": 0,
        "likeCount": 0,
        "favoriteCount": 0,
        "views": 0,
        "commentCount": 0,
    }
    
    product_result = await parse_client.create_object("Product", product_data)
    product_id = product_result.get("objectId")
    
    await parse_client.update_object("AIIPAsset", asset_id, {
        "status": "pending",
        "listedProductId": product_id,
        "isListed": True,
    })
    
    return {"success": True, "product_id": product_id, "message": "已提交审核"}


@router.post("/batch-submit")
async def batch_submit_for_review(
    request: BatchSubmitRequest,
    user_id: str = Depends(get_current_user_id),
):
    """
    批量提交 AI 资产申请上架审核
    - 对每项校验归属与状态（仅 draft/offline/rejected 或 Product 的 draft）
    - 可同步更新 name/description/category/price
    - 将资产置为 pending；如果是 AIIPAsset，还会创建对应 Product 条目
    """
    if not request.items:
        raise HTTPException(status_code=400, detail="未选中任何资产")

    valid_categories = ["image", "audio", "video", "model", "music", "digital-human", "comic", "other"]

    async def _submit_one(item: BatchSubmitItem) -> dict:
        aid = item.asset_id
        try:
            if item.price is None or item.price < 0:
                return {"asset_id": aid, "success": False, "error": "价格不能为负数"}
            if item.category and item.category not in valid_categories:
                return {"asset_id": aid, "success": False, "error": "无效的资产分类"}

            # 优先当作 AIIPAsset 查（任务转化产物），回落 Product（直接 publish）
            asset_cls = ""
            asset_data = None
            try:
                asset_data = await parse_client.get_object("AIIPAsset", aid)
                asset_cls = "AIIPAsset"
            except Exception:
                try:
                    asset_data = await parse_client.get_object("Product", aid)
                    asset_cls = "Product"
                except Exception:
                    return {"asset_id": aid, "success": False, "error": "资产不存在"}

            owner_id = asset_data.get("ownerId") or asset_data.get("creatorId") or asset_data.get("owner")
            if owner_id != user_id:
                return {"asset_id": aid, "success": False, "error": "无权操作该资产"}

            current_status = asset_data.get("status") or ""
            if current_status not in ("draft", "offline", "rejected", ""):
                return {"asset_id": aid, "success": False, "error": f"资产当前状态({current_status})不可提交"}

            new_name = (item.name or asset_data.get("name") or "").strip()
            new_desc = item.description if item.description is not None else (asset_data.get("description") or "")
            new_category = item.category or asset_data.get("category") or "other"
            if not new_name or len(new_name) < 2:
                return {"asset_id": aid, "success": False, "error": "资产名称至少2个字符"}

            if asset_cls == "AIIPAsset":
                product_data = {
                    "name": new_name,
                    "description": new_desc,
                    "cover": asset_data.get("cover") or asset_data.get("coverKey", ""),
                    "category": new_category,
                    "price": float(item.price or 0),
                    "status": "pending",
                    "creatorId": user_id,
                    "owner": user_id,
                    "copyright": asset_data.get("copyright", ""),
                    "license": asset_data.get("license", "CC-BY-NC-ND"),
                    "sales": 0,
                    "likeCount": 0,
                    "favoriteCount": 0,
                    "views": 0,
                    "commentCount": 0,
                }
                prod_res = await parse_client.create_object("Product", product_data)
                prod_id = prod_res.get("objectId")
                await parse_client.update_object("AIIPAsset", aid, {
                    "name": new_name,
                    "description": new_desc,
                    "category": new_category,
                    "price": float(item.price or 0),
                    "status": "pending",
                    "listedProductId": prod_id,
                    "isListed": True,
                })
                return {"asset_id": aid, "success": True, "product_id": prod_id}
            else:
                # Product 直接更新
                update_data = {
                    "name": new_name,
                    "description": new_desc,
                    "category": new_category,
                    "price": float(item.price or 0),
                    "status": "pending",
                }
                await parse_client.update_object("Product", aid, update_data)
                return {"asset_id": aid, "success": True, "product_id": aid}
        except Exception as e:
            logger.error(f"[BatchSubmit] 处理失败 asset_id={aid}: {e}")
            return {"asset_id": aid, "success": False, "error": str(e)}

    import asyncio
    results = await asyncio.gather(*[_submit_one(item) for item in request.items])
    success_count = sum(1 for r in results if r.get("success"))
    return {
        "success": True,
        "total": len(request.items),
        "success_count": success_count,
        "failed_count": len(request.items) - success_count,
        "results": results,
    }


@router.post("/{asset_id}/purchase")
async def purchase_asset(asset_id: str, user_id: str = Depends(get_current_user_id)):
    """购买资产"""
    # 获取资产信息
    try:
        asset = await parse_client.get_object("Product", asset_id)
    except Exception:
        raise HTTPException(status_code=404, detail="资产不存在")
    
    # 检查资产状态
    if asset.get("status") != "approved":
        raise HTTPException(status_code=400, detail="该资产暂不可购买")
    
    # 检查是否是自己发布的
    if asset.get("creatorId") == user_id:
        raise HTTPException(status_code=400, detail="不能购买自己发布的资产")
    
    # 检查是否已购买
    existing = await parse_client.query_objects(
        "Order",
        where={"userId": user_id, "productId": asset_id, "status": "completed"}
    )
    if existing.get("results"):
        raise HTTPException(status_code=400, detail="您已购买过该资产")
    
    price = float(asset.get("price", 0))
    creator_id = asset.get("creatorId")
    
    # 免费资产直接转移所有权
    if price == 0:
        await parse_client.update_object("Product", asset_id, {
            "owner": user_id,
            "sales": parse_client.increment(1)
        })
        await parse_client.create_object("Order", {
            "orderNo": generate_order_no(),
            "userId": user_id,
            "productId": asset_id,
            "productName": asset.get("name"),
            "amount": 0,
            "type": "purchase",
            "status": "completed",
            "completedAt": datetime.now(timezone.utc).isoformat(),
        })
        return {"success": True, "message": "资产已获取", "free": True}
    
    # 付费资产创建订单
    order_no = generate_order_no()
    await parse_client.create_object("Order", {
        "orderNo": order_no,
        "userId": user_id,
        "productId": asset_id,
        "productName": asset.get("name"),
        "amount": price,
        "type": "purchase",
        "status": "pending",
    })
    
    return {
        "success": True,
        "order_id": order_no,
        "amount": price,
        "message": "订单已创建，请完成支付"
    }


@router.post("/{asset_id}/purchase-with-balance")
async def purchase_asset_with_balance(
    asset_id: str,
    request: BalancePayRequest,
    user_id: str = Depends(get_current_user_id),
):
    """
    使用用户积分余额（totalIncentive）购买商品。
    - 校验商品存在且 approved
    - 校验非自己的商品
    - 校验之前未完成过该商品的购买
    - 校验支付密码
    - 校验余额充足
    - 原子扣卖家 / 增买家、Product.sales+1、创建 completed 订单
    - 同步从购物车移除
    """
    # 1. 商品校验
    try:
        asset = await parse_client.get_object("Product", asset_id)
    except Exception:
        raise HTTPException(status_code=404, detail="商品不存在")

    if asset.get("status") != "approved":
        raise HTTPException(status_code=400, detail="该商品暂不可购买")

    creator_id = asset.get("creatorId")
    if creator_id == user_id:
        raise HTTPException(status_code=400, detail="不能购买自己的商品")

    # 2. 重复购买校验
    existing = await parse_client.query_objects(
        "Order",
        where={"userId": user_id, "productId": asset_id, "status": "completed"}
    )
    if existing.get("results"):
        raise HTTPException(status_code=400, detail="您已购买过该商品")

    price = float(asset.get("price", 0) or 0)
    
    # 3. 余额校验（免费商品直接通过）
    try:
        buyer = await parse_client.get_user(user_id)
    except Exception:
        raise HTTPException(status_code=404, detail="用户不存在")

    # 4. 支付密码校验（仅需真实扣款时校验；免费商品跳过）
    if price > 0:
        _require_payment_password(buyer, request.payment_password)

    balance = incentive_service._read_balance(buyer)
    if price > 0 and balance < price:
        raise HTTPException(status_code=400, detail=f"积分余额不足，需要 {price:g} 积分，当前 {balance:g}")
    
    # 4. 先创建订单（需要 orderNo 作为账本 relatedOrderNo）
    order_no = generate_order_no()
    
    # 5. 扣买家（统一账本）
    if price > 0:
        buyer_result = await incentive_service.adjust_user_balance(
            user_id=user_id,
            delta=-float(price),
            type_="purchase",
            category="product_purchase",
            description=f"购买商品: {asset.get('name', '')}",
            related_id=f"purchase_{order_no}",
            related_order_no=order_no,
        )
        if not buyer_result.get("success"):
            raise HTTPException(status_code=500, detail=buyer_result.get("error", "扣减余额失败"))

        # 6. 卖家入账（失败则回滚买家，整体交易失败）
        if creator_id:
            seller_result = await incentive_service.adjust_user_balance(
                user_id=creator_id,
                delta=float(price),
                type_="reward",
                category="product_income",
                description=f"商品售卖收入: {asset.get('name', '')}",
                related_id=f"income_{order_no}",
                related_order_no=order_no,
            )
            if not seller_result.get("success"):
                # 回滚买家扣款
                logger.error(
                    f"[积分支付] 卖家入账失败，回滚买家: buyer={user_id} seller={creator_id} price={price} err={seller_result.get('error')}"
                )
                try:
                    await incentive_service.adjust_user_balance(
                        user_id=user_id,
                        delta=float(price),
                        type_="refund",
                        category="purchase_rollback",
                        description=f"商品购买回滚（卖家入账失败）: {asset.get('name', '')}",
                        related_id=f"purchase_rollback_{order_no}",
                        related_order_no=order_no,
                        check_idempotent=False,
                    )
                except Exception as _re:
                    logger.error(f"[积分支付] 买家回滚异常: {_re}")
                raise HTTPException(
                    status_code=500,
                    detail=f"卖家入账失败，交易已回滚: {seller_result.get('error', '')}",
                )
    
    # 7. 商品销售数 +1（失败不阻断）
    try:
        await parse_client.update_object("Product", asset_id, {
            "sales": parse_client.increment(1)
        })
    except Exception as e:
        logger.warning(f"[积分支付] 更新商品销量失败: {e}")
    
    # 8. 创建 completed 订单
    try:
        await parse_client.create_object("Order", {
            "orderNo": order_no,
            "userId": user_id,
            "productId": asset_id,
            "productName": asset.get("name"),
            "amount": price,
            "type": "purchase",
            "paymentMethod": "balance",
            "status": "completed",
            "completedAt": datetime.now(timezone.utc).isoformat(),
            "sellerId": creator_id,
        })
    except Exception as e:
        logger.error(f"[积分支付] 创建订单失败: {e}")
    
    # 9. 同步从购物车移除（失败不阻断）
    try:
        import json
        cart_key = f"cart:{user_id}"
        cart_data = await redis_client.get(cart_key)
        if cart_data:
            items = json.loads(cart_data)
            new_items = [it for it in items if it.get("asset_id") != asset_id]
            if new_items:
                try:
                    ttl = await redis_client.client.ttl(cart_key)
                except Exception:
                    ttl = 0
                ttl = ttl if (isinstance(ttl, int) and ttl > 0) else CART_TTL
                await redis_client.set(cart_key, json.dumps(new_items), ex=ttl)
            else:
                await redis_client.delete(cart_key)
    except Exception as e:
        logger.warning(f"[积分支付] 同步购物车失败（忽略）: {e}")
    
    return {
        "success": True,
        "order_no": order_no,
        "amount": price,
        "message": "购买成功",
    }


# ============ 购物车接口（必须在 /{asset_id} 通配路由之前注册）============


# ============ 管理/运营端接口 ============

class AdminAssetReviewRequest(BaseModel):
    asset_id: str
    status: str  # approved / rejected
    review_note: Optional[str] = None


@router.get("/admin/list")
async def admin_list_assets(
    page: int = 1,
    limit: int = 20,
    status: Optional[str] = None,
    category: Optional[str] = None,
    keyword: Optional[str] = None,
    owner_id: Optional[str] = None,
    operator_id: str = Depends(get_operator_user_id),
):
    """
    管理/运营端：获取 AI 资产列表
    - status: draft / pending / approved / rejected / all
    - category: image / audio / video / model 等
    - keyword: 按名称模糊搜索
    - owner_id: 按所有者 userId 精确过滤
    """
    where: dict = {}
    if status and status != "all":
        where["status"] = status
    if category and category != "all":
        where["category"] = category
    if keyword:
        where["name"] = {"$regex": keyword, "$options": "i"}
    if owner_id:
        # 支持按 userId 精确 或 用户名 模糊搜索
        user_kw = owner_id.strip()
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
        where["ownerId"] = {"$in": list(candidate_ids)} if len(candidate_ids) > 1 else user_kw

    skip = (page - 1) * limit
    order = "createdAt" if where.get("status") == "pending" else "-createdAt"
    try:
        result = await parse_client.query_objects(
            "AIIPAsset",
            where=where if where else None,
            order=order,
            limit=limit,
            skip=skip,
        )
        total = await parse_client.count_objects("AIIPAsset", where if where else None)
    except Exception as e:
        logger.error(f"[Admin][AI资产列表] 查询失败: {e}")
        raise HTTPException(status_code=500, detail="查询失败")

    # 补充所有者信息 + 审核人用户名（共用缓存）
    items = []
    user_cache: dict = {}

    async def _resolve_name(uid: str) -> str:
        if not uid:
            return ""
        if uid in user_cache:
            return user_cache[uid]
        try:
            u = await parse_client.get_user(uid)
            name = (u.get("username") or "") if u else ""
        except Exception:
            name = ""
        user_cache[uid] = name
        return name

    for a in result.get("results", []):
        owner_id = a.get("ownerId") or ""
        owner_name = a.get("ownerName") or ""
        if not owner_name and owner_id:
            owner_name = await _resolve_name(owner_id)
        reviewer_id = a.get("reviewedBy") or ""
        reviewer_name = await _resolve_name(reviewer_id) if reviewer_id else ""
        items.append({
            "id": a.get("objectId"),
            "objectId": a.get("objectId"),
            "name": a.get("name") or "",
            "description": a.get("description") or "",
            "category": a.get("category") or "",
            "price": a.get("price", 0),
            "status": a.get("status") or "draft",
            "cover": a.get("cover") or "",
            "assetUrl": a.get("assetUrl") or "",
            "ownerId": owner_id,
            "ownerName": owner_name,
            "isListed": bool(a.get("isListed")),
            "listedProductId": a.get("listedProductId") or "",
            "views": a.get("views", 0),
            "createdAt": a.get("createdAt"),
            "updatedAt": a.get("updatedAt"),
            # 审核相关字段：用于展示驳回/下架原因
            "reviewNote": a.get("reviewNote") or "",
            "offlineReason": a.get("offlineReason") or "",
            "reviewedAt": a.get("reviewedAt") or "",
            "reviewedBy": reviewer_id,
            "reviewerName": reviewer_name,
        })

    return {
        "data": items,
        "total": total,
        "page": page,
        "limit": limit,
    }


@router.post("/admin/review")
async def admin_review_asset(
    request: AdminAssetReviewRequest,
    http_request: Request,
    operator_id: str = Depends(get_operator_user_id),
):
    """
    管理/运营端：审核 AI 资产（approved / rejected）
    """
    if request.status not in ("approved", "rejected"):
        raise HTTPException(status_code=400, detail="status 只能为 approved 或 rejected")

    try:
        asset = await parse_client.get_object("AIIPAsset", request.asset_id)
    except Exception:
        raise HTTPException(status_code=404, detail="资产不存在")

    previous_status = asset.get("status")
    update_data: dict = {
        "status": request.status,
        "reviewedAt": datetime.now(timezone.utc).isoformat(),
        "reviewedBy": operator_id,
    }
    if request.review_note:
        update_data["reviewNote"] = request.review_note
    # 驳回时，同步写入 offlineReason，便于用户端展示具体原因
    if request.status == "rejected":
        update_data["offlineReason"] = request.review_note or (
            "已审核商品被驳回下架" if previous_status == "approved" else "审核驳回"
        )

    try:
        await parse_client.update_object("AIIPAsset", request.asset_id, update_data)
    except Exception as e:
        logger.error(f"[Admin][AI资产审核] 更新失败: {e}")
        raise HTTPException(status_code=500, detail="审核失败")

    # 如果资产已关联 Product（提交审核时创建），同步更新 Product 状态
    listed_product_id = asset.get("listedProductId")
    if listed_product_id:
        try:
            product_update: dict = {
                "status": request.status,
                "reviewedAt": update_data["reviewedAt"],
                "reviewedBy": operator_id,
                "reviewNote": request.review_note or "",
            }
            if request.status == "rejected":
                product_update["offlineReason"] = update_data.get("offlineReason", "审核驳回")
            await parse_client.update_object("Product", listed_product_id, product_update)
        except Exception as e:
            logger.warning(f"[Admin][AI资产审核] 同步 Product 状态失败: {e}")
    elif request.status == "approved":
        # 兑底：历史数据或旧路径未创建 Product，审批通过时自动创建，确保商城可见
        try:
            product_data = {
                "name": asset.get("name") or "",
                "description": asset.get("description") or "",
                "cover": asset.get("cover") or asset.get("coverKey", ""),
                "category": asset.get("category") or "other",
                "price": float(asset.get("price") or 0),
                "status": "approved",
                "creatorId": asset.get("ownerId") or asset.get("creatorId") or "",
                "owner": asset.get("ownerId") or asset.get("owner") or "",
                "copyright": asset.get("copyright", ""),
                "license": asset.get("license", "CC-BY-NC-ND"),
                "reviewedAt": update_data["reviewedAt"],
                "reviewedBy": operator_id,
                "reviewNote": request.review_note or "",
                "sales": 0,
                "likeCount": 0,
                "favoriteCount": 0,
                "views": 0,
                "commentCount": 0,
            }
            prod_res = await parse_client.create_object("Product", product_data)
            prod_id = prod_res.get("objectId")
            if prod_id:
                await parse_client.update_object("AIIPAsset", request.asset_id, {
                    "listedProductId": prod_id,
                    "isListed": True,
                })
        except Exception as e:
            logger.warning(f"[Admin][AI资产审核] 兑底创建 Product 失败: {e}")

    await log_operation(
        operator_id=operator_id,
        action="review",
        module="assets",
        target_class="AIIPAsset",
        target_id=request.asset_id,
        target_name=asset.get("name") or "",
        description=f"AI资产审核: {request.status}",
        detail={"note": request.review_note or ""},
        request=http_request,
    )

    return {"success": True, "asset_id": request.asset_id, "status": request.status}


@router.get("/admin/stats")
async def admin_asset_stats(operator_id: str = Depends(get_operator_user_id)):
    """管理/运营端：AI 资产统计"""
    stats: dict = {}
    for st in ("draft", "pending", "approved", "rejected"):
        try:
            stats[st] = await parse_client.count_objects("AIIPAsset", {"status": st})
        except Exception:
            stats[st] = 0
    try:
        stats["total"] = await parse_client.count_objects("AIIPAsset")
    except Exception:
        stats["total"] = 0
    return stats


@router.post("/cart/checkout")
async def checkout_cart(user_id: str = Depends(get_current_user_id)):
    """购物车结算"""
    cart_key = f"cart:{user_id}"
    cart_data = await redis_client.get(cart_key)
    if not cart_data:
        raise HTTPException(status_code=400, detail="购物车为空")
    
    import json
    cart_items = json.loads(cart_data)
    
    orders = []
    total_amount = 0
    
    for item in cart_items:
        try:
            product = await parse_client.get_object("Product", item["asset_id"])
            if not product or product.get("status") != "approved":
                continue
            
            price = float(product.get("price", 0))
            total_amount += price
            
            order_no = generate_order_no()
            await parse_client.create_object("Order", {
                "orderNo": order_no,
                "userId": user_id,
                "productId": item["asset_id"],
                "productName": product.get("name"),
                "amount": price,
                "type": "purchase",
                "status": "pending",
            })
            orders.append({"order_no": order_no, "amount": price})
        except:
            pass
    
    await redis_client.delete(cart_key)
    
    return {
        "success": True,
        "orders": orders,
        "total": total_amount,
        "message": f"已创建 {len(orders)} 个订单"
    }


@router.post("/cart/checkout-with-balance")
async def checkout_cart_with_balance(
    request: BalancePayRequest,
    user_id: str = Depends(get_current_user_id),
):
    """
    使用账户积分余额结算购物车（批量支付）。
    步骤：
    1. 读取购物车，过滤已删/未上架/自有/已购买的商品
    2. 汇总有效商品总价，预校验用户余额充足，不足直接报错
    3. 循环逐件执行：扣买家 → 加卖家 → Product.sales+1 → 创建 completed 订单
    4. 成功项从购物车移除；全部处理完成后返回总结
    """
    import json
    cart_key = f"cart:{user_id}"
    cart_data = await redis_client.get(cart_key)
    if not cart_data:
        raise HTTPException(status_code=400, detail="购物车为空")
    try:
        cart_items = json.loads(cart_data)
    except Exception:
        cart_items = []
    if not cart_items:
        raise HTTPException(status_code=400, detail="购物车为空")

    # 1. 逐项校验，收集有效商品
    valid_list = []  # [(asset_id, product, price)]
    skipped = []     # [{asset_id, reason}]
    for item in cart_items:
        aid = item.get("asset_id") if isinstance(item, dict) else None
        if not aid:
            continue
        try:
            product = await parse_client.get_object("Product", aid)
        except Exception as e:
            logger.warning(f"[购物车结算] 商品查询失败 {aid}: {e}")
            skipped.append({"asset_id": aid, "reason": "商品不存在"})
            continue
        if not product or product.get("status") != "approved":
            skipped.append({"asset_id": aid, "reason": "商品已下架"})
            continue
        if product.get("creatorId") == user_id:
            skipped.append({"asset_id": aid, "reason": "不能购买自己的商品"})
            continue
        # 重复购买校验
        try:
            existing = await parse_client.query_objects(
                "Order",
                where={"userId": user_id, "productId": aid, "status": "completed"}
            )
            if existing.get("results"):
                skipped.append({"asset_id": aid, "reason": "已购买过"})
                continue
        except Exception:
            pass
        price = float(product.get("price", 0) or 0)
        valid_list.append((aid, product, price))

    if not valid_list:
        raise HTTPException(status_code=400, detail="购物车中无可购买的商品")

    # 2. 余额预校验
    total_amount = sum(p for (_, _, p) in valid_list)
    try:
        buyer = await parse_client.get_user(user_id)
    except Exception:
        raise HTTPException(status_code=404, detail="用户不存在")

    # 2.1 支付密码校验（总价 > 0 才校验；全免费跳过）
    if total_amount > 0:
        _require_payment_password(buyer, request.payment_password)

    balance = incentive_service._read_balance(buyer)
    if total_amount > 0 and balance < total_amount:
        raise HTTPException(
            status_code=400,
            detail=f"积分余额不足，需要 {total_amount:g} 积分，当前 {balance:g}"
        )

    # 3. 逐项执行交易
    completed_orders = []
    completed_asset_ids = set()
    for (aid, product, price) in valid_list:
        creator_id = product.get("creatorId")
        order_no = generate_order_no()

        # 扣买家
        if price > 0:
            br = await incentive_service.adjust_user_balance(
                user_id=user_id,
                delta=-float(price),
                type_="purchase",
                category="product_purchase",
                description=f"购买商品: {product.get('name', '')}",
                related_id=f"purchase_{order_no}",
                related_order_no=order_no,
            )
            if not br.get("success"):
                logger.error(f"[购物车结算] 扣买家失败 order_no={order_no} err={br.get('error')}")
                # 后续商品不再执行，直接跳出（实际用户余额足够时几乎不应发生）
                raise HTTPException(
                    status_code=500,
                    detail=f"支付失败（部分订单已完成）: {br.get('error', '')}"
                )

            # 加卖家；失败则回滚买家
            if creator_id:
                sr = await incentive_service.adjust_user_balance(
                    user_id=creator_id,
                    delta=float(price),
                    type_="reward",
                    category="product_income",
                    description=f"商品售卖收入: {product.get('name', '')}",
                    related_id=f"income_{order_no}",
                    related_order_no=order_no,
                )
                if not sr.get("success"):
                    logger.error(
                        f"[购物车结算] 卖家入账失败，回滚买家 order_no={order_no} err={sr.get('error')}"
                    )
                    try:
                        await incentive_service.adjust_user_balance(
                            user_id=user_id,
                            delta=float(price),
                            type_="refund",
                            category="purchase_rollback",
                            description=f"商品购买回滚（卖家入账失败）: {product.get('name', '')}",
                            related_id=f"purchase_rollback_{order_no}",
                            related_order_no=order_no,
                            check_idempotent=False,
                        )
                    except Exception as _re:
                        logger.error(f"[购物车结算] 买家回滚异常: {_re}")
                    raise HTTPException(
                        status_code=500,
                        detail=f"卖家入账失败，已回滚该笔交易（前面订单已完成）: {sr.get('error', '')}"
                    )

        # 商品销售数+1（失败不阻断）
        try:
            await parse_client.update_object("Product", aid, {
                "sales": parse_client.increment(1)
            })
        except Exception as e:
            logger.warning(f"[购物车结算] 更新商品销量失败 {aid}: {e}")

        # 创建 completed 订单
        try:
            await parse_client.create_object("Order", {
                "orderNo": order_no,
                "userId": user_id,
                "productId": aid,
                "productName": product.get("name"),
                "amount": price,
                "type": "purchase",
                "paymentMethod": "balance",
                "status": "completed",
                "completedAt": datetime.now(timezone.utc).isoformat(),
                "sellerId": creator_id,
            })
        except Exception as e:
            logger.error(f"[购物车结算] 创建订单失败 {aid}: {e}")

        completed_orders.append({
            "order_no": order_no,
            "asset_id": aid,
            "name": product.get("name"),
            "amount": price,
        })
        completed_asset_ids.add(aid)

    # 4. 清理购物车：仅移除已完成/已跳过的商品
    try:
        skipped_ids = {s["asset_id"] for s in skipped}
        remaining = [
            it for it in cart_items
            if isinstance(it, dict)
            and it.get("asset_id")
            and it["asset_id"] not in completed_asset_ids
            and it["asset_id"] not in skipped_ids
        ]
        if remaining:
            try:
                ttl = await redis_client.client.ttl(cart_key)
            except Exception:
                ttl = 0
            ttl = ttl if (isinstance(ttl, int) and ttl > 0) else CART_TTL
            await redis_client.set(cart_key, json.dumps(remaining), ex=ttl)
        else:
            await redis_client.delete(cart_key)
    except Exception as e:
        logger.warning(f"[购物车结算] 清理购物车失败（忽略）: {e}")

    # 5. 返回最新余额
    try:
        buyer_after = await parse_client.get_user(user_id)
        balance_after = incentive_service._read_balance(buyer_after)
    except Exception:
        balance_after = max(balance - total_amount, 0)

    return {
        "success": True,
        "orders": completed_orders,
        "total_amount": total_amount,
        "balance_after": balance_after,
        "message": f"支付成功，共 {len(completed_orders)} 件商品",
    }