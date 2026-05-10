"""
AI资产发布与购买接口
"""
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timezone
import uuid

from app.core.parse_client import parse_client
from app.core.deps import get_current_user_id
from app.core.security import generate_order_no
from app.core.logger import logger

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
    """编辑AI资产"""
    try:
        asset = await parse_client.get_object("Product", asset_id)
    except Exception:
        raise HTTPException(status_code=404, detail="资产不存在")
    
    creator_id = asset.get("creatorId")
    owner = asset.get("owner")
    if creator_id != user_id and owner != user_id:
        raise HTTPException(status_code=403, detail="无权编辑")
    
    if asset.get("status") not in ["draft", ""]:
        raise HTTPException(status_code=400, detail="只有草稿状态可编辑")
    
    update_data = {k: v for k, v in request.dict(exclude_unset=True).items() if v is not None}
    
    if request.tags is not None:
        update_data["tags"] = request.tags
    
    if update_data:
        await parse_client.update_object("Product", asset_id, update_data)
    
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


@router.get("/cart")
async def get_cart(user_id: str = Depends(get_current_user_id)):
    """获取购物车"""
    cart_key = f"cart:{user_id}"
    cart_data = await parse_client.redis.get(cart_key)
    if not cart_data:
        return {"data": [], "total": 0}
    
    import json
    cart_items = json.loads(cart_data)
    
    result = []
    for item in cart_items:
        try:
            product = await parse_client.get_object("Product", item["asset_id"])
            if product and product.get("status") == "approved":
                result.append({
                    "asset_id": item["asset_id"],
                    "name": product.get("name"),
                    "price": product.get("price", 0),
                    "coverKey": product.get("coverKey"),
                    "addedAt": item.get("addedAt"),
                })
        except:
            pass
    
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
    cart_data = await parse_client.redis.get(cart_key)
    
    import json
    cart_items = json.loads(cart_data) if cart_data else []
    
    for item in cart_items:
        if item["asset_id"] == asset_id:
            raise HTTPException(status_code=400, detail="该资产已在购物车中")
    
    cart_items.append({
        "asset_id": asset_id,
        "addedAt": datetime.now(timezone.utc).isoformat()
    })
    
    await parse_client.redis.setex(cart_key, CART_TTL, json.dumps(cart_items))
    
    return {"success": True, "message": "已添加到购物车", "count": len(cart_items)}


@router.delete("/cart/{asset_id}")
async def remove_from_cart(asset_id: str, user_id: str = Depends(get_current_user_id)):
    """从购物车移除"""
    cart_key = f"cart:{user_id}"
    cart_data = await parse_client.redis.get(cart_key)
    if not cart_data:
        raise HTTPException(status_code=400, detail="购物车为空")
    
    import json
    cart_items = json.loads(cart_data)
    cart_items = [item for item in cart_items if item["asset_id"] != asset_id]
    
    await parse_client.redis.setex(cart_key, CART_TTL, json.dumps(cart_items))
    
    return {"success": True, "message": "已从购物车移除", "count": len(cart_items)}


@router.post("/cart/checkout")
async def checkout_cart(user_id: str = Depends(get_current_user_id)):
    """购物车结算"""
    cart_key = f"cart:{user_id}"
    cart_data = await parse_client.redis.get(cart_key)
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
    
    await parse_client.redis.delete(cart_key)
    
    return {
        "success": True,
        "orders": orders,
        "total": total_amount,
        "message": f"已创建 {len(orders)} 个订单"
    }