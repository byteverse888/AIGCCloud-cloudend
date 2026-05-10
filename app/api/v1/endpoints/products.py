"""
商品管理端点 - 审核、举报等
"""
import asyncio
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional, List
from enum import Enum
from datetime import datetime, timezone

from app.core.parse_client import parse_client
from app.core.email_client import email_client
from app.core.deps import get_current_user_id, get_admin_user_id, get_operator_user_id

router = APIRouter()


# ============ 枚举与模型 ============

class ProductStatus(str, Enum):
    DRAFT = "draft"
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    OFFLINE = "offline"


class ProductCategory(str, Enum):
    IMAGE = "image"
    AUDIO = "audio"
    VIDEO = "video"
    MODEL = "model"
    OTHER = "other"


class ReviewProductRequest(BaseModel):
    product_id: str
    status: ProductStatus
    review_note: Optional[str] = None


class ReportProductRequest(BaseModel):
    product_id: str
    reason: str
    description: Optional[str] = None


class BatchReviewRequest(BaseModel):
    product_ids: List[str]
    status: ProductStatus
    review_note: Optional[str] = None


# 举报原因
REPORT_REASONS = {
    "copyright": "侵权/盗版",
    "inappropriate": "不当内容",
    "fraud": "虚假信息",
    "spam": "垃圾广告",
    "other": "其他",
}

# 自动下架阈值
AUTO_OFFLINE_THRESHOLD = 5


# ============ 端点 ============

@router.post("/review")
async def review_product(
    request: ReviewProductRequest,
    user_id: str = Depends(get_operator_user_id)
):
    """
    审核商品(管理员/运营人员)
    """
    # 获取商品
    try:
        product = await parse_client.get_object("Product", request.product_id)
    except Exception:
        raise HTTPException(status_code=404, detail="商品不存在")
    
    # 更新商品状态
    update_data = {
        "status": request.status,
        "reviewedAt": datetime.now(timezone.utc).isoformat(),
        "reviewedBy": user_id,
    }
    if request.review_note:
        update_data["reviewNote"] = request.review_note
    
    await parse_client.update_object("Product", request.product_id, update_data)
    
    # 创建审核记录
    await parse_client.create_object("ProductReview", {
        "productId": request.product_id,
        "operatorId": user_id,
        "status": request.status,
        "note": request.review_note,
    })
    
    # 发送通知给创作者
    creator_id = product.get("creatorId")
    if creator_id:
        try:
            creator = await parse_client.get_user(creator_id)
            await email_client.send_product_review_notification(
                to=creator.get("email"),
                username=creator.get("username"),
                product_name=product.get("name"),
                status=request.status,
                note=request.review_note
            )
        except Exception:
            pass  # 邮件发送失败不影响主流程
    
    return {
        "success": True,
        "product_id": request.product_id,
        "status": request.status,
    }


@router.post("/batch-review")
async def batch_review_products(
    request: BatchReviewRequest,
    user_id: str = Depends(get_operator_user_id)
):
    """
    批量审核商品(管理员/运营人员) - 并发执行
    """
    async def _review_one(product_id: str) -> dict:
        try:
            await parse_client.update_object("Product", product_id, {
                "status": request.status,
                "reviewedAt": datetime.now(timezone.utc).isoformat(),
                "reviewedBy": user_id,
                "reviewNote": request.review_note,
            })
            return {"product_id": product_id, "success": True}
        except Exception as e:
            return {"product_id": product_id, "success": False, "error": str(e)}

    results = await asyncio.gather(*[_review_one(pid) for pid in request.product_ids])

    return {
        "success": True,
        "results": results,
        "total": len(request.product_ids),
        "success_count": sum(1 for r in results if r["success"])
    }


@router.post("/report")
async def report_product(
    request: ReportProductRequest,
    user_id: str = Depends(get_current_user_id)
):
    """
    举报商品
    """
    # 检查商品是否存在
    try:
        product = await parse_client.get_object("Product", request.product_id)
    except Exception:
        raise HTTPException(status_code=404, detail="商品不存在")
    
    # 检查是否已举报过
    existing = await parse_client.query_objects(
        "ProductReport",
        where={"productId": request.product_id, "reporterId": user_id}
    )
    if existing.get("results"):
        raise HTTPException(status_code=400, detail="您已举报过此商品")
    
    # 创建举报记录
    await parse_client.create_object("ProductReport", {
        "productId": request.product_id,
        "reporterId": user_id,
        "reason": request.reason,
        "description": request.description,
        "status": "pending",  # pending, processed, dismissed
    })
    
    # 更新商品举报计数
    await parse_client.update_object("Product", request.product_id, {
        "reportCount": parse_client.increment(1)
    })
    
    # 检查是否达到自动下架阈值
    report_count = product.get("reportCount", 0) + 1
    if report_count >= AUTO_OFFLINE_THRESHOLD:
        await parse_client.update_object("Product", request.product_id, {
            "status": ProductStatus.OFFLINE,
            "offlineReason": "举报次数过多，自动下架待审核"
        })
    
    return {
        "success": True,
        "message": "举报已提交，我们将尽快处理",
    }


@router.get("/pending")
async def get_pending_products(
    page: int = 1,
    limit: int = 20,
    category: Optional[str] = None,
    user_id: str = Depends(get_operator_user_id)
):
    """
    获取待审核商品列表
    """
    where = {"status": ProductStatus.PENDING}
    if category:
        where["category"] = category
    
    skip = (page - 1) * limit
    result = await parse_client.query_objects(
        "Product",
        where=where,
        order="createdAt",  # 按创建时间升序，先提交的先审核
        limit=limit,
        skip=skip
    )
    
    total = await parse_client.count_objects("Product", where)
    
    return {
        "data": result.get("results", []),
        "total": total,
        "page": page,
        "limit": limit,
    }


@router.get("/reports")
async def get_product_reports(
    page: int = 1,
    limit: int = 20,
    status: Optional[str] = None,
    admin_id: str = Depends(get_admin_user_id)
):
    """
    获取举报列表(管理员)
    """
    where = {}
    if status:
        where["status"] = status
    
    skip = (page - 1) * limit
    result = await parse_client.query_objects(
        "ProductReport",
        where=where if where else None,
        order="-createdAt",
        limit=limit,
        skip=skip
    )
    
    total = await parse_client.count_objects("ProductReport", where if where else None)
    
    # 丰富举报信息
    reports = []
    for report in result.get("results", []):
        # 获取商品信息
        try:
            product = await parse_client.get_object("Product", report["productId"])
            report["product"] = {
                "name": product.get("name"),
                "cover": product.get("cover"),
                "status": product.get("status"),
            }
        except Exception:
            report["product"] = None
        
        # 获取举报人信息
        try:
            reporter = await parse_client.get_user(report["reporterId"])
            report["reporter"] = {
                "username": reporter.get("username"),
            }
        except Exception:
            report["reporter"] = None
        
        report["reason_text"] = REPORT_REASONS.get(report.get("reason"), report.get("reason"))
        reports.append(report)
    
    return {
        "data": reports,
        "total": total,
        "page": page,
        "limit": limit,
    }


@router.post("/reports/{report_id}/process")
async def process_report(
    report_id: str,
    action: str,  # approve, dismiss
    note: Optional[str] = None,
    admin_id: str = Depends(get_admin_user_id)
):
    """
    处理举报(管理员)
    """
    # 获取举报记录
    try:
        report = await parse_client.get_object("ProductReport", report_id)
    except Exception:
        raise HTTPException(status_code=404, detail="举报记录不存在")
    
    if action == "approve":
        # 认定举报有效，下架商品
        await parse_client.update_object("Product", report["productId"], {
            "status": ProductStatus.OFFLINE,
            "offlineReason": f"举报属实: {report.get('reason')}"
        })
        status = "processed"
    elif action == "dismiss":
        # 驳回举报
        status = "dismissed"
    else:
        raise HTTPException(status_code=400, detail="无效的操作")
    
    # 更新举报状态
    await parse_client.update_object("ProductReport", report_id, {
        "status": status,
        "processedAt": datetime.now(timezone.utc).isoformat(),
        "processedBy": admin_id,
        "processNote": note,
    })
    
    return {
        "success": True,
        "report_id": report_id,
        "action": action,
    }


@router.get("/stats")
async def get_product_stats(user_id: str = Depends(get_operator_user_id)):
    """
    获取商品统计数据(管理员)
    """
    stats = {}
    
    # 各状态商品数量
    for status in ProductStatus:
        count = await parse_client.count_objects("Product", {"status": status.value})
        stats[f"status_{status.value}"] = count
    
    # 待处理举报数
    pending_reports = await parse_client.count_objects("ProductReport", {"status": "pending"})
    stats["pending_reports"] = pending_reports
    
    # 总商品数
    total = await parse_client.count_objects("Product")
    stats["total"] = total
    
    return stats


# 注：点赞/收藏/评论等简单CRUD操作已迁移至前端 Server Actions
# 参见 aigccloud/src/lib/parse-actions.ts
