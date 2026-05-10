"""
操作日志查询接口

- admin 可查看全部日志
- operator 只能查看自己产生的日志
- 查询动作本身不记录日志
"""
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException

from app.core.deps import get_operator_user_id
from app.core.parse_client import parse_client
from app.core.logger import logger

router = APIRouter()


@router.get("")
async def list_operation_logs(
    page: int = 1,
    limit: int = 20,
    action: Optional[str] = None,
    module: Optional[str] = None,
    operator_id: Optional[str] = None,
    keyword: Optional[str] = None,
    operator_user_id: str = Depends(get_operator_user_id),
):
    """
    查询操作日志列表

    - admin：可查全部，支持按 operator_id 过滤
    - operator：仅能查自己的日志（强制 operatorId=self）
    """
    # 当前用户角色
    try:
        me = await parse_client.get_user(operator_user_id)
        my_role = me.get("role", "")
    except Exception:
        raise HTTPException(status_code=401, detail="无法读取当前用户")

    where: dict = {}

    # 非 admin 仅能看自己的记录
    if my_role != "admin":
        where["operatorId"] = operator_user_id
    elif operator_id:
        where["operatorId"] = operator_id

    if action:
        where["action"] = action
    if module:
        where["module"] = module
    if keyword:
        # 简单关键字匹配 description（Parse Server 支持 $regex）
        where["description"] = {"$regex": keyword, "$options": "i"}

    skip = (page - 1) * limit
    try:
        result = await parse_client.query_objects(
            "OperationLog",
            where=where if where else None,
            order="-createdAt",
            limit=limit,
            skip=skip,
        )
        total = await parse_client.count_objects("OperationLog", where if where else None)
    except Exception as e:
        logger.error(f"[OperationLog] 查询失败: {e}")
        raise HTTPException(status_code=500, detail=f"查询操作日志失败: {e}")

    logs = []
    for it in result.get("results", []):
        logs.append({
            "objectId": it.get("objectId"),
            "operatorId": it.get("operatorId", ""),
            "operatorName": it.get("operatorName", ""),
            "operatorRole": it.get("operatorRole", ""),
            "action": it.get("action", ""),
            "module": it.get("module", ""),
            "targetClass": it.get("targetClass", ""),
            "targetId": it.get("targetId", ""),
            "targetName": it.get("targetName", ""),
            "description": it.get("description", ""),
            "detail": it.get("detail") or {},
            "ipAddress": it.get("ipAddress", ""),
            "userAgent": it.get("userAgent", ""),
            "status": it.get("status", "success"),
            "errorMessage": it.get("errorMessage", ""),
            "createdAt": it.get("createdAt", ""),
        })

    return {
        "data": logs,
        "total": total,
        "page": page,
        "limit": limit,
    }
