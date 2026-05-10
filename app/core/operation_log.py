"""
操作日志工具

统一记录管理/运营/登录登出等关键操作到 Parse Server 的 OperationLog 类。
记录失败不得阻塞主业务流程（仅输出 warning）。
查询接口本身不应调用此工具。
"""
from typing import Any, Optional

from fastapi import Request

from app.core.parse_client import parse_client
from app.core.logger import logger


def _extract_request_meta(request: Optional[Request]) -> dict:
    """从 Request 抽取 IP 与 UserAgent"""
    if not request:
        return {"ipAddress": "", "userAgent": ""}
    try:
        # 优先看反代头，没有则 client.host
        xff = request.headers.get("x-forwarded-for", "")
        real_ip = request.headers.get("x-real-ip", "")
        if xff:
            ip = xff.split(",")[0].strip()
        elif real_ip:
            ip = real_ip.strip()
        else:
            ip = request.client.host if request.client else ""
        ua = request.headers.get("user-agent", "")
        return {"ipAddress": ip[:64], "userAgent": ua[:255]}
    except Exception:
        return {"ipAddress": "", "userAgent": ""}


async def _resolve_operator(operator_id: str) -> dict:
    """根据 operator_id 查出 username/role（失败时返回空值）"""
    if not operator_id:
        return {"operatorName": "anonymous", "operatorRole": ""}
    try:
        user = await parse_client.get_user(operator_id)
        return {
            "operatorName": user.get("username") or "",
            "operatorRole": user.get("role") or "",
        }
    except Exception:
        return {"operatorName": "", "operatorRole": ""}


async def log_operation(
    *,
    operator_id: str,
    action: str,
    module: str,
    target_class: str = "",
    target_id: str = "",
    target_name: str = "",
    description: str = "",
    detail: Optional[dict] = None,
    status: str = "success",
    error_message: str = "",
    request: Optional[Request] = None,
    operator_name: Optional[str] = None,
    operator_role: Optional[str] = None,
) -> None:
    """
    写入一条操作日志。失败只打 warning，不抛异常。

    - action: create/update/delete/login/logout/ban/unban/reset_password/recharge/review ...
    - module: users/auth/products/orders/system ...
    """
    try:
        if operator_name is None or operator_role is None:
            info = await _resolve_operator(operator_id)
            operator_name = operator_name if operator_name is not None else info.get("operatorName", "")
            operator_role = operator_role if operator_role is not None else info.get("operatorRole", "")

        meta = _extract_request_meta(request)
        data: dict[str, Any] = {
            "operatorId": operator_id or "",
            "operatorName": operator_name or "",
            "operatorRole": operator_role or "",
            "action": action,
            "module": module,
            "targetClass": target_class or "",
            "targetId": target_id or "",
            "targetName": target_name or "",
            "description": description or "",
            "detail": detail or {},
            "ipAddress": meta["ipAddress"],
            "userAgent": meta["userAgent"],
            "status": status,
            "errorMessage": error_message or "",
        }
        await parse_client.create_object("OperationLog", data)
    except Exception as e:
        logger.warning(f"[OperationLog] 写入操作日志失败: {e}")
