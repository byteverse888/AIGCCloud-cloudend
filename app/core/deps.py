"""
依赖注入
"""
from typing import Optional, Dict, Any
from fastapi import Header, HTTPException, status
from app.core.security import verify_jwt_token
from app.core.parse_client import parse_client


async def get_current_user_id(
    authorization: Optional[str] = Header(None, alias="Authorization")
) -> str:
    """从Authorization Header获取当前用户ID"""
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authorization header"
        )
    
    # Bearer token
    if authorization.startswith("Bearer "):
        token = authorization[7:]
    else:
        token = authorization
    
    user_id = verify_jwt_token(token)
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token"
        )
    
    return user_id


async def get_optional_user_id(
    authorization: Optional[str] = Header(None, alias="Authorization")
) -> Optional[str]:
    """可选的用户ID获取"""
    if not authorization:
        return None
    
    if authorization.startswith("Bearer "):
        token = authorization[7:]
    else:
        token = authorization
    
    return verify_jwt_token(token)


async def verify_admin_user(
    user_id: str
) -> bool:
    """验证是否为管理员用户"""
    try:
        user = await parse_client.get_user(user_id)
        return user.get("role") == "admin"
    except Exception:
        return False


async def get_admin_user_id(
    authorization: Optional[str] = Header(None, alias="Authorization")
) -> str:
    """获取并验证管理员用户ID"""
    user_id = await get_current_user_id(authorization)
    
    if not await verify_admin_user(user_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin permission required"
        )
    
    return user_id


async def verify_operator_user(user_id: str) -> bool:
    """验证是否为运营人员或管理员"""
    try:
        user = await parse_client.get_user(user_id)
        role = user.get("role")
        return role in ["operator", "admin"]
    except Exception:
        return False


async def get_operator_user_id(
    authorization: Optional[str] = Header(None, alias="Authorization")
) -> str:
    """获取并验证运营人员用户ID"""
    user_id = await get_current_user_id(authorization)
    
    if not await verify_operator_user(user_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Operator permission required"
        )
    
    return user_id


# ============ Parse Session Token 验证 ============

async def get_parse_user(
    parse_session: Optional[str] = Header(None, alias="X-Parse-Session-Token"),
    user_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    通过 Parse Session Token 获取并验证用户
    
    Args:
        parse_session: Parse session token（从请求头获取）
        user_id: 预期的用户 ID，用于验证身份匹配
        
    Returns:
        用户信息字典
        
    Raises:
        HTTPException: 未提供 token、token 无效或用户不匹配
    """
    if not parse_session:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="未提供会话令牌"
        )
    
    try:
        user = await parse_client.validate_session(parse_session, user_id)
        return user
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e)
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="会话验证失败"
        )


async def get_optional_parse_user(
    parse_session: Optional[str] = Header(None, alias="X-Parse-Session-Token")
) -> Optional[Dict[str, Any]]:
    """
    可选的 Parse Session Token 验证
    
    如果提供了 token 则验证并返回用户信息，否则返回 None
    """
    if not parse_session:
        return None
    
    try:
        return await parse_client.get_current_user(parse_session)
    except Exception:
        return None
