"""
认证端点 - 处理登录和Parse配置下发
"""
import uuid
import json
import secrets
import httpx
from fastapi import APIRouter, HTTPException, Depends, Request, Header
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone
from eth_account.messages import encode_defunct
from web3 import Web3

from app.core.parse_client import parse_client
from app.core.redis_client import redis_client
from app.core.email_client import email_client
from app.core.captcha import captcha_service
from app.core.security import (
    create_access_token,
    verify_jwt_token,
    generate_sms_code,
    generate_activation_token,
)
from app.core.config import settings
from app.core.logger import logger
from app.core.rate_limit import login_rate_limit, register_rate_limit, sms_rate_limit
from app.core.operation_log import log_operation
from app.core.deps import get_current_user_id

router = APIRouter()


# ============ 请求/响应模型 ============

class LoginRequest(BaseModel):
    username: str
    password: str
    captcha_id: Optional[str] = None
    captcha_text: Optional[str] = None


class PhoneLoginRequest(BaseModel):
    phone: str
    code: str


class SendSmsRequest(BaseModel):
    phone: str
    type: str = "login"  # login, register


class EmailRegisterRequest(BaseModel):
    """邮箱注册请求"""
    email: str
    password: str


class EmailLoginRequest(BaseModel):
    """邮箱登录请求"""
    email: str
    password: str
    captcha_id: Optional[str] = None
    captcha_text: Optional[str] = None


class Web3InitRequest(BaseModel):
    """Web3 登录初始化请求"""
    address: str


class Web3LoginRequest(BaseModel):
    """<Web3 登录请求"""
    address: str
    signature: str
    message: str
    password: Optional[str] = None  # 内置钱包需要密码，MetaMask 不需要
    captcha_id: Optional[str] = None  # 验证码ID（注册时必填）
    captcha_text: Optional[str] = None  # 验证码文本（注册时必填）


class Web3SimpleLoginRequest(BaseModel):
    """Web3 地址+密码简单登录请求（无需签名，适用于已注册用户）"""
    address: str
    password: str


class LoginResponse(BaseModel):
    success: bool
    token: str  # FastAPI JWT Token
    user: dict
    message: Optional[str] = None


class ParseConfigResponse(BaseModel):
    server_url: str
    app_id: str
    # 注意：不下发 Master Key，只下发 JS Key（如果需要客户端直连）
    # 但根据新架构，客户端不再直连Parse


# ============ 端点 ============

@router.get("/public-config")
async def get_public_config():
    """
    对外公开的站点配置（登录页/侧栏/页脚等使用）
    不需要鉴权。返回站点名称、Logo、备案、页脚、以及登录验证码开关等。
    敏感字段（如支付密钥、SMTP密码）不返回。
    """
    default_config = {
        "siteName": "巴特星球",
        "productName": "巴特星球",
        "slogan": "",
        "siteUrl": "",
        "contactEmail": "",
        "contactPhone": "",
        "supportEmail": "",
        "companyName": "",
        "companyAddress": "",
        "icp": "",
        "policeICP": "",
        "businessLicense": "",
        "otherLicense": "",
        "footerText": "",
        "footerCopyright": "",
        "friendLinks": "",
        "lightLogo": "",
        "darkLogo": "",
        "favicon": "",
        "loginBg": "",
        "loginCaptcha": False,
    }
    try:
        result = await parse_client.query_objects("SystemConfig", limit=100)
        configs: dict = {}
        for item in result.get("results", []):
            cat = item.get("category", "")
            if cat:
                configs[cat] = item.get("settings", {}) or {}

        general = configs.get("general", {}) or {}
        logo = configs.get("logo", {}) or {}
        security = configs.get("security", {}) or {}

        merged = {
            "siteName": general.get("siteName") or default_config["siteName"],
            "productName": logo.get("productName") or general.get("siteName") or default_config["productName"],
            "slogan": logo.get("slogan") or "",
            "siteUrl": general.get("siteUrl") or "",
            "contactEmail": general.get("contactEmail") or "",
            "contactPhone": logo.get("contactPhone") or general.get("contactPhone") or "",
            "supportEmail": logo.get("supportEmail") or "",
            "companyName": logo.get("companyName") or "",
            "companyAddress": logo.get("companyAddress") or "",
            "icp": logo.get("icp") or general.get("icp") or "",
            "policeICP": logo.get("policeICP") or "",
            "businessLicense": logo.get("businessLicense") or "",
            "otherLicense": logo.get("otherLicense") or "",
            "footerText": logo.get("footerText") or "",
            "footerCopyright": logo.get("footerCopyright") or "",
            "friendLinks": logo.get("friendLinks") or "",
            "lightLogo": logo.get("lightLogo") or "",
            "darkLogo": logo.get("darkLogo") or "",
            "favicon": logo.get("favicon") or "",
            "loginBg": logo.get("loginBg") or "",
            "loginCaptcha": bool(security.get("loginCaptcha", False)),
        }
        return {"success": True, "data": merged}
    except Exception as e:
        logger.warning(f"[public-config] 获取系统配置失败，返回默认值: {e}")
        return {"success": True, "data": default_config}


async def _is_login_captcha_enabled() -> bool:
    """读取 SystemConfig 中 security.loginCaptcha 开关"""
    try:
        existing = await parse_client.query_objects(
            "SystemConfig", where={"category": "security"}, limit=1
        )
        results = existing.get("results", [])
        if results:
            settings = results[0].get("settings", {}) or {}
            return bool(settings.get("loginCaptcha", False))
    except Exception as e:
        logger.warning(f"[Auth] 读取 loginCaptcha 开关失败: {e}")
    return False


async def _verify_login_captcha_if_enabled(captcha_id: Optional[str], captcha_text: Optional[str]) -> None:
    """如果后台开启了登录验证码，则必须校验验证码。未开启直接放行。"""
    if not await _is_login_captcha_enabled():
        return
    if not captcha_id or not captcha_text:
        raise HTTPException(status_code=400, detail="请输入图形验证码")
    ok = await captcha_service.verify(captcha_id, captcha_text)
    if not ok:
        raise HTTPException(status_code=400, detail="验证码错误或已过期")


@router.get("/captcha")
async def get_captcha():
    """
    获取图片验证码
    
    返回:
        - captcha_id: 验证码ID，用于提交时验证
        - image: data:image/png;base64,... 格式的图片
    """
    captcha_id, image_data = await captcha_service.generate()
    
    return {
        "captcha_id": captcha_id,
        "image": image_data
    }


@router.post("/login", response_model=LoginResponse, dependencies=[Depends(login_rate_limit)])
async def login(request: LoginRequest, http_request: Request):
    """
    用户登录
    - 验证用户名密码
    - 返回JWT Token和Parse配置
    """
    logger.info(f"[登录] 用户尝试登录: {request.username}")
    # 根据开关条件性校验图形验证码
    await _verify_login_captcha_if_enabled(request.captcha_id, request.captcha_text)
    try:
        # 通过Parse验证登录
        try:
            user_data = await parse_client.login_user(request.username, request.password)
        except httpx.HTTPStatusError:
            logger.warning(f"[登录] 登录失败: {request.username}")
            # 登录失败，检查用户是否存在
            try:
                existing = await parse_client.query_users(where={"username": request.username})
                if not existing.get("results"):
                    raise HTTPException(status_code=404, detail="该用户名未注册")
                else:
                    raise HTTPException(status_code=401, detail="密码错误")
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"[登录] 检查用户失败: {str(e)}")
                raise HTTPException(status_code=401, detail="用户名或密码错误")
        
        user_id = user_data.get("objectId")
        session_token = user_data.get("sessionToken")
        
        # 生成FastAPI JWT Token
        jwt_token = create_access_token(data={
            "sub": user_id,
            "username": user_data.get("username"),
            "role": user_data.get("role", "user"),
            "parse_session": session_token,  # 包含Parse session以便后续使用
        })
        
        # 更新最后登录时间（使用 session token）
        try:
            await parse_client.update_user_with_session(
                user_id, {"lastLoginAt": datetime.now(timezone.utc).isoformat()}, session_token
            )
        except Exception:
            pass
        
        # 构建用户信息（过滤敏感字段）
        safe_user = {
            "objectId": user_data.get("objectId"),
            "username": user_data.get("username"),
            "email": user_data.get("email"),
            "phone": user_data.get("phone"),
            "role": user_data.get("role", "user"),
            "level": user_data.get("level", 1),
            "memberLevel": user_data.get("memberLevel", "normal"),
            "memberExpireAt": user_data.get("memberExpireAt"),
            "coins": user_data.get("coins", 0),  # 金币余额
            "avatar": user_data.get("avatar"),
            "avatarKey": user_data.get("avatarKey"),
            "web3Address": user_data.get("web3Address"),
            "inviteCount": user_data.get("inviteCount", 0),
        }
        
        logger.info(f"[登录] 登录成功: {request.username} (ID: {user_id})")

        # 记录登录操作日志
        await log_operation(
            operator_id=user_id or "",
            action="login",
            module="auth",
            target_class="_User",
            target_id=user_id or "",
            target_name=user_data.get("username", ""),
            description=f"用户名密码登录",
            detail={"method": "username_password"},
            request=http_request,
            operator_name=user_data.get("username", ""),
            operator_role=user_data.get("role", "user"),
        )

        return LoginResponse(
            success=True,
            token=jwt_token,
            user=safe_user,
            message="登录成功"
        )
        
    except HTTPException as e:
        logger.warning(f"[登录] 登录失败: {request.username} - {e.detail}")
        raise
    except Exception as e:
        logger.error(f"[登录] 登录异常: {request.username} - {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/send-sms", dependencies=[Depends(sms_rate_limit)])
async def send_sms_code(request: SendSmsRequest):
    """
    发送短信验证码
    """
    phone = request.phone
    sms_type = request.type
    
    # 验证手机号格式
    if not phone or len(phone) != 11 or not phone.isdigit():
        raise HTTPException(status_code=400, detail="请输入有效的手机号")
    
    # 检查发送频率限制（60秒内只能发一次）
    rate_key = f"sms_rate:{phone}"
    if await redis_client.get(rate_key):
        raise HTTPException(status_code=429, detail="发送过于频繁，请60秒后重试")
    
    # 如果是注册，检查手机号是否已存在
    if sms_type == "register":
        existing = await parse_client.query_users(where={"phone": phone})
        if existing.get("results"):
            raise HTTPException(status_code=400, detail="该手机号已注册")
    
    # 如果是登录，检查手机号是否存在
    if sms_type == "login":
        existing = await parse_client.query_users(where={"phone": phone})
        if not existing.get("results"):
            raise HTTPException(status_code=400, detail="该手机号未注册")
    
    # 生成验证码
    code = generate_sms_code()
    
    # 存储验证码（5分钟有效）
    code_key = f"sms_code:{sms_type}:{phone}"
    await redis_client.set(code_key, code, ex=300)
    
    # 设置发送频率限制
    await redis_client.set(rate_key, "1", ex=60)
    
    # TODO: 实际发送短信（对接短信服务商）
    # 开发环境直接打印验证码
    print(f"[SMS] Phone: {phone}, Code: {code}, Type: {sms_type}")
    
    return {
        "success": True,
        "message": "验证码已发送",
        # 开发环境返回验证码方便测试
        "code": code if settings.debug else None
    }


@router.post("/phone-login")
async def phone_login(request: PhoneLoginRequest):
    """
    手机号验证码登录
    """
    phone = request.phone
    code = request.code
    
    # 验证验证码
    code_key = f"sms_code:login:{phone}"
    stored_code = await redis_client.get(code_key)
    
    if not stored_code or stored_code != code:
        raise HTTPException(status_code=400, detail="验证码错误或已过期")
    
    # 删除验证码
    await redis_client.delete(code_key)
    
    # 查找用户
    users = await parse_client.query_users(where={"phone": phone})
    if not users.get("results"):
        raise HTTPException(status_code=400, detail="用户不存在")
    
    user_data = users["results"][0]
    user_id = user_data.get("objectId")
    
    # 生成JWT Token
    jwt_token = create_access_token(data={
        "sub": user_id,
        "username": user_data.get("username"),
        "role": user_data.get("role", "user"),
    })
    
    # 获取 Parse session token
    session_token = user_data.get("sessionToken")
    
    # 更新最后登录时间（使用 session token）
    if session_token:
        try:
            await parse_client.update_user_with_session(
                user_id, {"lastLoginAt": datetime.now(timezone.utc).isoformat()}, session_token
            )
        except Exception:
            pass
    
    # 构建用户信息
    safe_user = {
        "objectId": user_data.get("objectId"),
        "username": user_data.get("username"),
        "email": user_data.get("email"),
        "phone": user_data.get("phone"),
        "role": user_data.get("role", "user"),
        "level": user_data.get("level", 1),
        "memberLevel": user_data.get("memberLevel", "normal"),
        "coins": user_data.get("coins", 0),
        "avatar": user_data.get("avatar"),
        "avatarKey": user_data.get("avatarKey"),
        "web3Address": user_data.get("web3Address"),
        "inviteCount": user_data.get("inviteCount", 0),
    }
    
    return {
        "success": True,
        "token": jwt_token,
        "user": safe_user,
        "message": "登录成功"
    }


@router.post("/email/register", dependencies=[Depends(register_rate_limit)])
async def email_register(request: EmailRegisterRequest):
    """
    邮箱注册
    1. 检查邮箱是否已存在
    2. 生成激活Token
    3. 存储到Redis
    4. 发送激活邮件
    """
    logger.info(f"[Auth] 邮箱注册请求: {request.email}")
    
    # 检查 Parse 是否已有该邮箱
    existing = await parse_client.query_users(where={"email": request.email})
    if existing.get("results"):
        logger.warning(f"[Auth] 注册失败: 邮箱已存在 {request.email}")
        raise HTTPException(status_code=400, detail="该邮箱已注册")
    
    # 生成激活Token
    token = generate_activation_token()
    
    # 存储注册信息到Redis (24小时有效)
    user_data = {
        "email": request.email,
        "password": request.password,
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    await redis_client.set_activation_token(token, user_data, ex=86400)
    
    # 发送激活邮件 - 链接指向前端激活页面
    frontend_url = settings.frontend_url.rstrip("/")
    activation_link = f"{frontend_url}/activate?token={token}"
    
    subject = "【巴特星球】账号激活"
    body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; text-align: center; border-radius: 10px 10px 0 0; }}
            .content {{ background: #f9f9f9; padding: 30px; border-radius: 0 0 10px 10px; }}
            .button {{ display: inline-block; background: #667eea; color: white; padding: 12px 30px; text-decoration: none; border-radius: 5px; margin: 20px 0; }}
            .footer {{ text-align: center; color: #999; margin-top: 20px; font-size: 12px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>欢迎加入巴特星球</h1>
            </div>
            <div class="content">
                <p>亲爱的用户，</p>
                <p>感谢您注册巴特星球AIGC云平台！请点击下方按钮激活您的账号：</p>
                <p style="text-align: center;">
                    <a href="{activation_link}" class="button">激活账号</a>
                </p>
                <p>或者复制以下链接到浏览器：</p>
                <p style="word-break: break-all; color: #666;">{activation_link}</p>
                <p>此链接24小时内有效。</p>
                <p>如果您没有注册过账号，请忽略此邮件。</p>
            </div>
            <div class="footer">
                <p>© 2026 巴特星球 - AIGC云平台</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    await email_client.send(request.email, subject, body)
    
    return {
        "success": True, 
        "message": "注册申请已提交，请进入邮箱点击激活链接完成注册"
    }


@router.get("/email/activate")
async def email_activate(token: str):
    """
    邮箱激活
    1. 验证Token
    2. 创建 Parse User (email作为username)
    3. 发放初始金币
    """
    logger.info(f"[Auth] 邮箱激活请求: token={token[:10]}...")
    
    user_data = await redis_client.get_activation_token(token)
    if not user_data:
        raise HTTPException(status_code=400, detail="激活链接无效或已过期")
    
    email = user_data["email"]
    password = user_data["password"]
    
    # 再次检查是否已被注册（防止在等待激活期间被注册）
    existing = await parse_client.query_users(where={"email": email})
    if existing.get("results"):
        await redis_client.delete_activation_token(token)
        raise HTTPException(status_code=400, detail="该邮箱已被激活或注册")
    
    # 在 Parse 中创建用户
    try:
        # 1. 先创建用户（不带 emailVerified，因为客户端 REST API 不允许手动设置该字段）
        create_result = await parse_client.create_user({
            "username": email,  # 邮箱作为用户名
            "email": email,
            "password": password,
            "role": "user",
            "level": 1,
            "coins": 100,  # 新用户赠送 100 金币
            "memberLevel": "normal",
        })
        
        if not create_result.get("objectId"):
            raise HTTPException(status_code=500, detail="激活失败，创建用户记录失败")
            
        user_id = create_result.get("objectId")
        
        # 2. 使用 Master Key 手动标记邮箱已验证
        try:
            await parse_client.update_user_with_master_key(user_id, {"emailVerified": True})
            logger.info(f"[Auth] 邮箱已通过 Master Key 标记为已验证: {email}")
        except Exception as e:
            logger.warning(f"[Auth] 标记邮箱验证失败 (Master Key): {str(e)}")
            # 即使这一步失败了，用户已经创建成功，不影响核心逻辑
            
        logger.info(f"[Auth] 邮箱激活成功: {email} (ID: {user_id})")
        
        # 删除 Token
        await redis_client.delete_activation_token(token)
        
        # 返回 JSON 响应（前端激活页面会调用此接口）
        return {
            "success": True,
            "message": "邮箱激活成功",
            "email": email
        }
        
    except Exception as e:
        logger.error(f"[Auth] 激活异常: {str(e)}")
        raise HTTPException(status_code=500, detail="激活过程中发生异常")


@router.post("/email/login", dependencies=[Depends(login_rate_limit)])
async def email_login(request: EmailLoginRequest):
    """
    邮箱登录
    1. 使用 Parse 登录接口验证 (email作为username)
    2. 生成 JWT
    """
    logger.info(f"[Auth] 邮箱登录请求: {request.email}")
    # 根据开关条件性校验图形验证码
    await _verify_login_captcha_if_enabled(request.captcha_id, request.captcha_text)

    try:
        user_data = await parse_client.login_user(request.email, request.password)
    except httpx.HTTPStatusError:
        logger.warning(f"[Auth] 邮箱登录失败: {request.email}")
        raise HTTPException(status_code=401, detail="邮箱或密码错误")
    
    session_token = user_data.get("sessionToken")
    user_id = user_data.get("objectId")
    
    logger.info(f"[Auth] 邮箱登录成功: {request.email} (ID: {user_id})")
    
    # 更新登录时间
    if session_token and user_id:
        await _update_last_login(user_id, session_token, request.email)
    
    # 生成 JWT
    jwt_token = create_access_token(data={
        "sub": user_id,
        "user_id": user_id,
        "email": request.email,
        "session_token": session_token,
        "parse_session": session_token,
    })
    
    # 构建响应
    safe_user, parse_config = _build_user_response(user_data, session_token, "") # 邮箱登录暂无 address
    
    return {
        "success": True,
        "token": jwt_token,
        "user": safe_user,
        "parse_config": parse_config,
        "message": "登录成功"
    }


@router.post("/logout")
async def logout(
    http_request: Request,
    user_id: str = Depends(get_current_user_id),
    parse_session: Optional[str] = Header(None, alias="X-Parse-Session-Token")
):
    """
    用户登出
    """
    if not user_id:
        raise HTTPException(status_code=401, detail="未登录")
    
    # 清除 Parse session
    if parse_session:
        success = await parse_client.logout_session(parse_session)
        if success:
            logger.info(f"[Auth] Parse session 已清除")
        else:
            logger.warning(f"[Auth] Parse session 清除失败")
    
    # 记录登出操作日志
    try:
        u = await parse_client.get_user(user_id)
        username = u.get("username", "")
        role = u.get("role", "user")
    except Exception:
        username = ""
        role = ""
    await log_operation(
        operator_id=user_id,
        action="logout",
        module="auth",
        target_class="_User",
        target_id=user_id,
        target_name=username,
        description="用户登出",
        request=http_request,
        operator_name=username,
        operator_role=role,
    )
    
    return {"success": True, "message": "登出成功"}


@router.get("/me")
async def get_current_user(token: str = Depends(verify_jwt_token)):
    """
    获取当前用户信息
    """
    if not token:
        raise HTTPException(status_code=401, detail="未登录")
    
    # token 是 user_id
    try:
        user = await parse_client.get_user(token)
        return {
            "success": True,
            "user": {
                "objectId": user.get("objectId"),
                "username": user.get("username"),
                "email": user.get("email"),
                "phone": user.get("phone"),
                "role": user.get("role", "user"),
                "level": user.get("level", 1),
                "memberLevel": user.get("memberLevel", "normal"),
                "coins": user.get("coins", 0),
                "avatar": user.get("avatar"),
                "avatarKey": user.get("avatarKey"),
                "web3Address": user.get("web3Address"),
            }
        }
    except Exception:
        raise HTTPException(status_code=404, detail="用户不存在")


@router.get("/config")
async def get_parse_config(authorization: Optional[str] = Header(None)):
    """
    获取Parse配置（需要JWT认证）
    返回 parse_config 包含 appId 和 jsKey
    """
    # 验证 JWT Token
    if not authorization or not authorization.startswith('Bearer '):
        raise HTTPException(status_code=401, detail="未提供认证Token")
    
    token = authorization.replace('Bearer ', '')
    try:
        verify_jwt_token(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Token无效")
    
    return {
        "parse_config": {
            "serverUrl": settings.parse_server_url,
            "appId": settings.parse_app_id,
            "jsKey": settings.parse_js_key,
        }
    }


@router.post("/refresh")
async def refresh_token(current_token: str = Depends(verify_jwt_token)):
    """
    刷新JWT Token
    """
    if not current_token:
        raise HTTPException(status_code=401, detail="Token无效")
    
    # 获取用户信息
    try:
        user = await parse_client.get_user(current_token)
        
        # 生成新Token
        new_token = create_access_token(data={
            "sub": user.get("objectId"),
            "username": user.get("username"),
            "role": user.get("role", "user"),
        })
        
        return {
            "success": True,
            "token": new_token,
        }
    except Exception:
        raise HTTPException(status_code=401, detail="Token无效")


# ============ Web3 认证 ============

def validate_eth_address(address: str) -> str:
    """验证并返回 checksum 格式地址"""
    try:
        return Web3.to_checksum_address(address)
    except Exception:
        raise HTTPException(status_code=400, detail="无效的钱包地址")


def verify_signature(message: str, signature: str, expected_address: str) -> str:
    """
    验证签名并返回恢复的地址 (checksum 格式)
    失败时返回空字符串
    """
    try:
        w3 = Web3()
        message_hash = encode_defunct(text=message)
        recovered = w3.eth.account.recover_message(message_hash, signature=signature)
        return Web3.to_checksum_address(recovered)
    except Exception:
        return ""


async def _generate_unique_nonce(address: str) -> str:
    """
    生成 128 位强度 nonce，使用 SETNX 原子操作确保全局唯一
    """
    max_attempts = 10
    for _ in range(max_attempts):
        # 128 位 = 16 字节 = 32 字符 hex
        nonce = secrets.token_hex(16)
        key = f"nonce:{nonce}"
        # 原子操作，仅当键不存在时设置
        if await redis_client.setnx(key, address, ex=900):  # 15 分钟有效
            return nonce
    raise HTTPException(status_code=500, detail="Nonce 生成失败，请重试")


@router.get("/web3/nonce")
async def web3_get_nonce(address: str):
    """
    申请 Nonce - Electron 客户端专用
    
    - 生成 128 位高强度随机数
    - 存入 Redis，15 分钟有效
    - 返回 nonce 和过期时间
    """
    logger.info(f"[Web3] 申请Nonce: {address[:10]}...")
    
    address = validate_eth_address(address)
    nonce = await _generate_unique_nonce(address)
    
    logger.info(f"[Web3] Nonce已生成: {address[:10]}... -> {nonce[:8]}...")
    return {
        "success": True,
        "nonce": nonce,
        "expires_in": 900,  # 15分钟
        "message": f"Sign in to AIGCCloud: {nonce}"
    }


# 带密码的签名验证逻辑
async def _verify_web3_signature(request: Web3LoginRequest):
    """
    Web3 签名验证逻辑 (带密码的前端接口)
    返回: (address, username)
    """
    # 1. 标准化地址
    address = validate_eth_address(request.address)
    
    # 2. 从 message 中提取 nonce
    # 消息格式: "Sign in to AIGCCloud: {nonce}"
    nonce_from_message = None
    if ": " in request.message:
        nonce_from_message = request.message.split(": ")[-1].strip()
    
    # 3. 尝试新格式验证: nonce:{nonce} -> address
    used_key = None
    is_new_format = False
    
    if nonce_from_message:
        new_key = f"nonce:{nonce_from_message}"
        stored_address = await redis_client.get(new_key)
        if stored_address:
            # 新格式：验证地址匹配
            if stored_address.lower() != address.lower():
                logger.warning(f"[Web3] Nonce地址不匹配: {address[:10]}...")
                raise HTTPException(status_code=400, detail="无效的签名消息")
            used_key = new_key
            is_new_format = True
    
    # 4. 尝试旧格式验证: web3_nonce:{address} -> nonce
    if not used_key:
        old_key = f"web3_nonce:{address.lower()}"
        stored_nonce = await redis_client.get(old_key)
        if stored_nonce:
            # 旧格式：验证 message 包含 nonce
            if stored_nonce not in request.message:
                raise HTTPException(status_code=400, detail="无效的签名消息")
            used_key = old_key
    
    if not used_key:
        logger.warning(f"[Web3] Nonce过期或不存在: {address[:10]}...")
        raise HTTPException(status_code=400, detail="验证已过期，请重新获取")
    
    # 5. 验证签名
    recovered = verify_signature(request.message, request.signature, address)
    if not recovered or recovered.lower() != address.lower():
        logger.warning(f"[Web3] 签名验证失败: {address[:10]}...")
        raise HTTPException(status_code=400, detail="签名验证失败")
    
    # 6. 删除 nonce（一次性使用，防止重放攻击）
    await redis_client.delete(used_key)
    
    # 7. 验证密码
    if not request.password:
        raise HTTPException(status_code=400, detail="请输入登录密码")
    if len(request.password) < 6:
        raise HTTPException(status_code=400, detail="登录密码至少6位")
    
    username = address.lower()
    return address, username

async def _update_last_login(user_id: str, session_token: str, address: str):
    """更新最后登录时间（使用 session token）"""
    try:
        await parse_client.update_user_with_session(
            user_id, {"lastLoginAt": datetime.now(timezone.utc).isoformat()}, session_token
        )
        logger.debug(f"[Web3] 更新登录时间成功: {address[:10]}...")
    except Exception as e:
        logger.warning(f"[Web3] 更新登录时间失败: {e}")


def _build_user_response(user_data: dict, session_token: str, address: str):
    """构建用户响应数据"""
    user_id = user_data.get("objectId")
    # web3Address: 优先使用传入的 address，否则使用 user_data 中的值
    web3_address = address if address else user_data.get("web3Address")
    safe_user = {
        "objectId": user_id,
        "sessionToken": session_token,
        "username": user_data.get("username"),
        "email": user_data.get("email"),
        "phone": user_data.get("phone"),
        "role": user_data.get("role", "user"),
        "level": user_data.get("level", 1),
        "memberLevel": user_data.get("memberLevel", "normal"),
        "memberExpireAt": user_data.get("memberExpireAt"),
        "coins": user_data.get("coins", 0),
        "avatar": user_data.get("avatar"),
        "avatarKey": user_data.get("avatarKey"),
        "web3Address": web3_address,
        "inviteCount": user_data.get("inviteCount", 0),
    }
    # Parse 配置 - 登录后动态下发，客户端无需静态配置
    parse_config = {
        "serverUrl": settings.parse_server_url,
        "appId": settings.parse_app_id,        # X-Parse-Application-Id
        "jsKey": settings.parse_js_key,         # X-Parse-Javascript-Key
    }
    return safe_user, parse_config


@router.post("/web3/register", dependencies=[Depends(register_rate_limit)])
async def web3_register(request: Web3LoginRequest):
    """
    Web3 注册 - 验证签名并创建新用户
    
    流程：
    1. 验证验证码
    2. 验证签名
    3. 创建 Parse User
    4. 返回 session token
    """
    
    logger.info(f"[Web3] 注册请求: {request.address[:10]}...")
    
    # 1. 验证验证码（仅当传入验证码参数时才验证，导入私钥/助记词方式不需要验证码）
    if request.captcha_id and request.captcha_text:
        is_valid = await captcha_service.verify(request.captcha_id, request.captcha_text)
        if not is_valid:
            raise HTTPException(status_code=400, detail="验证码错误或已过期")
    
    # 2. 验证签名
    address, username = await _verify_web3_signature(request)
    
    # 创建新用户
    try:
        create_result = await parse_client.create_user({
            "username": username,
            "password": request.password,
            "web3Address": address,
            "role": "user",
            "level": 1,
            "coins": 100,  # 新用户赠送 100 金币
            "memberLevel": "normal",
        })
        
        if not create_result.get("objectId"):
            raise HTTPException(status_code=500, detail="创建用户失败")
        
        user_data = create_result
        session_token = create_result.get("sessionToken")
        user_id = create_result.get("objectId")
        
        logger.info(f"[Web3] 注册成功: {address[:10]}... (ID: {user_id})")
        
        # 发放注册激励奖励
        try:
            from app.core.incentive_service import incentive_service
            await incentive_service.grant_register_reward(user_id)
            logger.info(f"[Web3] 注册激励发放成功: {user_id}")
        except Exception as e:
            logger.error(f"[Web3] 注册激励发放失败: {e}")
        
    except httpx.HTTPStatusError as e:
        error_data = e.response.json() if e.response.headers.get("content-type", "").startswith("application/json") else {}
        if error_data.get("code") == 202:  # 用户已存在
            logger.warning(f"[Web3] 用户已存在: {address[:10]}...")
            raise HTTPException(status_code=400, detail="该地址已注册，请直接登录")
        raise
    
    # 更新登录时间
    if session_token and user_id:
        await _update_last_login(user_id, session_token, address)
    
    # 生成 JWT（包含 session_token）
    jwt_token = create_access_token(data={
        "sub": user_id,
        "user_id": user_id,
        "address": address,
        "session_token": session_token,
        "parse_session": session_token,
    })
    
    # 构建响应
    safe_user, parse_config = _build_user_response(user_data, session_token, address)
    
    return {
        "success": True,
        "token": jwt_token,  # 返回 JWT
        "user": safe_user,
        "parse_config": parse_config,
        "is_new_user": True,
        "message": "注册成功"
    }


@router.post("/web3/login", dependencies=[Depends(login_rate_limit)])
async def web3_login(request: Web3LoginRequest):
    """
    Web3 登录 - 验证签名并登录已有用户
    
    流程：
    1. 验证签名
    2. 登录 Parse User
    3. 返回 session token
    """
    
    logger.info(f"[Web3] 登录请求: {request.address[:10]}...")
    
    # 验证签名
    address, username = await _verify_web3_signature(request)
    
    # 登录 Parse
    logger.debug(f"[Web3] 登录Parse: username={username}")
    
    try:
        user_data = await parse_client.login_user(username, request.password)
    except httpx.HTTPStatusError as e:
        logger.warning(f"[Web3] 登录失败: {address[:10]}...")
        try:
            error_data = e.response.json()
            error_code = error_data.get("code")
            if error_code == 101:
                raise HTTPException(status_code=401, detail="该地址未注册或密码错误")
            raise HTTPException(status_code=401, detail=error_data.get("error", "登录失败"))
        except (ValueError, AttributeError):
            raise HTTPException(status_code=401, detail="登录失败，请检查账户和密码")
    
    session_token = user_data.get("sessionToken")
    user_id = user_data.get("objectId")
    
    logger.info(f"[Web3] 登录成功: {address[:10]}... (ID: {user_id})")
    
    # 更新登录时间
    if session_token and user_id:
        await _update_last_login(user_id, session_token, address)
    
    # 生成 JWT（包含 session_token）
    jwt_token = create_access_token(data={
        "sub": user_id,
        "user_id": user_id,
        "address": address,
        "session_token": session_token,
        "parse_session": session_token,
    })
    
    # 构建响应
    safe_user, parse_config = _build_user_response(user_data, session_token, address)
    
    return {
        "success": True,
        "token": jwt_token,
        "user": safe_user,
        "parse_config": parse_config,
        "is_new_user": False,
        "message": "登录成功"
    }



@router.post("/web3/simple-login", dependencies=[Depends(login_rate_limit)])
async def web3_simple_login(request: Web3SimpleLoginRequest):
    """
    Web3 简单登录 - 仅需地址+密码（无需签名）
    适用于已注册用户的快速登录，免去私钥签名步骤
    """
    
    logger.info(f"[Web3] 简单登录请求: {request.address[:10]}...")
    
    address = validate_eth_address(request.address)
    username = address.lower()
    
    if not request.password or len(request.password) < 6:
        raise HTTPException(status_code=400, detail="密码至少6位")
    
    # 直接调用 Parse 登录
    try:
        user_data = await parse_client.login_user(username, request.password)
    except httpx.HTTPStatusError as e:
        try:
            error_data = e.response.json()
            error_code = error_data.get("code")
            if error_code == 101:
                raise HTTPException(status_code=401, detail="地址未注册或密码错误")
            raise HTTPException(status_code=401, detail=error_data.get("error", "登录失败"))
        except (ValueError, AttributeError):
            raise HTTPException(status_code=401, detail="登录失败，请检查账户和密码")
    
    session_token = user_data.get("sessionToken")
    user_id = user_data.get("objectId")
    
    logger.info(f"[Web3] 简单登录成功: {address[:10]}... (ID: {user_id})")
    
    if session_token and user_id:
        await _update_last_login(user_id, session_token, address)
    
    jwt_token = create_access_token(data={
        "sub": user_id,
        "user_id": user_id,
        "address": address,
        "session_token": session_token,
        "parse_session": session_token,
    })
    
    safe_user, parse_config = _build_user_response(user_data, session_token, address)
    
    return {
        "success": True,
        "token": jwt_token,
        "user": safe_user,
        "parse_config": parse_config,
        "is_new_user": False,
        "message": "登录成功"
    }


@router.post("/web3/logout")
async def web3_logout(
    authorization: Optional[str] = Header(None)
):
    """
    Web3 登出 - 清除 JWT 和 Parse sessionToken
    
    流程：
    1. 验证 JWT ，获取 session_token
    2. 调用 Parse Server 撤销 sessionToken
    3. 客户端清除本地存储
    """
    
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="未提供认证Token")
    
    token = authorization[7:]
    
    try:
        # 解析 JWT 获取 session_token
        from app.core.security import decode_access_token
        payload = decode_access_token(token)
        if not payload:
            raise HTTPException(status_code=401, detail="Token无效")
            
        session_token = payload.get("session_token") or payload.get("parse_session")
        
        if not session_token:
            logger.warning("[Web3] 登出失败: JWT 中未找到 session_token")
            raise HTTPException(status_code=400, detail="无效的认证Token")
        
        # 调用 Parse Server 登出接口撤销 sessionToken
        success = await parse_client.logout_session(session_token)
        
        if success:
            logger.info(f"[Web3] 登出成功: session_token={session_token[:20]}...")
        else:
            logger.warning(f"[Web3] Parse 登出失败")
        
        # 可选：将 JWT 加入黑名单
        # await redis_client.set(f"jwt_blacklist:{token}", "1", ex=86400)
        
        return {
            "success": True,
            "message": "登出成功"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[Web3] 登出异常: {e}")
        raise HTTPException(status_code=500, detail="登出失败")
