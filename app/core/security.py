"""
安全工具：JWT、密码哈希、Token生成等
"""
import secrets
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any
from jose import jwt, JWTError
from passlib.context import CryptContext
from app.core.config import settings


# 密码哈希上下文
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# bcrypt 工作因子最大接受 72 字节；新版不再自动截断，统一在应用层防御截断以防止 ValueError
_BCRYPT_MAX_BYTES = 72


def _truncate_for_bcrypt(password: str) -> str:
    if not password:
        return password
    raw = password.encode("utf-8")
    if len(raw) <= _BCRYPT_MAX_BYTES:
        return password
    # 安全截断到 72 字节，遵循 UTF-8 边界
    return raw[:_BCRYPT_MAX_BYTES].decode("utf-8", errors="ignore")


def hash_password(password: str) -> str:
    """哈希密码"""
    return pwd_context.hash(_truncate_for_bcrypt(password))


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """验证密码"""
    return pwd_context.verify(_truncate_for_bcrypt(plain_password), hashed_password)


def generate_token(length: int = 32) -> str:
    """生成随机Token"""
    return secrets.token_urlsafe(length)


def generate_activation_token() -> str:
    """生成账号激活Token"""
    return generate_token(48)


def generate_reset_token() -> str:
    """生成密码重置Token"""
    return generate_token(32)


def generate_sms_code(length: int = 6) -> str:
    """生成短信验证码"""
    return ''.join([str(secrets.randbelow(10)) for _ in range(length)])


def generate_order_no(prefix: str = "ORD") -> str:
    """生成订单号"""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    random_suffix = secrets.token_hex(4).upper()
    return f"{prefix}{timestamp}{random_suffix}"


def generate_task_id() -> str:
    """生成任务ID"""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    random_suffix = secrets.token_hex(6)
    return f"task_{timestamp}_{random_suffix}"


# ============ JWT 相关 ============

def create_access_token(
    data: Dict[str, Any],
    expires_delta: Optional[timedelta] = None
) -> str:
    """创建访问Token"""
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=settings.jwt_access_token_expire_minutes))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def decode_access_token(token: str) -> Optional[Dict[str, Any]]:
    """解码访问Token"""
    try:
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
        return payload
    except JWTError:
        return None


def verify_jwt_token(token: str) -> Optional[str]:
    """验证JWT Token，返回用户ID"""
    payload = decode_access_token(token)
    if payload is None:
        return None
    return payload.get("sub")


# ============ Web3 相关 ============

def is_valid_ethereum_address(address: str) -> bool:
    """验证以太坊地址格式"""
    from web3 import Web3
    return Web3.is_address(address) if address else False


def checksum_address(address: str) -> str:
    """转换为 EIP-55 校验和地址格式（使用 Web3 标准实现）"""
    from web3 import Web3
    if not is_valid_ethereum_address(address):
        return address
    return Web3.to_checksum_address(address)


# ============ 签名验证 ============

def generate_sign(params: Dict[str, Any], secret: str) -> str:
    """生成签名(用于支付回调验证等)"""
    sorted_items = sorted(params.items())
    sign_str = "&".join(f"{k}={v}" for k, v in sorted_items if v)
    sign_str += f"&key={secret}"
    return hashlib.md5(sign_str.encode()).hexdigest().upper()


def verify_sign(params: Dict[str, Any], sign: str, secret: str) -> bool:
    """验证签名"""
    expected_sign = generate_sign(params, secret)
    return sign == expected_sign
