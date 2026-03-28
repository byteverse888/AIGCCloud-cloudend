from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Server
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    debug: bool = True  # 开发模式

    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "aigccloud"
    postgres_password: str = ""
    postgres_db: str = "aigccloud"

    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: str = ""

    # Parse Server
    parse_server_url: str = "http://localhost:1337/parse"
    parse_app_id: str = "aigccloud"
    parse_rest_api_key: str = "restapi_service_key"
    parse_master_key: str = "master_key_for_server"  # Master Key，用于服务端高权限操作
    parse_js_key: str = "jskey_client"  # 客户端动态获取的 JS Key

    # JWT
    jwt_secret_key: str = "your-secret-key-256bits-random-string"  # 256位随机字符串
    jwt_secret_key: str = "your-secret-key-256bits-random-string"  # 256位随机字符串
    jwt_algorithm: str = "HS256"
    jwt_access_token_expire_minutes: int = 1440  # 24小时
    jwt_access_token_expire_minutes: int = 1440  # 24小时

    # WeChat Pay (测试数据)
    wechat_app_id: str = "wx_test_appid"
    wechat_mch_id: str = "1234567890"
    wechat_api_key: str = "test_api_key_32chars_placeholder"
    wechat_notify_url: str = "http://localhost:8000/api/v1/member/callback/wechat"
    wechat_test_mode: bool = False  # 测试模式，允许模拟支付
    
    # WeChat Pay API v3 (生产环境推荐)
    wechat_api_v3_key: str = ""  # API v3 密钥 (32字节)
    wechat_serial_no: str = ""   # 商户API证书序列号
    wechat_private_key_path: str = ""  # 商户私钥文件路径 (apiclient_key.pem)

    # Web3 运营账户（用于发放金币激励）
    web3_operator_private_key: str = "0x0000000000000000000000000000000000000000000000000000000000000001"
    web3_operator_address: str = "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"
    web3_coin_contract: str = ""  # 金币合约地址

    # Web3 联盟链
    web3_rpc_url: str = ""
    web3_chain_id: int = 1
    web3_contract_address: str = ""
    web3_private_key: str = ""
    
    # 运营激励账户（用于发放激励）
    incentive_wallet_private_key: str = ""  # 激励钱包私钥

    # Email
    smtp_host: str = ""
    smtp_port: int = 465
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from_name: str = "巴特星球"

    # Log
    log_dir: str = "./logs"
    log_file: str = "aigccloud.log"

    # Admin
    default_admin_username: str = "admin"
    default_admin_email: str = "admin@example.com"
    default_admin_password: str = "admin123456"

    # 对象存储 - 腾讯云COS
    storage_type: str = "cos"  # cos 或 s3
    cos_secret_id: str = ""
    cos_secret_key: str = ""
    cos_bucket: str = "aigccloud-1234567890"
    cos_region: str = "ap-shanghai"
    cos_cdn_domain: str = ""

    # 对象存储 - AWS S3
    aws_access_key: str = ""
    aws_secret_key: str = ""
    aws_s3_bucket: str = "aigccloud"
    aws_s3_region: str = "us-east-1"
    aws_cdn_domain: str = ""

    # S3 统一配置（兼容 RustFS/MinIO/COS/S3）
    s3_endpoint: str = "http://localhost:9000"  # S3 API 端点
    s3_access_key: str = "rustfs"
    s3_secret_key: str = "rustfs123456"
    s3_bucket: str = "aigccloud"
    s3_region: str = "us-east-1"
    s3_public_url: str = "http://localhost:9000"  # 文件公开访问域名

    @property
    def postgres_dsn(self) -> str:
        return f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def redis_url(self) -> str:
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
