"""
激励结算配置
支持 .env 文件 + 环境变量 + CLI 参数覆盖
"""
import os
from dataclasses import dataclass
from pathlib import Path

# .env 文件路径: incentive 目录内的 .env
_ENV_FILE = Path(__file__).resolve().parent / ".env"


def _load_dotenv():
    """简易 .env 加载（避免额外依赖）"""
    if not _ENV_FILE.exists():
        return
    for line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip("'\"")
        os.environ.setdefault(key, value)


_load_dotenv()


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int = 0) -> int:
    v = os.environ.get(key)
    return int(v) if v else default


def _env_float(key: str, default: float = 0.0) -> float:
    v = os.environ.get(key)
    return float(v) if v else default


@dataclass
class IncentiveConfig:
    """激励结算配置项（全部可通过环境变量覆盖）"""

    # ========== K8s ==========
    # 优先使用 kubeconfig 文件，其次回退到 token 方式
    k8s_kubeconfig: str = _env("K8S_KUBECONFIG", "")
    k8s_api_server: str = _env("K8S_API_SERVER", "https://127.0.0.1:6443")
    k8s_token: str = _env("K8S_TOKEN", "")
    k8s_ca_cert: str = _env("K8S_CA_CERT", "")
    k8s_page_size: int = _env_int("K8S_PAGE_SIZE", 500)

    # ========== Parse Server ==========
    parse_server_url: str = _env("PARSE_SERVER_URL", "http://localhost:1337/parse")
    parse_app_id: str = _env("PARSE_APP_ID", "BTGAPPId")
    parse_master_key: str = _env("PARSE_MASTER_KEY", "")
    parse_rest_api_key: str = _env("PARSE_REST_API_KEY", "")

    # ========== Web3 联盟链 ==========
    web3_rpc_url: str = _env("WEB3_RPC_URL", "")
    web3_chain_id: int = _env_int("WEB3_CHAIN_ID", 888)
    incentive_wallet_private_key: str = _env("INCENTIVE_WALLET_PRIVATE_KEY", "")

    # ========== 在线激励曲线（非线性奖励） ==========
    # 前 24h：每小时 1 × 基线分
    # 24-168h：每小时 1.5 × 基线分
    # 168-720h：每小时 2 × 基线分
    online_tier1_hours: int = _env_int("INCENTIVE_TIER1_HOURS", 24)
    online_tier2_hours: int = _env_int("INCENTIVE_TIER2_HOURS", 168)
    online_tier3_hours: int = _env_int("INCENTIVE_TIER3_HOURS", 720)
    online_tier1_multiplier: float = _env_float("INCENTIVE_TIER1_MULT", 1.0)
    online_tier2_multiplier: float = _env_float("INCENTIVE_TIER2_MULT", 1.5)
    online_tier3_multiplier: float = _env_float("INCENTIVE_TIER3_MULT", 2.0)

    # ========== 连续在线时间系数 ==========
    # 0-24h=1.0, 24-72h=1.2, 72-168h=1.5, 168h+=1.8
    cont_coeff_tier1: float = _env_float("INCENTIVE_CONT_COEFF_T1", 1.0)
    cont_coeff_tier2: float = _env_float("INCENTIVE_CONT_COEFF_T2", 1.2)
    cont_coeff_tier3: float = _env_float("INCENTIVE_CONT_COEFF_T3", 1.5)
    cont_coeff_tier4: float = _env_float("INCENTIVE_CONT_COEFF_T4", 1.8)

    # ========== 掉线恢复机制 ==========
    # <5min: 不清零; 5-30min: 衰减25%; >30min: 清零
    offline_grace_minutes: int = _env_int("INCENTIVE_OFFLINE_GRACE_MIN", 5)
    offline_soft_penalty_minutes: int = _env_int("INCENTIVE_OFFLINE_SOFT_MIN", 30)
    offline_soft_decay: float = _env_float("INCENTIVE_OFFLINE_DECAY", 0.25)

    # ========== 网络成熟度系数 ==========
    # <100节点=2.0, 100-1000=1.5, 1000-10000=1.0, >10000=0.8
    maturity_cold_start_threshold: int = _env_int("INCENTIVE_MATURITY_T1", 100)
    maturity_growth_threshold: int = _env_int("INCENTIVE_MATURITY_T2", 1000)
    maturity_saturated_threshold: int = _env_int("INCENTIVE_MATURITY_T3", 10000)
    maturity_cold_start_factor: float = _env_float("INCENTIVE_MATURITY_F1", 2.0)
    maturity_growth_factor: float = _env_float("INCENTIVE_MATURITY_F2", 1.5)
    maturity_mature_factor: float = _env_float("INCENTIVE_MATURITY_F3", 1.0)
    maturity_saturated_factor: float = _env_float("INCENTIVE_MATURITY_F4", 0.8)

    # ========== 算力短缺系数 ==========
    # 供需比 = 需求容量/供给容量
    shortage_oversupply: float = _env_float("INCENTIVE_SHORTAGE_F1", 0.8)
    shortage_balanced: float = _env_float("INCENTIVE_SHORTAGE_F2", 1.0)
    shortage_light: float = _env_float("INCENTIVE_SHORTAGE_F3", 1.2)
    shortage_severe: float = _env_float("INCENTIVE_SHORTAGE_F4", 1.5)
    shortage_crisis: float = _env_float("INCENTIVE_SHORTAGE_F5", 2.0)

    # ========== 可兑换积分 ==========
    # 用户付费的 80% 转化为可兑换积分
    task_revenue_node_share: float = _env_float("INCENTIVE_NODE_SHARE", 0.8)
    # 积分-现金兑换率（1积分 = X 元，默认 0.01）
    exchange_rate: float = _env_float("INCENTIVE_EXCHANGE_RATE", 0.01)

    # ========== 节点类型基线分 ==========
    base_score_edge: int = _env_int("INCENTIVE_BASE_EDGE", 5)
    base_score_light: int = _env_int("INCENTIVE_BASE_LIGHT", 10)
    base_score_super: int = _env_int("INCENTIVE_BASE_SUPER", 20)

    # ========== 结算 / 转账 ==========
    min_transfer_amount: int = _env_int("INCENTIVE_MIN_TRANSFER", 1000)
    batch_transfer_size: int = _env_int("INCENTIVE_BATCH_SIZE", 100)
    # 统计周期（小时），默认 1h（每小时统计一次）
    collect_interval_hours: int = _env_int("INCENTIVE_COLLECT_HOURS", 1)
    # 清算时间（小时，24=凌晨0点后24小时即次日）
    settlement_hour: int = _env_int("INCENTIVE_SETTLEMENT_HOUR", 2)

    # ========== 日志 ==========
    log_dir: str = _env("INCENTIVE_LOG_DIR", str(Path(__file__).resolve().parent / "logs"))
    log_file: str = _env("INCENTIVE_LOG_FILE", "incentive.log")
    log_level: str = _env("INCENTIVE_LOG_LEVEL", "INFO")
    log_max_bytes: int = _env_int("INCENTIVE_LOG_MAX_BYTES", 50 * 1024 * 1024)
    log_backup_count: int = _env_int("INCENTIVE_LOG_BACKUP_COUNT", 5)
    log_retention_days: int = _env_int("INCENTIVE_LOG_RETENTION_DAYS", 30)


# 全局配置单例
config = IncentiveConfig()
