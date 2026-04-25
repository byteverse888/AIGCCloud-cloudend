"""
GPU 型号性能数据库
根据设计文档的 GPU 算力分表，建立型号 → 算力分 + 显存权重的映射。
支持模糊匹配，可动态扩展。
"""
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from incentive.logger import logger


@dataclass(frozen=True)
class GPUProfile:
    """GPU 型号画像"""
    name: str
    score: float        # 算力分（设计文档中的 GPU_score）
    vram_weight: float  # 显存权重
    tflops: float       # FP32 TFLOPS（参考值）
    vram_gb: int        # 显存 GB


# ========== GPU 型号数据库 ==========
# 按设计文档 + 补充常见型号
_GPU_DB: Dict[str, GPUProfile] = {}


def _reg(keywords: list, score: float, vram_weight: float, tflops: float, vram_gb: int):
    """注册一组关键字指向同一个 GPUProfile"""
    name = keywords[0]
    profile = GPUProfile(name=name, score=score, vram_weight=vram_weight,
                         tflops=tflops, vram_gb=vram_gb)
    for kw in keywords:
        _GPU_DB[kw.lower()] = profile


# --- 旗舰级 ---
_reg(["H100", "H100 SXM", "H100 PCIe"], score=100, vram_weight=1.5, tflops=267, vram_gb=80)
_reg(["A100", "A100 80GB", "A100 40GB", "A100-SXM4-80GB", "A100-SXM4-40GB", "A100-PCIE-40GB"],
     score=100, vram_weight=1.5, tflops=156, vram_gb=80)
_reg(["H200"], score=110, vram_weight=1.5, tflops=267, vram_gb=141)

# --- 消费级旗舰 ---
_reg(["RTX 4090", "GeForce RTX 4090", "NVIDIA GeForce RTX 4090"],
     score=65, vram_weight=1.2, tflops=82.6, vram_gb=24)
_reg(["A6000", "RTX A6000", "NVIDIA RTX A6000"],
     score=65, vram_weight=1.2, tflops=38.7, vram_gb=48)
_reg(["RTX 4080", "GeForce RTX 4080", "RTX 4080 SUPER", "NVIDIA GeForce RTX 4080"],
     score=45, vram_weight=1.0, tflops=48.7, vram_gb=16)
_reg(["RTX 4080S", "GeForce RTX 4080 SUPER", "NVIDIA GeForce RTX 4080 SUPER"],
     score=50, vram_weight=1.0, tflops=52.0, vram_gb=16)

# --- 标准节点 ---
_reg(["RTX 3090", "GeForce RTX 3090", "NVIDIA GeForce RTX 3090"],
     score=45, vram_weight=1.0, tflops=35.6, vram_gb=24)
_reg(["RTX 3090 Ti", "GeForce RTX 3090 Ti"],
     score=48, vram_weight=1.0, tflops=40.0, vram_gb=24)
_reg(["RTX 3080", "GeForce RTX 3080", "RTX 3080 Ti", "NVIDIA GeForce RTX 3080"],
     score=38, vram_weight=0.9, tflops=29.8, vram_gb=12)
_reg(["RTX 4070 Ti", "GeForce RTX 4070 Ti", "RTX 4070 Ti SUPER"],
     score=40, vram_weight=1.0, tflops=40.1, vram_gb=16)
_reg(["RTX 4070", "GeForce RTX 4070", "NVIDIA GeForce RTX 4070"],
     score=35, vram_weight=0.9, tflops=29.1, vram_gb=12)
_reg(["RTX 4060 Ti", "GeForce RTX 4060 Ti"],
     score=28, vram_weight=0.8, tflops=22.1, vram_gb=16)
_reg(["RTX 4060", "GeForce RTX 4060"],
     score=22, vram_weight=0.8, tflops=15.1, vram_gb=8)

# --- 边缘节点 ---
_reg(["RTX 2080 Ti", "GeForce RTX 2080 Ti", "NVIDIA GeForce RTX 2080 Ti"],
     score=25, vram_weight=0.8, tflops=13.4, vram_gb=11)
_reg(["RTX 2080", "GeForce RTX 2080", "RTX 2080 SUPER"],
     score=22, vram_weight=0.8, tflops=11.2, vram_gb=8)
_reg(["RTX 3060", "GeForce RTX 3060", "NVIDIA GeForce RTX 3060"],
     score=18, vram_weight=0.7, tflops=12.7, vram_gb=12)
_reg(["RTX 3060 Ti", "GeForce RTX 3060 Ti"],
     score=22, vram_weight=0.8, tflops=16.2, vram_gb=8)
_reg(["RTX 2060", "GeForce RTX 2060", "RTX 2060 SUPER"],
     score=15, vram_weight=0.6, tflops=6.5, vram_gb=6)
_reg(["RTX 3050", "GeForce RTX 3050"],
     score=10, vram_weight=0.5, tflops=9.1, vram_gb=8)
_reg(["GTX 1080 Ti", "GeForce GTX 1080 Ti"],
     score=12, vram_weight=0.5, tflops=11.3, vram_gb=11)
_reg(["GTX 1080", "GeForce GTX 1080"],
     score=10, vram_weight=0.4, tflops=8.9, vram_gb=8)

# --- 数据中心 ---
_reg(["A40", "NVIDIA A40"], score=55, vram_weight=1.2, tflops=37.4, vram_gb=48)
_reg(["A30", "NVIDIA A30"], score=40, vram_weight=1.0, tflops=10.3, vram_gb=24)
_reg(["V100", "Tesla V100", "V100-SXM2-16GB", "V100-SXM2-32GB"],
     score=35, vram_weight=1.0, tflops=15.7, vram_gb=32)
_reg(["T4", "Tesla T4"], score=20, vram_weight=0.7, tflops=8.1, vram_gb=16)
_reg(["L40", "NVIDIA L40"], score=55, vram_weight=1.2, tflops=90.5, vram_gb=48)
_reg(["L40S", "NVIDIA L40S"], score=60, vram_weight=1.3, tflops=91.6, vram_gb=48)

# --- CPU Only / 手机 ---
_reg(["CPU", "cpu_only"], score=5, vram_weight=0.3, tflops=0, vram_gb=0)
_reg(["MOBILE", "mobile", "phone"], score=2, vram_weight=0.1, tflops=0, vram_gb=0)

# 默认值（未识别型号）
_DEFAULT_PROFILE = GPUProfile(name="Unknown", score=5, vram_weight=0.3, tflops=0, vram_gb=0)


def lookup_gpu(gpu_name: str) -> GPUProfile:
    """
    根据 GPU 名称查找性能画像。
    支持模糊匹配：先精确匹配，再按关键字包含匹配。
    """
    if not gpu_name:
        return _DEFAULT_PROFILE

    normalized = gpu_name.strip().lower()

    # 精确匹配
    if normalized in _GPU_DB:
        return _GPU_DB[normalized]

    # 模糊匹配：检查数据库中的关键字是否被包含在 gpu_name 中
    best_match: Optional[GPUProfile] = None
    best_len = 0
    for key, profile in _GPU_DB.items():
        if key in normalized and len(key) > best_len:
            best_match = profile
            best_len = len(key)

    if best_match:
        return best_match

    logger.warning(f"[GPU] 未识别型号: {gpu_name}，使用默认 score=5")
    return _DEFAULT_PROFILE


def get_all_known_gpus() -> Dict[str, GPUProfile]:
    """返回去重后的所有 GPU 画像（按 name 去重）"""
    seen = {}
    for profile in _GPU_DB.values():
        if profile.name not in seen:
            seen[profile.name] = profile
    return seen
