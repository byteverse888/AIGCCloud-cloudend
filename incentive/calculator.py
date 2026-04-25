"""
积分计算引擎
根据设计文档实现完整的积分计算公式：
  贡献积分 = 基线分 × 在线时长奖励 × 连续在线系数 × GPU系数 × 显存权重 × 网络成熟度系数 × 算力短缺系数
  可兑换积分 = 用户付费的 80% 按兑换率转化，按节点算力比例分配
"""
from dataclasses import dataclass
from typing import Optional

from incentive.config import config
from incentive.gpu_database import lookup_gpu, GPUProfile
from incentive.logger import logger


@dataclass
class NodeSnapshot:
    """节点快照（从 K8s + Parse 汇总的当前状态）"""
    node_name: str
    eth_address: str
    gpu_name: str           # GPU 型号字符串
    gpu_count: int          # GPU 卡数
    is_online: bool         # 当前是否 Ready
    continuous_hours: float # 连续在线小时数
    running_pods: int       # 正在运行的 Pod 数
    # 以下字段从 Parse User 读取
    parse_user_id: str = ""
    total_contribution: float = 0.0   # 历史累计贡献积分
    exchangeable_balance: float = 0.0 # 当前可兑换积分余额
    pending_settlement: float = 0.0   # 待清算积分


@dataclass
class IncentiveResult:
    """单个节点一个周期的积分计算结果"""
    node_name: str
    eth_address: str
    parse_user_id: str

    # 计算组件
    base_score: float          # 节点基线分
    online_hours: float        # 本周期在线时长
    online_multiplier: float   # 在线奖励倍率
    continuous_coeff: float    # 连续在线系数
    gpu_score: float           # GPU 算力分
    vram_weight: float         # 显存权重
    gpu_count_factor: float    # 多卡系数
    maturity_factor: float     # 网络成熟度系数
    shortage_factor: float     # 算力短缺系数
    pod_bonus: float           # Pod 任务加成

    # 结果
    contribution_points: float # 本周期贡献积分
    exchangeable_points: float # 本周期可兑换积分（仅来自任务收入分成，此处为 0，由 settlement 层填充）
    formula: str               # 完整计算公式字符串（透明可审计）


class IncentiveCalculator:
    """积分计算器"""

    def __init__(self, total_nodes: int = 0, supply_demand_ratio: float = 1.0):
        """
        Args:
            total_nodes: 当前全网节点总数（用于计算网络成熟度系数）
            supply_demand_ratio: 供需比（需求/供给），用于计算算力短缺系数
        """
        self.total_nodes = total_nodes
        self.supply_demand_ratio = supply_demand_ratio
        self._maturity = self._calc_maturity_factor(total_nodes)
        self._shortage = self._calc_shortage_factor(supply_demand_ratio)
        logger.info(
            f"[Calculator] 初始化: 全网节点={total_nodes}, "
            f"网络成熟度系数={self._maturity}, "
            f"算力短缺系数={self._shortage}, "
            f"供需比={supply_demand_ratio:.2f}"
        )

    # ========== 系数计算 ==========

    @staticmethod
    def _calc_maturity_factor(total_nodes: int) -> float:
        """网络成熟度系数：节点越多系数越低"""
        c = config
        if total_nodes < c.maturity_cold_start_threshold:
            return c.maturity_cold_start_factor
        elif total_nodes < c.maturity_growth_threshold:
            return c.maturity_growth_factor
        elif total_nodes < c.maturity_saturated_threshold:
            return c.maturity_mature_factor
        else:
            return c.maturity_saturated_factor

    @staticmethod
    def _calc_shortage_factor(ratio: float) -> float:
        """算力短缺系数：供需比越高系数越大"""
        if ratio < 0.5:
            return config.shortage_oversupply
        elif ratio < 0.8:
            return config.shortage_balanced
        elif ratio < 1.2:
            return config.shortage_light
        elif ratio < 1.5:
            return config.shortage_severe
        else:
            return config.shortage_crisis

    @staticmethod
    def get_online_multiplier(continuous_hours: float) -> float:
        """在线奖励倍率（非线性曲线）"""
        c = config
        if continuous_hours < c.online_tier1_hours:
            return c.online_tier1_multiplier
        elif continuous_hours < c.online_tier2_hours:
            return c.online_tier2_multiplier
        else:
            return c.online_tier3_multiplier

    @staticmethod
    def get_continuous_coeff(continuous_hours: float) -> float:
        """连续在线时间系数"""
        c = config
        if continuous_hours < 24:
            return c.cont_coeff_tier1
        elif continuous_hours < 72:
            return c.cont_coeff_tier2
        elif continuous_hours < 168:
            return c.cont_coeff_tier3
        else:
            return c.cont_coeff_tier4

    @staticmethod
    def get_base_score(gpu_name: str, gpu_count: int) -> float:
        """
        节点基线分 —— 根据 GPU 配置判断节点类型。
        超级节点(>=2 高端 GPU): 20
        轻量节点(有 GPU): 10
        端侧节点(CPU only): 5
        """
        c = config
        profile = lookup_gpu(gpu_name)
        if profile.score <= 5:
            return c.base_score_edge
        elif gpu_count >= 2 and profile.score >= 45:
            return c.base_score_super
        else:
            return c.base_score_light

    @staticmethod
    def apply_offline_penalty(previous_hours: float, offline_minutes: float) -> float:
        """
        掉线惩罚：返回调整后的连续在线时长。
        <5min: 不清零
        5-30min: 衰减 25%
        >30min: 清零
        """
        c = config
        if offline_minutes < c.offline_grace_minutes:
            return previous_hours
        elif offline_minutes < c.offline_soft_penalty_minutes:
            return previous_hours * (1 - c.offline_soft_decay)
        else:
            return 0.0

    # ========== 核心计算 ==========

    def calculate(self, node: NodeSnapshot, period_hours: float = 1.0) -> IncentiveResult:
        """
        计算一个节点在指定周期内的贡献积分。

        公式: Pc = B_base × online_hours × M_online × D_cont × GPU_score × VRAM_w × GPU_count_f × M_t × S_t + Pod_bonus
        """
        gpu_profile = lookup_gpu(node.gpu_name)
        base = self.get_base_score(node.gpu_name, node.gpu_count)
        online_hours = period_hours if node.is_online else 0.0
        online_mult = self.get_online_multiplier(node.continuous_hours)
        cont_coeff = self.get_continuous_coeff(node.continuous_hours)
        gpu_score = gpu_profile.score
        vram_w = gpu_profile.vram_weight
        # 多卡系数：每多一张卡，额外增加 0.8 倍（非线性递减避免刷卡）
        gpu_count_f = 1.0 + max(0, node.gpu_count - 1) * 0.8

        # Pod 任务加成：有 Pod 运行时额外加成
        pod_bonus = 0.0
        if node.running_pods > 0 and node.is_online:
            # 每个 Pod 贡献 base_score × 0.5 的额外积分
            pod_bonus = base * 0.5 * min(node.running_pods, 10)

        # 核心公式
        contribution = (
            base * online_hours * online_mult * cont_coeff
            * (gpu_score / 100.0)  # 归一化到 0~1+ 范围
            * vram_w
            * gpu_count_f
            * self._maturity
            * self._shortage
            + pod_bonus
        )
        contribution = round(contribution, 2)

        formula = (
            f"Pc = {base}(base) × {online_hours:.1f}h × {online_mult}(online) "
            f"× {cont_coeff}(cont) × {gpu_score}/100(gpu) × {vram_w}(vram) "
            f"× {gpu_count_f:.1f}(cards) × {self._maturity}(maturity) "
            f"× {self._shortage}(shortage) + {pod_bonus:.1f}(pod) = {contribution}"
        )

        return IncentiveResult(
            node_name=node.node_name,
            eth_address=node.eth_address,
            parse_user_id=node.parse_user_id,
            base_score=base,
            online_hours=online_hours,
            online_multiplier=online_mult,
            continuous_coeff=cont_coeff,
            gpu_score=gpu_score,
            vram_weight=vram_w,
            gpu_count_factor=gpu_count_f,
            maturity_factor=self._maturity,
            shortage_factor=self._shortage,
            pod_bonus=pod_bonus,
            contribution_points=contribution,
            exchangeable_points=0.0,  # 由 settlement 层通过任务收入分配
            formula=formula,
        )
