"""
查询接口
提供节点级和全网级的积分/状态查询，供 CLI 或 API 调用。
"""
import json
from typing import Any, Dict, List, Optional

from incentive.calculator import IncentiveCalculator
from incentive.gpu_database import lookup_gpu
from incentive.logger import logger
from incentive.parse_client import parse_client


def query_node(eth_address: str) -> Optional[Dict[str, Any]]:
    """
    查询任意节点的积分信息。
    返回示例:
    {
        "web3Address": "0x1234...",
        "totalContribution": 15000,
        "exchangeableBalance": 320,
        "pendingSettlement": 1200,
        "continuousOnlineHours": 48,
        "nodeCoefficient": 1.85,
        "gpuModel": "RTX 4090",
        "gpuCount": 1,
        "isOnline": true,
        "runningPods": 2
    }
    """
    user = parse_client.find_user_by_eth(eth_address.lower())
    if not user:
        logger.warning(f"[Query] 未找到节点: {eth_address}")
        return None

    gpu_name = user.get("gpuModel", "")
    gpu_profile = lookup_gpu(gpu_name)

    return {
        "web3Address": user.get("web3Address", ""),
        "username": user.get("username", ""),
        "totalContribution": round(float(user.get("totalContribution", 0)), 2),
        "exchangeableBalance": round(float(user.get("exchangeableBalance", 0)), 2),
        "pendingSettlement": round(float(user.get("pendingSettlement", 0)), 2),
        "continuousOnlineHours": round(float(user.get("continuousOnlineHours", 0)), 2),
        "nodeCoefficient": round(float(user.get("nodeCoefficient", 0)), 2),
        "gpuModel": gpu_name,
        "gpuScore": gpu_profile.score,
        "gpuCount": int(user.get("gpuCount", 0)),
        "isOnline": user.get("isOnline", False),
        "runningPods": int(user.get("runningPods", 0)),
        "lastSeenAt": user.get("lastSeenAt", ""),
        "lastSettledAt": user.get("lastSettledAt", ""),
    }


def query_node_history(eth_address: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    查询节点积分历史记录。
    返回示例:
    [
        {"type": "online_reward", "amount": 180, "description": "公式...", "createdAt": "..."},
        {"type": "settlement", "amount": 5000, "description": "每日清算...", "createdAt": "..."},
    ]
    """
    user = parse_client.find_user_by_eth(eth_address.lower())
    if not user:
        return []

    user_id = user.get("objectId", "")
    result = parse_client.query_objects(
        "IncentiveLog",
        where={"userId": user_id},
        order="-createdAt",
        limit=limit,
    )
    logs = result.get("results", [])
    return [
        {
            "type": log.get("type", ""),
            "amount": log.get("amount", 0),
            "description": log.get("description", ""),
            "status": log.get("status", ""),
            "settlementStatus": log.get("settlementStatus", ""),
            "txHash": log.get("txHash", ""),
            "batchId": log.get("batchId", ""),
            "createdAt": log.get("createdAt", ""),
        }
        for log in logs
    ]


def query_network() -> Dict[str, Any]:
    """
    查询全网信息。
    返回示例:
    {
        "totalNodes": 500,
        "onlineNodes": 420,
        "totalGPU": 600,
        "totalTFLOPS": 25000,
        "runningPods": 300,
        "supplyDemandRatio": 0.5,
        "maturityFactor": 1.5,
        "shortageFactor": 1.0,
        "utilization": 0.5,
        "totalContributionIssued": 5000000,
        "totalSettled": 3000000,
    }
    """
    # 从 Parse Config 读取实时网络统计
    stats = {}
    params = {}
    try:
        result = parse_client._request("GET", "/config")
        params = result.get("params", {})
        stats = {
            "totalNodes": params.get("networkTotalNodes", 0),
            "onlineNodes": params.get("networkOnlineNodes", 0),
            "totalGPU": params.get("networkTotalGPU", 0),
            "runningPods": params.get("networkRunningPods", 0),
            "supplyDemandRatio": params.get("networkSupplyDemandRatio", 0),
            "maturityFactor": params.get("networkMaturityFactor", 1.0),
            "shortageFactor": params.get("networkShortageFactor", 1.0),
            "utilization": params.get("networkUtilization", 0),
            "lastUpdated": params.get("networkLastUpdated", ""),
        }
    except Exception as e:
        logger.error(f"[Query] 读取全网统计失败: {e}")

    # 补充聚合数据（使用 Parse Config 缓存值，避免全量扫描）
    try:
        # 优先从 Config 中读取缓存的统计值（由 collector 每小时更新）
        total_issued = float(params.get("networkTotalContributionIssued", 0))
        total_settled = float(params.get("networkTotalSettled", 0))
        # 如果 Config 中没有缓存值，回退到分页扫描（仅首次运行或缓存丢失时）
        if total_issued == 0 and total_settled == 0:
            total_issued = _sum_incentive_amount("online_reward")
            total_settled = _sum_incentive_amount("settlement")
        stats["totalContributionIssued"] = round(total_issued, 2)
        stats["totalSettled"] = round(total_settled, 2)
    except Exception as e:
        logger.error(f"[Query] 聚合积分数据失败: {e}")

    return stats


def _sum_incentive_amount(log_type: str) -> float:
    """汇总某类型积分日志的总额（分页累加）"""
    total = 0.0
    skip = 0
    limit = 1000
    while True:
        result = parse_client.query_objects(
            "IncentiveLog",
            where={"type": log_type},
            limit=limit,
            skip=skip,
            keys="amount",
        )
        logs = result.get("results", [])
        for log in logs:
            total += float(log.get("amount", 0))
        if len(logs) < limit:
            break
        skip += limit
    return total


def print_node_info(eth_address: str):
    """打印节点信息（CLI 使用）"""
    info = query_node(eth_address)
    if not info:
        print(f"未找到节点: {eth_address}")
        return
    print(f"\n{'='*50}")
    print(f"节点: {info['web3Address']}")
    print(f"用户: {info['username']}")
    print(f"{'─'*50}")
    print(f"  总贡献积分     : {info['totalContribution']}")
    print(f"  可兑换积分余额 : {info['exchangeableBalance']}")
    print(f"  待清算积分     : {info['pendingSettlement']}")
    print(f"  连续在线时间   : {info['continuousOnlineHours']} 小时")
    print(f"  当前节点系数   : {info['nodeCoefficient']}")
    print(f"  GPU            : {info['gpuModel']} × {info['gpuCount']} (score={info['gpuScore']})")
    print(f"  在线状态       : {'在线' if info['isOnline'] else '离线'}")
    print(f"  运行 Pod       : {info['runningPods']}")
    print(f"{'='*50}\n")


def print_network_info():
    """打印全网信息（CLI 使用）"""
    info = query_network()
    print(f"\n{'='*50}")
    print(f"全网概览")
    print(f"{'─'*50}")
    print(f"  总节点         : {info.get('totalNodes', 0)}")
    print(f"  在线节点       : {info.get('onlineNodes', 0)}")
    print(f"  总 GPU         : {info.get('totalGPU', 0)}")
    print(f"  运行 Pod       : {info.get('runningPods', 0)}")
    print(f"  算力利用率     : {info.get('utilization', 0):.1%}")
    print(f"  供需比         : {info.get('supplyDemandRatio', 0):.3f}")
    print(f"  网络成熟度系数 : {info.get('maturityFactor', 0)}")
    print(f"  算力短缺系数   : {info.get('shortageFactor', 0)}")
    print(f"  总发放贡献积分 : {info.get('totalContributionIssued', 0)}")
    print(f"  总清算积分     : {info.get('totalSettled', 0)}")
    print(f"  最后更新       : {info.get('lastUpdated', '')}")
    print(f"{'='*50}\n")
