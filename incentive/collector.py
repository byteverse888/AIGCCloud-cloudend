"""
节点统计采集器（按调度周期定时执行）
从 K8s 获取 node/pod 信息，计算每个节点的在线积分，更新到 Parse User 表。
"""
from datetime import datetime, timezone
from typing import Any, Dict, List

from incentive.calculator import IncentiveCalculator, NodeSnapshot
from incentive.config import config
from incentive.gpu_database import lookup_gpu
from incentive.k8s_client import K8sClient
from incentive.logger import logger
from incentive.parse_client import parse_client


def run_collect():
    """
    本采集周期统计任务入口。
    流程：
    1. 从 K8s 分页获取所有 Node + Pod
    2. 解析节点信息，构建 NodeSnapshot
    3. 从 Parse 读取节点对应用户的历史数据
    4. 计算本采集周期贡献积分
    5. 更新 Parse User 表（连续在线时长、总贡献积分、待清算积分）
    6. 写入 IncentiveLog
    """
    logger.info("=" * 60)
    logger.info("[Collector] 开始本周期节点统计")
    now = datetime.now(timezone.utc)

    # 1. 获取 K8s 数据
    k8s = K8sClient()
    nodes = k8s.list_nodes()
    pods = k8s.list_pods(field_selector="status.phase=Running")

    if not nodes:
        logger.warning("[Collector] 未获取到任何节点，跳过")
        return

    # 过滤：只保留 Ready 的边缘节点
    all_count = len(nodes)
    nodes = [n for n in nodes if k8s.is_edge_node(n) and k8s.is_node_ready(n)]
    logger.info(f"[Collector] 节点过滤: 总节点={all_count}, Ready边缘节点={len(nodes)}")

    if not nodes:
        logger.warning("[Collector] 无 Ready 的边缘节点，跳过")
        return

    total_nodes = len(nodes)
    online_nodes = total_nodes  # 已过滤，全部是 Ready

    # 2. 计算供需比（简化：running pods / total GPU capacity）
    total_gpu_capacity = 0
    total_running_pods = len(pods)
    for n in nodes:
        total_gpu_capacity += k8s.get_node_gpu_count(n)
    supply_demand_ratio = (total_running_pods / total_gpu_capacity) if total_gpu_capacity > 0 else 0.0

    logger.info(
        f"[Collector] 全网: 总节点={total_nodes}, 在线={online_nodes}, "
        f"总GPU={total_gpu_capacity}, 运行Pod={total_running_pods}, "
        f"供需比={supply_demand_ratio:.2f}"
    )

    # 3. 初始化计算器
    calculator = IncentiveCalculator(
        total_nodes=total_nodes,
        supply_demand_ratio=supply_demand_ratio,
    )

    # 4. 遍历节点，构建快照并计算
    user_updates: List[Dict[str, Any]] = []
    log_count = 0
    total_new_contribution = 0.0  # 本次采集新增的总积分

    for node in nodes:
        node_name = k8s.parse_node_name(node)
        eth_address = k8s.extract_eth_address(node_name)
        if not eth_address:
            continue

        is_ready = k8s.is_node_ready(node)
        gpu_info = k8s.get_node_gpu_info(node)
        gpu_count = k8s.get_node_gpu_count(node)
        running_pod_count = k8s.count_running_pods_on_node(pods, node_name)

        # 从 Parse 读取用户数据
        user = parse_client.find_user_by_eth(eth_address)
        if not user:
            logger.info(f"[Collector] 未找到用户: {eth_address}，跳过")
            continue

        user_id = user.get("objectId", "")
        prev_continuous = float(user.get("continuousOnlineHours", 0))
        prev_total = float(user.get("totalContribution", 0))
        prev_pending = float(user.get("pendingSettlement", 0))
        last_seen = user.get("lastSeenAt", "")

        # 掉线检测
        # 正常采集间隔约 collect_interval_hours * 60 分钟
        # 只有当 gap 显著超过预期间隔时，才视为掉线
        expected_gap_minutes = config.collect_interval_hours * 60
        # 容忍预期间隔的 1.5 倍（应对 cron 延迟、系统负载等）
        max_normal_gap_minutes = expected_gap_minutes * 1.5

        continuous_hours = prev_continuous
        if is_ready:
            if last_seen:
                try:
                    last_dt = datetime.fromisoformat(last_seen.replace("Z", "+00:00"))
                    gap_minutes = (now - last_dt).total_seconds() / 60
                    # 仅当 gap 超出正常采集间隔的合理范围时，才视为掉线
                    if gap_minutes > max_normal_gap_minutes:
                        # 实际掉线时长 = gap - 预期间隔（去除正常的采集等待时间）
                        offline_minutes = gap_minutes - expected_gap_minutes
                        continuous_hours = IncentiveCalculator.apply_offline_penalty(
                            prev_continuous, offline_minutes
                        )
                        if continuous_hours < prev_continuous:
                            logger.info(
                                f"[Collector] {eth_address[:10]}... 掉线 {offline_minutes:.0f}min "
                                f"(gap={gap_minutes:.0f}min), "
                                f"连续在线 {prev_continuous:.1f}h -> {continuous_hours:.1f}h"
                            )
                except (ValueError, TypeError):
                    pass
            continuous_hours += config.collect_interval_hours
        else:
            # 节点离线 —— 检查掉线时长
            heartbeat = k8s.get_last_heartbeat(node)
            if heartbeat:
                gap_minutes = (now - heartbeat).total_seconds() / 60
                # 离线场景：gap 中扣除预期间隔，剩余才是真正掉线时间
                offline_minutes = max(0, gap_minutes - expected_gap_minutes)
                continuous_hours = IncentiveCalculator.apply_offline_penalty(
                    prev_continuous, offline_minutes
                )
            else:
                continuous_hours = 0.0

        # 构建快照
        snapshot = NodeSnapshot(
            node_name=node_name,
            eth_address=eth_address,
            gpu_name=gpu_info,
            gpu_count=gpu_count,
            is_online=is_ready,
            continuous_hours=continuous_hours,
            running_pods=running_pod_count,
            parse_user_id=user_id,
            total_contribution=prev_total,
            exchangeable_balance=float(user.get("exchangeableBalance", 0)),
            pending_settlement=prev_pending,
        )

        # 计算积分
        result = calculator.calculate(snapshot, period_hours=config.collect_interval_hours)

        if result.contribution_points > 0:
            total_new_contribution += result.contribution_points
            logger.info(
                f"[Collector] {eth_address[:10]}... | "
                f"GPU={gpu_info} ×{gpu_count} | "
                f"在线={continuous_hours:.1f}h | "
                f"Pod={running_pod_count} | "
                f"积分=+{result.contribution_points:.2f} | "
                f"{result.formula}"
            )

            # 创建积分日志
            try:
                parse_client.create_incentive_log(
                    user_id=user_id,
                    eth_address=eth_address,
                    log_type="online_reward",
                    amount=result.contribution_points,
                    description=result.formula,
                )
                log_count += 1
            except Exception as e:
                logger.error(f"[Collector] 写入积分日志失败 {eth_address}: {e}")

        # 准备 Parse 用户更新
        new_total = prev_total + result.contribution_points
        new_pending = prev_pending + result.contribution_points

        user_updates.append({
            "objectId": user_id,
            "data": {
                "continuousOnlineHours": round(continuous_hours, 2),
                "totalContribution": round(new_total, 2),
                "pendingSettlement": round(new_pending, 2),
                "lastSeenAt": now.isoformat(),
                "nodeCoefficient": round(
                    result.gpu_score / 100 * result.vram_weight
                    * result.gpu_count_factor, 4
                ),
                # 节点系数计算明细，供前端 tooltip 展示
                "nodeCalcDetail": {
                    "gpuName": gpu_info,
                    "gpuScore": result.gpu_score,
                    "gpuVramGb": lookup_gpu(gpu_info).vram_gb,
                    "vramWeight": result.vram_weight,
                    "gpuCount": gpu_count,
                    "gpuCountFactor": round(result.gpu_count_factor, 2),
                    "baseScore": result.base_score,
                    "onlineMultiplier": result.online_multiplier,
                    "continuousCoeff": result.continuous_coeff,
                    "runningPods": running_pod_count,
                    "podBonus": round(result.pod_bonus, 2),
                },
            },
        })

    # 5. 批量更新 Parse
    if user_updates:
        parse_client.batch_update_users(user_updates)
        logger.info(f"[Collector] 更新 {len(user_updates)} 个用户，写入 {log_count} 条积分日志")

    # 6. 更新全网统计信息到 Parse Config
    _update_network_stats(total_nodes, online_nodes, total_gpu_capacity,
                          total_running_pods, supply_demand_ratio, calculator,
                          total_new_contribution)

    logger.info("[Collector] 本周期统计完成")
    logger.info("=" * 60)


def _update_network_stats(total_nodes: int, online_nodes: int,
                          total_gpu: int, running_pods: int,
                          supply_demand_ratio: float,
                          calculator: IncentiveCalculator,
                          new_contribution: float = 0.0):
    """将全网统计信息写入 Parse Config（供前端/查询接口使用）"""
    try:
        # 读取当前 Config 中的累计积分，增量更新
        prev_total_issued = 0.0
        try:
            cfg_result = parse_client._request("GET", "/config")
            prev_total_issued = float(cfg_result.get("params", {}).get("networkTotalContributionIssued", 0))
        except Exception:
            pass

        stats = {
            "networkTotalNodes": total_nodes,
            "networkOnlineNodes": online_nodes,
            "networkTotalGPU": total_gpu,
            "networkRunningPods": running_pods,
            "networkSupplyDemandRatio": round(supply_demand_ratio, 3),
            "networkMaturityFactor": calculator._maturity,
            "networkShortageFactor": calculator._shortage,
            "networkUtilization": round(running_pods / max(total_gpu, 1), 3),
            "networkTotalContributionIssued": round(prev_total_issued + new_contribution, 2),
            "networkLastUpdated": datetime.now(timezone.utc).isoformat(),
        }
        parse_client._request("PUT", "/config", {"params": stats})
        logger.info(f"[Collector] 全网统计已更新: {stats}")
    except Exception as e:
        logger.error(f"[Collector] 更新全网统计失败: {e}")
