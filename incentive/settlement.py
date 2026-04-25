"""
每日积分清算 + 批量转账
设计文档：每天凌晨 2 点，将待清算积分通过联盟链转账到节点账户，转账成功后清零。
"""
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from incentive.config import config
from incentive.logger import logger
from incentive.parse_client import parse_client
from incentive.web3_client import web3_client


def run_settlement():
    """
    每日清算入口。
    流程：
    1. 查询所有 pendingSettlement >= min_transfer_amount 的用户
    2. 按 batch_transfer_size 分批
    3. 每批调用 Web3 批量转账
    4. 转账成功后：更新 Parse User pendingSettlement=0，标记 IncentiveLog 为已清算
    """
    logger.info("=" * 60)
    logger.info("[Settlement] 开始每日积分清算")

    batch_id = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    min_amount = config.min_transfer_amount
    batch_size = config.batch_transfer_size

    # 1. 获取待清算用户
    eligible_users = _get_eligible_users(min_amount)
    if not eligible_users:
        logger.info(f"[Settlement] 无满足最低转账额度({min_amount})的用户，跳过")
        logger.info("=" * 60)
        return

    logger.info(f"[Settlement] 待清算用户: {len(eligible_users)} 个, batch_id={batch_id}")

    # 2. 分批转账
    total_transferred = 0
    total_success = 0
    total_failed = 0

    for i in range(0, len(eligible_users), batch_size):
        chunk = eligible_users[i: i + batch_size]
        batch_num = i // batch_size + 1
        logger.info(f"[Settlement] 第 {batch_num} 批, {len(chunk)} 个账户")

        # 构建转账列表（保留 user 引用以便后续匹配）
        transfer_items: List[Tuple[Dict[str, Any], str, int]] = []  # (user, eth_addr, amount)
        for user in chunk:
            eth_addr = user.get("web3Address", "")
            amount = round(float(user.get("pendingSettlement", 0)))
            if eth_addr and amount >= min_amount:
                transfer_items.append((user, eth_addr, amount))

        if not transfer_items:
            continue

        # 执行批量转账
        transfers: List[Tuple[str, int]] = [(addr, amt) for _, addr, amt in transfer_items]
        results = web3_client.batch_transfer(transfers)

        # 处理结果（results 与 transfer_items 一一对应）
        success_users: List[Dict[str, Any]] = []
        for idx, result in enumerate(results):
            user = transfer_items[idx][0]
            user_id = user.get("objectId", "")
            eth_addr = result["address"]
            amount = result["amount"]

            if result.get("success"):
                tx_hash = result.get("tx_hash", "")
                total_success += 1
                total_transferred += amount
                success_users.append({
                    "objectId": user_id,
                    "eth_address": eth_addr,
                    "amount": amount,
                    "tx_hash": tx_hash,
                })
                logger.info(
                    f"[Settlement] 转账成功: {eth_addr[:10]}... "
                    f"amount={amount}, tx={tx_hash[:20]}..."
                )
            else:
                total_failed += 1
                logger.error(
                    f"[Settlement] 转账失败: {eth_addr[:10]}... "
                    f"amount={amount}, error={result.get('error', '未知')}"
                )

        # 转账成功的用户：清零 pendingSettlement，记录 tx_hash
        if success_users:
            _process_successful_transfers(success_users, batch_id)

    # 3. 汇总
    logger.info(
        f"[Settlement] 清算完成: "
        f"成功={total_success}, 失败={total_failed}, "
        f"总转账积分={total_transferred}, batch_id={batch_id}"
    )
    logger.info("=" * 60)


def _get_eligible_users(min_amount: int) -> List[Dict[str, Any]]:
    """分页获取所有 pendingSettlement >= min_amount 的用户"""
    all_users: List[Dict[str, Any]] = []
    skip = 0
    limit = 200

    while True:
        try:
            result = parse_client.query_users(
                where={"pendingSettlement": {"$gte": min_amount}},
                limit=limit,
                skip=skip,
                keys="objectId,web3Address,pendingSettlement,username",
            )
            users = result.get("results", [])
            all_users.extend(users)

            if len(users) < limit:
                break
            skip += limit
        except Exception as e:
            err_str = str(e)
            if "errorMissingColumn" in err_str or "42703" in err_str:
                logger.warning("[Settlement] pendingSettlement 列尚未创建（需先执行 collect）")
            else:
                logger.error(f"[Settlement] 查询待清算用户失败: {e}")
            break

    return all_users


def _process_successful_transfers(success_users: List[Dict[str, Any]], batch_id: str):
    """处理转账成功的用户：清零待清算积分 + 标记日志"""
    # 批量更新 Parse User
    updates = []
    for user in success_users:
        updates.append({
            "objectId": user["objectId"],
            "data": {
                "pendingSettlement": 0,
                "lastSettlementBatchId": batch_id,
            },
        })
    parse_client.batch_update_users(updates)

    # 为每个用户创建清算日志
    for user in success_users:
        try:
            parse_client.create_incentive_log(
                user_id=user["objectId"],
                eth_address=user["eth_address"],
                log_type="settlement",
                amount=user["amount"],
                description=f"每日清算转账, tx={user['tx_hash']}, batch={batch_id}",
                batch_id=batch_id,
            )
            # 标记该用户的 unsettled 日志为已清算
            _settle_user_logs(user["objectId"], user["tx_hash"], batch_id)
        except Exception as e:
            logger.error(f"[Settlement] 写入清算日志失败 {user['eth_address']}: {e}")


def _settle_user_logs(user_id: str, tx_hash: str, batch_id: str):
    """标记用户的 unsettled 日志为已清算"""
    try:
        result = parse_client.query_objects(
            "IncentiveLog",
            where={"userId": user_id, "settlementStatus": "unsettled"},
            limit=1000,
            keys="objectId",
        )
        log_ids = [log["objectId"] for log in result.get("results", [])]
        if log_ids:
            parse_client.mark_logs_settled(log_ids, tx_hash, batch_id)
            logger.debug(f"[Settlement] 标记 {len(log_ids)} 条日志为已清算: user={user_id}")
    except Exception as e:
        logger.error(f"[Settlement] 标记日志失败 user={user_id}: {e}")
