"""
ARQ 任务定义
"""
from datetime import datetime, timedelta
from app.core.logger import logger
from app.core.parse_client import parse_client
from app.core.wechat_pay import wechat_pay


# ============ 支付相关任务 ============

async def process_pending_orders(ctx):
    """处理待支付订单"""
    logger.info("[ARQ] 开始处理待支付订单...")
    
    try:
        result = await parse_client.query_objects(
            "Order",
            where={"status": "pending"},
            limit=50,
        )
        orders = result.get("results", [])
        
        if not orders:
            logger.info("[ARQ] 无待处理订单")
            return {"processed": 0}
        
        processed = 0
        for order in orders:
            order_id = order.get("orderId")
            try:
                pay_result = await wechat_pay.query_order(order_id)
                
                if pay_result.get("trade_state") == "SUCCESS":
                    await parse_client.update_object(
                        "Order",
                        order["objectId"],
                        {"status": "paid"}
                    )
                    logger.info(f"[ARQ] 订单 {order_id} 支付成功")
                    processed += 1
                    
                    # 触发后续处理
                    from app.core.arq_worker import enqueue_task
                    await enqueue_task("process_paid_order", order["objectId"])
                    
            except Exception as e:
                logger.error(f"[ARQ] 处理订单 {order_id} 失败: {e}")
                
        return {"processed": processed, "total": len(orders)}
        
    except Exception as e:
        logger.error(f"[ARQ] 处理待支付订单失败: {e}")
        raise


async def process_paid_order(ctx, order_object_id: str):
    """处理已支付订单"""
    logger.info(f"[ARQ] 处理已支付订单: {order_object_id}")
    
    try:
        order = await parse_client.get_object("Order", order_object_id)
        if not order:
            logger.error(f"[ARQ] 订单不存在: {order_object_id}")
            return {"success": False}
        
        order_type = order.get("type")
        user_id = order.get("userId")
        
        if order_type == "recharge":
            coins = order.get("coins", 0)
            if coins > 0:
                # 充值订单：更新订单状态为完成（充值奖励暂不实现）
                await parse_client.update_object(
                    "Order",
                    order_object_id,
                    {"status": "completed", "completedAt": datetime.now().isoformat()}
                )
                logger.info(f"[ARQ] 用户 {user_id} 充值订单完成: {coins} 金币")
                
        elif order_type == "subscription":
            from app.api.v1.endpoints.member import complete_member_order
            await complete_member_order(order.get("orderId"), order)
            
        return {"success": True}
        
    except Exception as e:
        logger.error(f"[ARQ] 处理已支付订单失败: {e}")
        raise


async def process_paid_tx_orders(ctx):
    """处理支付中(paid)状态的订单，验证链上交易"""
    logger.info("[ARQ] 开始处理支付中订单...")
    
    try:
        result = await parse_client.query_objects(
            "Order",
            where={"status": "paid"},
            order="-createdAt",
            limit=100
        )
        orders = result.get("results", [])
        
        if not orders:
            logger.info("[ARQ] 无支付中订单")
            return {"processed": 0}
        
        processed = 0
        for order in orders:
            order_id = order.get("objectId")
            tx_hash = order.get("txHash")
            
            if not tx_hash:
                continue
            
            try:
                from app.api.v1.endpoints.payment import _verify_tx_status
                buyer_address = order.get("buyerAddress")
                seller_address = order.get("sellerAddress")
                amount = int(order.get("amount", 0))
                product_id = order.get("productId")
                
                verify_result = await _verify_tx_status(tx_hash, buyer_address, seller_address, amount)
                tx_status = verify_result.get("tx_status", "error")
                
                if tx_status == "confirmed" and verify_result.get("verified"):
                    if product_id:
                        await parse_client.update_object("Product", product_id, {
                            "owner": buyer_address,
                            "sales": {"__op": "Increment", "amount": 1}
                        })
                    
                    await parse_client.update_object("Order", order_id, {
                        "status": "completed",
                        "completedAt": datetime.now().isoformat()
                    })
                    logger.info(f"[ARQ] 订单已完成: {order_id}")
                    processed += 1
                
                elif tx_status == "failed":
                    await parse_client.update_object("Order", order_id, {
                        "status": "payment_failed"
                    })
                    logger.warning(f"[ARQ] 订单支付失败: {order_id}")
                
            except Exception as e:
                logger.error(f"[ARQ] 处理订单 {order_id} 失败: {e}")
                
        return {"processed": processed}
        
    except Exception as e:
        logger.error(f"[ARQ] 处理支付中订单失败: {e}")
        raise


# ============ AI 任务相关 ============

async def execute_ai_task(ctx, task_id: str, task_type: str, params: dict):
    """执行 AI 任务"""
    logger.info(f"[ARQ] 执行 AI 任务: {task_id}, 类型: {task_type}")
    
    try:
        await parse_client.update_object(
            "AITask",
            task_id,
            {
                "status": "processing",
                "startedAt": {"__type": "Date", "iso": datetime.utcnow().isoformat() + "Z"}
            }
        )
        
        # 模拟处理
        import asyncio
        await asyncio.sleep(2)
        
        result = {"type": task_type, "status": "completed"}
        
        await parse_client.update_object(
            "AITask",
            task_id,
            {
                "status": "completed",
                "result": result,
                "completedAt": {"__type": "Date", "iso": datetime.utcnow().isoformat() + "Z"}
            }
        )
        
        logger.info(f"[ARQ] AI 任务完成: {task_id}")
        return {"success": True}
        
    except Exception as e:
        logger.error(f"[ARQ] AI 任务失败: {task_id}, 错误: {e}")
        await parse_client.update_object(
            "AITask",
            task_id,
            {
                "status": "failed",
                "error": str(e)
            }
        )
        raise


async def check_timeout_tasks(ctx):
    """检查超时任务"""
    logger.info("[ARQ] 检查超时任务...")
    
    try:
        timeout_threshold = datetime.utcnow() - timedelta(minutes=30)
        # Parse 日期格式：不带微秒
        iso_date = timeout_threshold.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        
        # 先查询 processing 状态的任务，不带日期过滤
        result = await parse_client.query_objects(
            "AITask",
            where={"status": "processing"},
            limit=100
        )
        tasks = result.get("results", [])
        
        if not tasks:
            return {"timeout_count": 0}
        
        # 在内存中过滤超时任务
        timeout_count = 0
        for task in tasks:
            started_at = task.get("startedAt")
            if not started_at:
                continue
            
            # Parse 返回的日期格式: {"__type": "Date", "iso": "..."}
            if isinstance(started_at, dict):
                started_iso = started_at.get("iso", "")
            else:
                started_iso = str(started_at)
            
            try:
                started_dt = datetime.fromisoformat(started_iso.replace("Z", "+00:00"))
                if started_dt.replace(tzinfo=None) < timeout_threshold:
                    await parse_client.update_object(
                        "AITask",
                        task["objectId"],
                        {
                            "status": "timeout",
                            "error": "任务处理超时"
                        }
                    )
                    logger.warning(f"[ARQ] 任务超时: {task['objectId']}")
                    timeout_count += 1
            except Exception as e:
                logger.warning(f"[ARQ] 解析任务日期失败: {task['objectId']}, {e}")
        
        return {"timeout_count": timeout_count}
        
    except Exception as e:
        logger.error(f"[ARQ] 检查超时任务失败: {e}")
        return {"timeout_count": 0}  # 不抛出异常，避免影响其他任务


# ============ 激励清算任务 ============

async def settle_pending_incentives(ctx):
    """
    清算待结算激励积分
    
    每24小时执行一次：
    1. 查询 pendingCoins >= 1000 的用户
    2. 调用 web3_client.mint() 铸造金币上链
    3. 更新 IncentiveLog 的 settlementStatus 为 settled
    4. 重置用户的 pendingCoins 为 0
    """
    from app.core.web3_client import web3_client
    from app.core.incentive_service import SETTLEMENT_THRESHOLD
    import uuid
    
    logger.info("[ARQ] 开始清算待结算激励积分...")
    
    try:
        # 1. 查询待结算积分 >= 阈值的用户
        result = await parse_client.query_users(
            where={"pendingCoins": {"$gte": SETTLEMENT_THRESHOLD}},
            limit=100
        )
        users = result.get("results", [])
        
        if not users:
            logger.info("[ARQ] 无需清算的用户")
            return {"settled_count": 0}
        
        settled_count = 0
        batch_id = f"batch_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
        
        for user in users:
            user_id = user.get("objectId")
            web3_address = user.get("web3Address")
            pending_coins = user.get("pendingCoins", 0)
            
            if not web3_address or pending_coins < SETTLEMENT_THRESHOLD:
                continue
            
            try:
                # 2. 铸造金币上链
                mint_result = await web3_client.mint(web3_address, int(pending_coins))
                
                if mint_result.get("success"):
                    tx_hash = mint_result.get("tx_hash", "")
                    settled_at = datetime.now().isoformat()
                    
                    # 3. 批量更新该用户所有 pending 的 IncentiveLog
                    pending_logs = await parse_client.query_objects(
                        "IncentiveLog",
                        where={"userId": user_id, "settlementStatus": "pending"},
                        limit=1000
                    )
                    
                    for log in pending_logs.get("results", []):
                        await parse_client.update_object(
                            "IncentiveLog",
                            log["objectId"],
                            {
                                "settlementStatus": "settled",
                                "txHash": tx_hash,
                                "settledAt": settled_at,
                                "batchId": batch_id,
                            }
                        )
                    
                    # 4. 重置用户 pendingCoins
                    await parse_client.update_user_with_master_key(user_id, {
                        "pendingCoins": 0
                    })
                    
                    settled_count += 1
                    logger.info(f"[ARQ] 用户 {user_id} 清算成功: {pending_coins} 金币, tx={tx_hash}")
                else:
                    # 铸造失败，标记日志为 failed
                    logger.error(f"[ARQ] 用户 {user_id} 清算失败: {mint_result.get('error')}")
                    
            except Exception as e:
                logger.error(f"[ARQ] 用户 {user_id} 清算异常: {e}")
        
        logger.info(f"[ARQ] 清算完成: batch={batch_id}, 成功 {settled_count}/{len(users)} 个用户")
        return {"settled_count": settled_count, "batch_id": batch_id}
        
    except Exception as e:
        logger.error(f"[ARQ] 清算待结算激励失败: {e}")
        return {"settled_count": 0}
