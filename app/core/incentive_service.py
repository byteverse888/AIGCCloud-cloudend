"""
激励/账户积分服务

简化两层模型（无补偿表、无双写）：
1) 账户积分（_User.totalIncentive）：商城购物/任务消耗 唯一支付来源
   - 所有变动 统一走 adjust_user_balance，写 AccountRecord 留痕
2) Web3 链上金币：仅在用户绑定钱包地址后，用于 账户积分 ⇄ 链上金币 兑换
幂等：带 related_id 的变动先查 AccountRecord 去重
"""
from typing import Optional, Tuple
from enum import Enum
from datetime import datetime, timezone

from app.core.config import settings
from app.core.parse_client import parse_client
from app.core.logger import logger


class IncentiveType(str, Enum):
    """激励类型（全部入账户积分 totalIncentive）"""
    REGISTER = "register"                   # 注册奖励
    DAILY_LOGIN = "daily_login"             # 每日登录签到
    INVITE_REGISTER = "invite_register"     # 邀请注册奖励
    INVITE_RECHARGE = "invite_recharge"     # 邀请首充返利
    TASK = "task"                           # 任务奖励
    RECHARGE = "recharge"                   # 充值返利
    ACTIVITY = "activity"                   # 活动奖励
    MEMBER_SUBSCRIBE = "member_subscribe"   # 会员订阅奖励


# 激励配置
INCENTIVE_CONFIG = {
    "register": 100,                      # 注册奖励
    "daily_login_normal": 5,              # 普通用户每日登录
    "daily_login_paid": 10,               # 付费用户每日登录
    "invite_register": 100,               # 邀请注册奖励
    "invite_first_recharge_rate": 0.1,    # 邀请首充返利比例
    "task_complete": 1,                   # 任务完成奖励
    "recharge_rate": 0.05,                # 充值返利比例（5%）
    "member_subscribe_vip": 50,           # VIP 订阅奖励
    "member_subscribe_svip": 100,         # SVIP 订阅奖励
}

# 兑换比例默认值（100 账户积分 = 1 链上金币），运行时以 SystemConfig.credits 为准
DEFAULT_EXCHANGE_POINTS = 100
DEFAULT_EXCHANGE_COINS = 1


class IncentiveService:
    """激励/账户积分服务"""

    # ============ 基础工具 ============

    @staticmethod
    def _read_balance(user: dict) -> float:
        """读用户账户积分余额（totalIncentive）"""
        try:
            return float(user.get("totalIncentive", 0) or 0)
        except Exception:
            return 0.0

    async def _get_exchange_rate(self) -> Tuple[int, int]:
        """
        从 SystemConfig.credits 读取 exchangePoints : exchangeYuan
        业务语义：exchangePoints 账户积分 = exchangeYuan 链上金币
        返回 (points, coins)
        """
        try:
            result = await parse_client.query_objects(
                "SystemConfig", where={"category": "credits"}, limit=1
            )
            items = result.get("results", [])
            if items:
                s = items[0].get("settings", {}) or {}
                points = int(float(s.get("exchangePoints", DEFAULT_EXCHANGE_POINTS)))
                coins = int(float(s.get("exchangeYuan", DEFAULT_EXCHANGE_COINS)))
                if points > 0 and coins > 0:
                    return points, coins
        except Exception as e:
            logger.warning(f"[IncentiveService] 读取兑换比例失败，使用默认: {e}")
        return DEFAULT_EXCHANGE_POINTS, DEFAULT_EXCHANGE_COINS

    async def _check_idempotent(self, related_id: str, category: Optional[str] = None) -> bool:
        """检查是否已存在 success 状态的账本记录，避免重复发放"""
        if not related_id:
            return False
        where = {"relatedId": related_id, "status": "success"}
        if category:
            where["category"] = category
        try:
            cnt = await parse_client.count_objects("AccountRecord", where)
            return cnt > 0
        except Exception as e:
            logger.warning(f"[IncentiveService] 幂等性检查失败: {e}")
            return False

    # ============ 账户积分核心：统一账本 ============

    async def adjust_user_balance(
        self,
        user_id: str,
        delta: float,
        type_: str,
        category: str,
        description: str,
        related_id: Optional[str] = None,
        related_order_no: Optional[str] = None,
        operator_id: Optional[str] = None,
        operator_name: Optional[str] = None,
        check_idempotent: bool = True,
    ) -> dict:
        """
        统一账户积分账本：原子变更 totalIncentive + 写 AccountRecord

        Args:
            delta: 变动金额（正加负减）
            type_: recharge/purchase/refund/reward/exchange/consume
            category: 细分类别（product_purchase/daily_sign/exchange_to_web3 ...）
            related_id: 幂等 key
        """
        if delta == 0:
            return {"success": False, "error": "变动金额不能为 0"}

        # 幂等
        if check_idempotent and related_id and await self._check_idempotent(related_id, category):
            logger.info(f"[账本] 幂等命中，跳过: related_id={related_id} category={category}")
            return {"success": True, "skipped": True, "message": "已处理过该笔"}

        try:
            user = await parse_client.get_user(user_id)
        except Exception:
            return {"success": False, "error": "用户不存在"}

        balance_before = self._read_balance(user)
        if delta < 0 and balance_before + delta < -0.0001:
            return {
                "success": False,
                "error": f"积分余额不足，当前 {balance_before:g}，需扣减 {abs(delta):g}",
            }

        balance_after = balance_before + delta

        # 仅写 totalIncentive
        try:
            await parse_client.update_user_with_master_key(
                user_id,
                {"totalIncentive": balance_after},
            )
        except Exception as e:
            logger.error(f"[账本] 更新用户余额失败: {e}", exc_info=True)
            return {"success": False, "error": f"更新余额失败: {e}"}

        # 写账本
        record_data = {
            "userId": user_id,
            "username": user.get("username", ""),
            "type": type_,
            "category": category,
            "amount": float(delta),
            "balance_before": balance_before,
            "balance_after": balance_after,
            "balance": balance_after,
            "description": description,
            "status": "success",
        }
        if related_id:
            record_data["relatedId"] = related_id
        if related_order_no:
            record_data["relatedOrderNo"] = related_order_no
        if operator_id:
            record_data["operator_id"] = operator_id
        if operator_name:
            record_data["operator_name"] = operator_name

        try:
            rec = await parse_client.create_object("AccountRecord", record_data)
            logger.info(
                f"[账本] user={user_id} {type_}/{category} {delta:+g} "
                f"({balance_before:g}→{balance_after:g}) rid={related_id}"
            )
            return {
                "success": True,
                "recordId": rec.get("objectId"),
                "balance_before": balance_before,
                "balance_after": balance_after,
            }
        except Exception as e:
            # 账本写入失败 → 回滚余额（保证强一致）
            logger.error(f"[账本] 写 AccountRecord 失败，回滚余额: {e}", exc_info=True)
            try:
                await parse_client.update_user_with_master_key(
                    user_id, {"totalIncentive": balance_before}
                )
            except Exception as _e:
                logger.error(f"[账本] 回滚余额失败: {_e}", exc_info=True)
            return {"success": False, "error": f"账本写入失败: {e}"}

    # ============ 业务激励（全部入账户积分 totalIncentive） ============

    async def grant_register_reward(self, user_id: str) -> dict:
        """注册奖励（账户积分，幂等 related_id=register_{user_id}）"""
        amount = float(INCENTIVE_CONFIG.get("register", 100))
        if amount <= 0:
            return {"success": True, "skipped": True, "amount": 0}
        return await self.adjust_user_balance(
            user_id=user_id,
            delta=amount,
            type_="reward",
            category="register",
            description="新用户注册奖励",
            related_id=f"register_{user_id}",
        )

    async def grant_task_reward(
        self,
        user_id: str,
        task_id: str,
        task_type: str,
        amount: Optional[float] = None,
    ) -> dict:
        """任务完成奖励（账户积分，幂等 related_id=task_id）"""
        reward_amount = float(amount if amount is not None else INCENTIVE_CONFIG.get("task_complete", 1))
        if reward_amount <= 0:
            return {"success": True, "skipped": True, "amount": 0}
        return await self.adjust_user_balance(
            user_id=user_id,
            delta=reward_amount,
            type_="reward",
            category="task_reward",
            description=f"完成 {task_type} 任务奖励",
            related_id=task_id,
        )

    async def grant_member_subscribe_reward(
        self,
        user_id: str,
        plan_name: str,
        member_level: str,
        order_id: Optional[str] = None,
        price: Optional[float] = None,
        bonus: Optional[int] = None,
    ) -> dict:
        """会员订阅奖励（账户积分，幂等 related_id=order_id）

        计算优先级：
        1. 传入 price 且 > 0：按 SystemConfig.credits 的 exchangePoints : exchangeYuan
           兑换比例赠送 = price * points / coins
        2. 否则传入 bonus 且 > 0：直接使用
        3. 否则回退 INCENTIVE_CONFIG 默认值
        """
        amount = 0.0
        if price is not None and float(price) > 0:
            points, coins = await self._get_exchange_rate()
            try:
                amount = round(float(price) * float(points) / float(coins), 2)
            except Exception:
                amount = 0.0
            description = (
                f"会员订阅奖励 - {plan_name}"
                f"（支付 {float(price):g} 元按兑换比例赠送：{points} 积分={coins} 元）"
            )
        elif bonus is not None and bonus > 0:
            amount = float(bonus)
            description = f"会员订阅奖励 - {plan_name}"
        else:
            amount = float(INCENTIVE_CONFIG.get(f"member_subscribe_{member_level}", 50))
            description = f"会员订阅奖励 - {plan_name}"

        if amount <= 0:
            return {"success": True, "skipped": True, "amount": 0}
        return await self.adjust_user_balance(
            user_id=user_id,
            delta=amount,
            type_="reward",
            category="member_subscribe",
            description=description,
            related_id=order_id or f"member_subscribe_{user_id}",
            related_order_no=order_id,
        )

    # ============ 账户积分激励（入 totalIncentive） ============

    async def grant_invite_register_reward(
        self,
        inviter_id: str,
        invitee_name: str,
        invitee_id: Optional[str] = None,
    ) -> dict:
        """
        邀请注册奖励（进入账户积分余额）

        幂等：以 invite_register_<invitee_id> 为 key，防止同一被邀请人重复触发
        """
        amount = float(INCENTIVE_CONFIG.get("invite_register", 100))
        related_id = f"invite_register_{invitee_id}" if invitee_id else None
        result = await self.adjust_user_balance(
            user_id=inviter_id,
            delta=amount,
            type_="reward",
            category="invite_register",
            description=f"邀请 {invitee_name} 注册奖励",
            related_id=related_id,
        )
        if result.get("success") and not result.get("skipped"):
            return {"success": True, "amount": amount, "message": "邀请奖励已发放"}
        return result

    async def grant_recharge_reward(
        self,
        user_id: str,
        recharge_amount: float,
        order_id: Optional[str] = None,
    ) -> dict:
        """
        充值赠送积分（进入账户积分余额）

        按管理端 SystemConfig.credits 的兑换比例 exchangePoints : exchangeYuan
        计算赠送金额 = recharge_amount * (points / coins)
        例：默认 100:1 时，充 10 元 赠送 1000 积分
        幂等以 order_id 为 key
        """
        points, coins = await self._get_exchange_rate()
        try:
            bonus = round(float(recharge_amount) * float(points) / float(coins), 2)
        except Exception:
            bonus = 0.0
        if bonus <= 0:
            return {"success": False, "error": "赠送积分为 0"}
        related_id = f"recharge_reward_{order_id}" if order_id else None
        result = await self.adjust_user_balance(
            user_id=user_id,
            delta=bonus,
            type_="reward",
            category="recharge_reward",
            description=f"充值 {recharge_amount:g} 元按兑换比例赠送 {bonus:g} 积分（{points} 积分={coins} 元）",
            related_id=related_id,
            related_order_no=order_id,
        )
        if result.get("success") and not result.get("skipped"):
            return {"success": True, "amount": bonus, "message": "充值赠送积分已发放"}
        return result

    async def grant_daily_sign_reward(self, user_id: str) -> dict:
        """
        每日签到奖励（进入账户积分余额）

        幂等：按 YYYY-MM-DD 去重，同日重复签到不重复发放
        """
        try:
            user = await parse_client.get_user(user_id)
        except Exception:
            return {"success": False, "error": "用户不存在"}

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        # 先查 DailySign 去重
        try:
            existed = await parse_client.query_objects(
                "DailySign",
                where={"userId": user_id, "signDate": today},
                limit=1,
            )
            if existed.get("results"):
                return {"success": False, "error": "今日已签到", "signed": True}
        except Exception as e:
            logger.warning(f"[签到] 查询历史失败: {e}")

        member_level = user.get("memberLevel", "normal")
        is_paid = member_level in ("vip", "svip")
        amount = float(
            INCENTIVE_CONFIG.get("daily_login_paid" if is_paid else "daily_login_normal", 5)
        )

        # 计算连续天数（查昨天是否有签到）
        try:
            from datetime import timedelta
            yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
            y = await parse_client.query_objects(
                "DailySign",
                where={"userId": user_id, "signDate": yesterday},
                limit=1,
            )
            if y.get("results"):
                continuous = int(y["results"][0].get("continuousDays", 0) or 0) + 1
            else:
                continuous = 1
        except Exception:
            continuous = 1

        related_id = f"daily_sign_{user_id}_{today}"
        result = await self.adjust_user_balance(
            user_id=user_id,
            delta=amount,
            type_="reward",
            category="daily_sign",
            description=f"每日签到奖励（连续 {continuous} 天）",
            related_id=related_id,
        )
        if not result.get("success"):
            return result

        # 写签到记录
        try:
            await parse_client.create_object(
                "DailySign",
                {
                    "userId": user_id,
                    "signDate": today,
                    "amount": amount,
                    "memberLevel": member_level,
                    "continuousDays": continuous,
                },
            )
        except Exception as e:
            logger.warning(f"[签到] 写入 DailySign 失败: {e}")

        return {
            "success": True,
            "amount": amount,
            "continuousDays": continuous,
            "message": f"签到成功，+{amount:g} 积分",
        }

    # ============ 账户积分 ⇄ Web3 金币 兑换 ============

    async def exchange_to_web3(self, user_id: str, points: float) -> dict:
        """
        账户积分 → 链上金币
        points: 要花费的账户积分
        比例：points 账户积分 = coins 链上金币
        """
        from app.core.web3_client import web3_client

        if points <= 0:
            return {"success": False, "error": "兑换数量必须大于 0"}

        try:
            user = await parse_client.get_user(user_id)
        except Exception:
            return {"success": False, "error": "用户不存在"}

        web3_address = user.get("web3Address")
        if not web3_address:
            return {"success": False, "error": "请先绑定 Web3 钱包"}

        rate_points, rate_coins = await self._get_exchange_rate()
        # 必须是兑换比例的整数倍
        if (points * rate_coins) % rate_points != 0:
            return {
                "success": False,
                "error": f"兑换数量必须是 {rate_points} 的整数倍",
            }
        coins = int(points * rate_coins / rate_points)
        if coins <= 0:
            return {"success": False, "error": "兑换金币数量不足"}

        # 1. 扣账户积分
        related_id = f"exchange_to_web3_{user_id}_{int(datetime.now(timezone.utc).timestamp() * 1000)}"
        deduct = await self.adjust_user_balance(
            user_id=user_id,
            delta=-float(points),
            type_="exchange",
            category="exchange_to_web3",
            description=f"兑换为 {coins} 链上金币",
            related_id=related_id,
            check_idempotent=False,
        )
        if not deduct.get("success"):
            return deduct

        # 2. 链上 mint
        try:
            mint_result = await web3_client.mint(web3_address, int(coins))
        except Exception as e:
            mint_result = {"success": False, "error": str(e)}

        if not mint_result.get("success"):
            # 回滚账户积分
            logger.warning(f"[兑换] mint 失败，回滚账户积分: user={user_id} points={points}")
            await self.adjust_user_balance(
                user_id=user_id,
                delta=float(points),
                type_="refund",
                category="exchange_rollback",
                description=f"兑换失败回滚：{mint_result.get('error', '')}",
                related_id=f"{related_id}_rollback",
                check_idempotent=False,
            )
            return {"success": False, "error": f"链上铸币失败，已退回积分: {mint_result.get('error', '')}"}

        # 3. 记 IncentiveLog settled
        try:
            await parse_client.create_object(
                "IncentiveLog",
                {
                    "userId": user_id,
                    "web3Address": web3_address,
                    "type": "exchange_in",
                    "amount": float(coins),
                    "txHash": mint_result.get("tx_hash", ""),
                    "description": f"由 {points:g} 账户积分兑换",
                    "status": "success",
                    "settlementStatus": "settled",
                    "relatedId": related_id,
                },
            )
        except Exception as e:
            logger.warning(f"[兑换] 写 IncentiveLog 失败: {e}")

        return {
            "success": True,
            "points": points,
            "coins": coins,
            "tx_hash": mint_result.get("tx_hash"),
            "message": f"已兑换 {coins} 链上金币",
        }

    async def exchange_to_balance(self, user_id: str, coins: float) -> dict:
        """
        链上金币 → 账户积分
        coins: 要销毁的链上金币
        """
        from app.core.web3_client import web3_client

        if coins <= 0:
            return {"success": False, "error": "兑换数量必须大于 0"}

        try:
            user = await parse_client.get_user(user_id)
        except Exception:
            return {"success": False, "error": "用户不存在"}

        web3_address = user.get("web3Address")
        if not web3_address:
            return {"success": False, "error": "请先绑定 Web3 钱包"}

        # 检查链上余额
        try:
            onchain = await web3_client.get_balance(web3_address)
        except Exception as e:
            return {"success": False, "error": f"查询链上余额失败: {e}"}
        if float(onchain) < float(coins):
            return {"success": False, "error": f"链上金币不足，当前 {onchain}"}

        rate_points, rate_coins = await self._get_exchange_rate()
        if (coins * rate_points) % rate_coins != 0:
            return {"success": False, "error": f"兑换数量必须是 {rate_coins} 的整数倍"}
        points = float(coins * rate_points / rate_coins)

        # 1. 链上 burn
        try:
            burn_result = await web3_client.burn(web3_address, int(coins))
        except Exception as e:
            burn_result = {"success": False, "error": str(e)}

        if not burn_result.get("success"):
            return {"success": False, "error": f"链上销毁失败: {burn_result.get('error', '')}"}

        related_id = f"exchange_to_balance_{burn_result.get('tx_hash') or int(datetime.now(timezone.utc).timestamp() * 1000)}"

        # 2. 加账户积分
        add = await self.adjust_user_balance(
            user_id=user_id,
            delta=float(points),
            type_="exchange",
            category="exchange_to_balance",
            description=f"由 {coins:g} 链上金币兑换",
            related_id=related_id,
            check_idempotent=False,
        )
        if not add.get("success"):
            # burn 已成功但加积分失败 → 直接报错（链上不可逆）
            logger.error(
                f"[兑换] burn 成功但加积分失败: user={user_id} coins={coins} err={add.get('error')}"
            )
            return {"success": False, "error": f"账户积分写入失败: {add.get('error', '')}"}

        # 3. 记 IncentiveLog
        try:
            await parse_client.create_object(
                "IncentiveLog",
                {
                    "userId": user_id,
                    "web3Address": web3_address,
                    "type": "exchange_out",
                    "amount": -float(coins),
                    "txHash": burn_result.get("tx_hash", ""),
                    "description": f"兑换为 {points:g} 账户积分",
                    "status": "success",
                    "settlementStatus": "settled",
                    "relatedId": related_id,
                },
            )
        except Exception as e:
            logger.warning(f"[兑换] 写 IncentiveLog 失败: {e}")

        return {
            "success": True,
            "coins": coins,
            "points": points,
            "tx_hash": burn_result.get("tx_hash"),
            "message": f"已兑换 {points:g} 账户积分",
        }


# 全局单例
incentive_service = IncentiveService()
