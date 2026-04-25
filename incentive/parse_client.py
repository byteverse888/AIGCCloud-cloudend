"""
独立的 Parse Server REST API 客户端（同步版）
用于激励结算模块读写 Parse 数据库，不依赖主应用的 async 客户端。
"""
import json
from typing import Any, Dict, List, Optional

import httpx

from incentive.config import config
from incentive.logger import logger


class ParseClient:
    """Parse Server 同步客户端"""

    def __init__(self):
        self.base_url = config.parse_server_url
        self.headers = {
            "X-Parse-Application-Id": config.parse_app_id,
            "X-Parse-Master-Key": config.parse_master_key,
            "Content-Type": "application/json",
        }
        self._client: Optional[httpx.Client] = None

    def _get_client(self) -> httpx.Client:
        """复用 httpx.Client 连接，避免每次请求创建新连接"""
        if self._client is None or self._client.is_closed:
            self._client = httpx.Client(timeout=30.0)
        return self._client

    def _request(self, method: str, endpoint: str,
                 data: Optional[Dict] = None,
                 params: Optional[Dict] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        client = self._get_client()
        try:
            resp = client.request(
                method=method, url=url, headers=self.headers,
                json=data, params=params,
            )
            if resp.status_code >= 400:
                logger.error(f"[Parse] {method} {endpoint} -> {resp.status_code}: {resp.text[:300]}")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"[Parse] 请求异常: {method} {endpoint} - {e}")
            raise

    # ========== 通用 CRUD ==========

    def create_object(self, class_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", f"/classes/{class_name}", data)

    def update_object(self, class_name: str, object_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("PUT", f"/classes/{class_name}/{object_id}", data)

    def query_objects(self, class_name: str, where: Optional[Dict] = None,
                      order: Optional[str] = None, limit: int = 100,
                      skip: int = 0, keys: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit, "skip": skip}
        if where:
            params["where"] = json.dumps(where)
        if order:
            params["order"] = order
        if keys:
            params["keys"] = keys
        return self._request("GET", f"/classes/{class_name}", params=params)

    def count_objects(self, class_name: str, where: Optional[Dict] = None) -> int:
        params: Dict[str, Any] = {"count": "1", "limit": "0"}
        if where:
            params["where"] = json.dumps(where)
        result = self._request("GET", f"/classes/{class_name}", params=params)
        return result.get("count", 0)

    def batch_operations(self, requests: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """批量操作（最多 50 条/批）"""
        return self._request("POST", "/batch", {"requests": requests})

    # ========== 用户操作 ==========

    def get_user(self, user_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/users/{user_id}")

    def update_user(self, user_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("PUT", f"/users/{user_id}", data)

    def query_users(self, where: Optional[Dict] = None, limit: int = 100,
                    skip: int = 0, keys: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit, "skip": skip}
        if where:
            params["where"] = json.dumps(where)
        if keys:
            params["keys"] = keys
        return self._request("GET", f"/classes/_User", params=params)

    def find_user_by_eth(self, eth_address: str) -> Optional[Dict[str, Any]]:
        """通过 ETH 地址查找用户（大小写不敏感）"""
        result = self.query_users(
            where={"web3Address": {"$regex": f"^{eth_address}$", "$options": "i"}},
            limit=1,
        )
        users = result.get("results", [])
        return users[0] if users else None

    # ========== 批量更新用户积分 ==========

    def batch_update_users(self, updates: List[Dict[str, Any]]):
        """
        批量更新用户字段。
        updates: [{"objectId": "xxx", "data": {"totalPoints": 100, ...}}, ...]
        每 50 条一批。
        """
        batch_size = 50
        for i in range(0, len(updates), batch_size):
            chunk = updates[i: i + batch_size]
            requests = []
            for item in chunk:
                requests.append({
                    "method": "PUT",
                    "path": f"/parse/users/{item['objectId']}",
                    "body": item["data"],
                })
            try:
                self.batch_operations(requests)
                logger.info(f"[Parse] 批量更新用户: {len(chunk)} 条")
            except Exception as e:
                logger.error(f"[Parse] 批量更新失败 (offset={i}): {e}")

    # ========== IncentiveLog 操作 ==========

    def create_incentive_log(self, user_id: str, eth_address: str,
                             log_type: str, amount: float,
                             description: str, batch_id: str = "") -> Dict[str, Any]:
        """创建一条积分日志"""
        data = {
            "userId": user_id,
            "web3Address": eth_address,
            "type": log_type,
            "amount": amount,
            "description": description,
            "settlementStatus": "unsettled",
            "batchId": batch_id,
        }
        return self.create_object("IncentiveLog", data)

    def get_unsettled_logs(self, min_amount: float = 0, limit: int = 1000) -> List[Dict[str, Any]]:
        """获取待清算日志"""
        where = {
            "settlementStatus": "unsettled",
        }
        if min_amount > 0:
            where["amount"] = {"$gte": min_amount}
        result = self.query_objects("IncentiveLog", where=where, limit=limit)
        return result.get("results", [])

    def mark_logs_settled(self, log_ids: List[str], tx_hash: str, batch_id: str):
        """标记日志为已清算"""
        batch_size = 50
        for i in range(0, len(log_ids), batch_size):
            chunk = log_ids[i: i + batch_size]
            requests = []
            for oid in chunk:
                requests.append({
                    "method": "PUT",
                    "path": f"/parse/classes/IncentiveLog/{oid}",
                    "body": {
                        "settlementStatus": "settled",
                        "txHash": tx_hash,
                        "batchId": batch_id,
                        "settledAt": {"__type": "Date", "iso": _iso_now()},
                    },
                })
            try:
                self.batch_operations(requests)
            except Exception as e:
                logger.error(f"[Parse] 标记清算失败 (offset={i}): {e}")

    # ========== 全局配置读取 ==========

    def get_config_value(self, key: str, default: Any = None) -> Any:
        """从 Parse config 表中读取配置"""
        try:
            result = self._request("GET", "/config")
            params = result.get("params", {})
            return params.get(key, default)
        except Exception:
            return default


def _iso_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


# 全局单例
parse_client = ParseClient()
