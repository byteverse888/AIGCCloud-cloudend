"""
Parse Server REST API 客户端
"""
import httpx
import json as json_lib
from typing import Optional, Dict, Any, List
from app.core.config import settings
from app.core.logger import logger


class ParseClient:
    """Parse Server REST API 客户端"""
    
    def __init__(self):
        self.base_url = settings.parse_server_url
        self.app_id = settings.parse_app_id
        self.rest_api_key = settings.parse_rest_api_key
        self.master_key = settings.parse_master_key
        self.headers = {
            "X-Parse-Application-Id": self.app_id,
            "X-Parse-REST-API-Key": self.rest_api_key,
            "Content-Type": "application/json",
        }
        # 需要 Master Key 的请求使用此 headers
        self.master_headers = {
            "X-Parse-Application-Id": self.app_id,
            "X-Parse-Master-Key": self.master_key,
            "Content-Type": "application/json",
        }
        # 全局连接池（复用 TCP 连接，避免每次请求建立新连接）
        self._client: Optional[httpx.AsyncClient] = None
        self._master_client: Optional[httpx.AsyncClient] = None
    
    async def _get_client(self) -> httpx.AsyncClient:
        """获取普通请求客户端（带连接池）"""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                headers=self.headers,
                timeout=30.0,
                limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            )
        return self._client
    
    async def _get_master_client(self) -> httpx.AsyncClient:
        """获取 Master Key 请求客户端（带连接池）"""
        if self._master_client is None or self._master_client.is_closed:
            self._master_client = httpx.AsyncClient(
                headers=self.master_headers,
                timeout=30.0,
                limits=httpx.Limits(max_connections=50, max_keepalive_connections=10),
            )
        return self._master_client
    
    async def close(self):
        """关闭连接池（应用关闭时调用）"""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
        if self._master_client and not self._master_client.is_closed:
            await self._master_client.aclose()
    
    async def _request(
        self, 
        method: str, 
        endpoint: str, 
        data: Optional[Dict] = None,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """发送请求到 Parse Server（使用连接池）"""
        url = f"{self.base_url}{endpoint}"
        
        # 调试日志：请求信息
        logger.debug(f"[Parse] 请求: {method} {url}")
        if data:
            safe_data = {k: ('***' if k in ['password'] else v) for k, v in data.items()}
            logger.debug(f"[Parse] Body: {json_lib.dumps(safe_data, ensure_ascii=False)}")
        if params:
            logger.debug(f"[Parse] Params: {params}")
        
        client = await self._get_client()
        try:
            response = await client.request(
                method=method,
                url=url,
                json=data,
                params=params,
            )
            
            logger.debug(f"[Parse] 响应: {response.status_code}")
            if response.status_code >= 400:
                logger.error(f"[Parse] 错误响应: {response.text}")
            
            response.raise_for_status()
            result = response.json()
            logger.debug(f"[Parse] 成功: {str(result)[:200]}...")
            return result
        except httpx.HTTPStatusError as e:
            logger.error(f"[Parse] HTTP错误: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"[Parse] 请求异常: {str(e)}")
            raise
    
    # ============ 对象操作 ============
    
    async def create_object(self, class_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """创建对象"""
        return await self._request("POST", f"/classes/{class_name}", data)
    
    async def get_object(self, class_name: str, object_id: str) -> Dict[str, Any]:
        """获取单个对象"""
        return await self._request("GET", f"/classes/{class_name}/{object_id}")
    
    async def update_object(self, class_name: str, object_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """更新对象"""
        return await self._request("PUT", f"/classes/{class_name}/{object_id}", data)
    
    async def delete_object(self, class_name: str, object_id: str) -> Dict[str, Any]:
        """删除对象"""
        return await self._request("DELETE", f"/classes/{class_name}/{object_id}")
    
    async def query_objects(
        self, 
        class_name: str, 
        where: Optional[Dict] = None,
        order: Optional[str] = None,
        limit: int = 100,
        skip: int = 0,
        count: bool = False,
        include: Optional[str] = None
    ) -> Dict[str, Any]:
        """查询对象列表"""
        import json
        params = {"limit": limit, "skip": skip}
        if where:
            params["where"] = json.dumps(where)
        if order:
            params["order"] = order
        if count:
            params["count"] = "1"
        if include:
            params["include"] = include
        return await self._request("GET", f"/classes/{class_name}", params=params)
    
    async def count_objects(self, class_name: str, where: Optional[Dict] = None) -> int:
        """统计对象数量"""
        import json
        params = {"count": "1", "limit": "0"}
        if where:
            params["where"] = json.dumps(where)
        result = await self._request("GET", f"/classes/{class_name}", params=params)
        return result.get("count", 0)
    
    async def query(self, class_name: str, where: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """简化查询，直接返回 results 列表"""
        result = await self.query_objects(class_name, where=where)
        return result.get("results", [])
    
    async def query_and_update(
        self, 
        class_name: str, 
        where: Dict[str, Any], 
        data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """查询第一个匹配对象并更新
        
        Args:
            class_name: 类名
            where: 查询条件
            data: 要更新的数据
            
        Returns:
            更新后的对象，或者 None 如果没找到
        """
        results = await self.query(class_name, where)
        if not results:
            logger.warning(f"[Parse] query_and_update: 未找到匹配对象 {class_name} {where}")
            return None
        
        obj = results[0]
        object_id = obj.get("objectId")
        if not object_id:
            logger.error(f"[Parse] query_and_update: 对象缺少 objectId")
            return None
        
        return await self.update_object(class_name, object_id, data)
    
    async def batch_operations(self, requests: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """批量操作"""
        return await self._request("POST", "/batch", {"requests": requests})
    
    # ============ 用户操作 ============
    
    async def create_user(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """创建用户
        
        Args:
            data: 用户数据字典，必须包含 username 和 password
        """
        return await self._request("POST", "/users", data)
    
    async def get_user(self, user_id: str) -> Dict[str, Any]:
        """获取用户信息（使用 Master Key）"""
        logger.info(f"[Parse] get_user called with user_id: {user_id}")
        url = f"{self.base_url}/users/{user_id}"
        client = await self._get_master_client()
        try:
            logger.info(f"[Parse] get_user URL: {url}")
            response = await client.get(url)
            logger.info(f"[Parse] get_user response: {response.status_code} - {response.text[:100]}")
            if response.status_code >= 400:
                logger.error(f"[Parse] 获取用户失败: {response.text}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"[Parse] 获取用户异常: {str(e)}")
            raise
    
    async def get_current_user(self, session_token: str) -> Dict[str, Any]:
        """通过 session token 获取当前用户信息"""
        client = await self._get_client()
        response = await client.get(
            f"{self.base_url}/users/me",
            headers={"X-Parse-Session-Token": session_token},
        )
        response.raise_for_status()
        return response.json()
    
    async def validate_session(self, session_token: str, expected_user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        验证 session token 并检查用户匹配
        
        Args:
            session_token: Parse session token
            expected_user_id: 预期的用户 ID，如果提供则验证是否匹配
            
        Returns:
            用户信息字典
            
        Raises:
            HTTPException: session 无效或用户不匹配
        """
        try:
            user = await self.get_current_user(session_token)
            user_id = user.get("objectId")
            
            # 如果提供了预期的用户 ID，验证是否匹配
            if expected_user_id and user_id != expected_user_id:
                logger.warning(f"[Session验证] 用户ID不匹配: session对应{user_id}, 请求的{expected_user_id}")
                raise ValueError("用户身份不匹配")
            
            logger.debug(f"[Session验证] 成功: user_id={user_id}, username={user.get('username')}")
            return user
        except httpx.HTTPStatusError as e:
            logger.warning(f"[Session验证] 失败: {e.response.status_code}")
            raise ValueError("Session无效或已过期")
        except Exception as e:
            logger.error(f"[Session验证] 异常: {e}")
            raise
    
    async def update_user(self, user_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """更新用户信息（需要 Master Key 或正确的 Session Token）"""
        return await self._request("PUT", f"/users/{user_id}", data)
    
    async def login_user(self, username: str, password: str) -> Dict[str, Any]:
        """通过 Parse Server 验证用户名密码登录
        
        Returns:
            成功时返回用户数据（含 sessionToken）
            
        Raises:
            httpx.HTTPStatusError: 登录失败时抛出
        """
        url = f"{self.base_url}/login"
        client = await self._get_client()
        response = await client.get(
            url,
            params={"username": username, "password": password},
            headers={"X-Parse-Revocable-Session": "1"},
        )
        response.raise_for_status()
        return response.json()

    async def logout_session(self, session_token: str) -> bool:
        """撤销 Parse session token
        
        Returns:
            True 表示登出成功，False 表示失败（不抛异常）
        """
        url = f"{self.base_url}/logout"
        client = await self._get_client()
        try:
            response = await client.post(
                url,
                headers={"X-Parse-Session-Token": session_token},
            )
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"[Parse] logout 失败: {e}")
            return False

    async def update_user_with_master_key(self, user_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """使用 Master Key 更新用户信息（用于 emailVerified 等敏感字段）"""
        url = f"{self.base_url}/users/{user_id}"
        logger.info(f"[Parse] 更新用户(Master): {user_id}, 数据: {data}")
        
        client = await self._get_master_client()
        try:
            response = await client.put(url, json=data)
            logger.info(f"[Parse] 更新用户响应: {response.status_code}")
            if response.status_code >= 400:
                logger.error(f"[Parse] 更新用户失败: {response.text}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"[Parse] 更新用户异常: {e}")
            raise
    
    async def update_user_with_session(self, user_id: str, data: Dict[str, Any], session_token: str) -> Dict[str, Any]:
        """使用 session token 更新用户信息"""
        url = f"{self.base_url}/users/{user_id}"
        logger.info(f"[Parse] 更新用户(session): {user_id}, 数据: {data}")
        
        client = await self._get_client()
        try:
            response = await client.put(
                url,
                json=data,
                headers={"X-Parse-Session-Token": session_token},
            )
            logger.info(f"[Parse] 更新用户响应: {response.status_code}")
            if response.status_code >= 400:
                logger.error(f"[Parse] 更新用户失败: {response.text}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"[Parse] 更新用户异常: {e}")
            raise
    
    async def count_users(self, where: Optional[Dict] = None) -> int:
        """
        统计 _User 类的用户数（必须用 Master Key）
        Parse Server 对 _User 类的普通 REST API Key 请求受 ACL/CLP 限制，
        会导致 count 返回 0，故这里必须显式使用 Master Key。
        """
        import json
        params = {"count": "1", "limit": "0"}
        if where:
            params["where"] = json.dumps(where)
        url = f"{self.base_url}/classes/_User"
        client = await self._get_master_client()
        try:
            response = await client.get(url, params=params)
            if response.status_code >= 400:
                logger.error(f"[Parse] count_users 失败: {response.text}")
                response.raise_for_status()
            return response.json().get("count", 0)
        except Exception as e:
            logger.error(f"[Parse] count_users 异常: {e}")
            raise

    async def query_users(
        self,
        where: Optional[Dict] = None,
        order: Optional[str] = None,
        limit: int = 100,
        skip: int = 0
    ) -> Dict[str, Any]:
        """查询用户列表（使用 Master Key 查询 /classes/_User）"""
        import json
        params = {"limit": limit, "skip": skip}
        if where:
            # PostgreSQL 不支持 $or 查询，改用两次查询
            if "$or" in where:
                or_conditions = where["$or"]
                results = []
                for cond in or_conditions:
                    single_params = {"limit": limit, "skip": 0, "where": json.dumps(cond)}
                    url = f"{self.base_url}/classes/_User"
                    client = await self._get_master_client()
                    try:
                        response = await client.get(url, params=single_params)
                        if response.status_code == 200:
                            data = response.json()
                            results.extend(data.get("results", []))
                    except Exception:
                        pass
                return {"results": results[:limit]}
            params["where"] = json.dumps(where)
        if order:
            params["order"] = order
        
        url = f"{self.base_url}/classes/_User"
        client = await self._get_master_client()
        try:
            response = await client.get(url, params=params)
            logger.debug(f"[Parse] 查询用户: {response.status_code}")
            if response.status_code >= 400:
                logger.error(f"[Parse] 查询用户失败: {response.text}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"[Parse] 查询用户异常: {str(e)}")
            raise
    
    # ============ 云函数调用 ============
    
    async def call_function(self, name: str, data: Optional[Dict] = None) -> Dict[str, Any]:
        """调用云函数"""
        return await self._request("POST", f"/functions/{name}", data or {})
    
    # ============ Schema 初始化 ============
    
    # 项目中用到的所有 Parse 业务类及其字段定义
    SCHEMA_DEFINITIONS: Dict[str, Dict] = {
        "Order": {
            "orderNo": {"type": "String"},
            "userId": {"type": "String"},
            "type": {"type": "String"},
            "amount": {"type": "Number"},
            "coins": {"type": "Number"},
            "status": {"type": "String"},
            "description": {"type": "String"},
            "paymentMethod": {"type": "String"},
            "plan": {"type": "String"},
            "productId": {"type": "String"},
            "productName": {"type": "String"},
            "buyerAddress": {"type": "String"},
            "sellerAddress": {"type": "String"},
            "txHash": {"type": "String"},
            "paidAt": {"type": "String"},
            "completedAt": {"type": "String"},
            "failReason": {"type": "String"}
        },
        "Product": {
            "name": {"type": "String"},
            "description": {"type": "String"},
            "cover": {"type": "String"},
            "price": {"type": "Number"},
            "owner": {"type": "String"},
            "status": {"type": "String"},
            "category": {"type": "String"},
            "creatorId": {"type": "String"},
            "creatorName": {"type": "String"},
            "creatorAddress": {"type": "String"},
            "mockType": {"type": "String"},
            "mockOwner": {"type": "String"},
            "sales": {"type": "Number"},
            "likeCount": {"type": "Number"},
            "favoriteCount": {"type": "Number"},
            "views": {"type": "Number"},
            "rating": {"type": "Number"},
            "commentCount": {"type": "Number"},
            "tags": {"type": "Array"},
            "reportCount": {"type": "Number"},
            "reviewedAt": {"type": "String"},
            "reviewedBy": {"type": "String"},
            "reviewNote": {"type": "String"},
            "offlineReason": {"type": "String"},
            "copyright": {"type": "String"},
            "license": {"type": "String"},
        },
        "ProductReview": {
            "productId": {"type": "String"},
            "operatorId": {"type": "String"},
            "status": {"type": "String"},
            "previousStatus": {"type": "String"},
            "note": {"type": "String"},
        },
        "ProductReport": {
            "productId": {"type": "String"},
            "reporterId": {"type": "String"},
            "reason": {"type": "String"},
            "description": {"type": "String"},
            "status": {"type": "String"},
            "handledAt": {"type": "String"},
            "handledBy": {"type": "String"},
            "action": {"type": "String"},
        },
        "MemberOrder": {
            "orderId": {"type": "String"},
            "userId": {"type": "String"},
            "planId": {"type": "String"},
            "planName": {"type": "String"},
            "level": {"type": "String"},
            "days": {"type": "Number"},
            "amount": {"type": "Number"},
            "bonus": {"type": "Number"},
            "status": {"type": "String"},
            "failReason": {"type": "String"}
        },
        "AITask": {
            "taskId": {"type": "String"},
            "designer": {"type": "String"},
            "executor": {"type": "String"},
            "type": {"type": "String"},
            "model": {"type": "String"},
            "data": {"type": "Object"},
            "status": {"type": "Number"},
            "cost": {"type": "Number"},
            "results": {"type": "Array"},
            "errorMessage": {"type": "String"},
            "rewardAmount": {"type": "Number"},
            "rewardTxHash": {"type": "String"},
            "claimedAt": {"type": "String"},
            "startedAt": {"type": "String"},
            "completedAt": {"type": "String"},
            "error": {"type": "String"}
        },
        "Incentive": {
            "userId": {"type": "String"},
            "type": {"type": "String"},
            "amount": {"type": "Number"},
            "description": {"type": "String"}
        },
        "IncentiveLog": {
            "userId": {"type": "String"},
            "web3Address": {"type": "String"},
            "type": {"type": "String"},
            "amount": {"type": "Number"},
            "txHash": {"type": "String"},
            "description": {"type": "String"},
            "status": {"type": "String"},
            "relatedId": {"type": "String"},
            "settlementStatus": {"type": "String"},
            "settledAt": {"type": "String"},
            "batchId": {"type": "String"}
        },
        # 云端模型市场
        "models": {
            "name": {"type": "String"},
            "description": {"type": "String"},
            "category": {"type": "String"},
            "paramSize": {"type": "String"},
            "modelSize": {"type": "String"},
            "float": {"type": "String"},
            "GPUMem": {"type": "String"},
            "version": {"type": "String"},
            "author": {"type": "String"},
            "tags": {"type": "Array"},
            "download": {"type": "Array"},
            "downloads": {"type": "Number"},
            "rating": {"type": "Number"},
            "likeCount": {"type": "Number"},
            "favoriteCount": {"type": "Number"},
        },
        # 云端应用市场
        "AIApp": {
            "name": {"type": "String"},
            "description": {"type": "String"},
            "category": {"type": "String"},
            "icon": {"type": "String"},
            "cover": {"type": "String"},
            "version": {"type": "String"},
            "versions": {"type": "Array"},
            "author": {"type": "String"},
            "tags": {"type": "Array"},
            "modelId": {"type": "String"},
            "modelName": {"type": "String"},
            "modelSource": {"type": "String"},
            "useCount": {"type": "Number"},
            "likeCount": {"type": "Number"},
            "favoriteCount": {"type": "Number"},
            "downloadCount": {"type": "Number"},
            "status": {"type": "String"},
            "isFeatured": {"type": "Boolean"},
            "isFree": {"type": "Boolean"},
            "paidLevel": {"type": "String"},
            "deployment": {"type": "Object"},
            "service": {"type": "Object"},
            "imgSize": {"type": "String"},
            "dockerImgUrl": {"type": "String"},
            "supportedType": {"type": "Array"},
        },
        # 用户交互记录（喜欢/收藏）
        "UserAction": {
            "userId": {"type": "String"},
            "targetId": {"type": "String"},
            "targetClass": {"type": "String"},
            "action": {"type": "String"},
        },
        # AIIP资产
        "AIIPAsset": {
            "name": {"type": "String"},
            "category": {"type": "String"},
            "description": {"type": "String"},
            "cover": {"type": "String"},
            "status": {"type": "String"},
            "price": {"type": "Number"},
            "ownerId": {"type": "String"},
            "ownerAddress": {"type": "String"},
            "ownerName": {"type": "String"},
            "mockOwner": {"type": "String"},
            "views": {"type": "Number"},
            "isListed": {"type": "Boolean"},
            "listedProductId": {"type": "String"},
            "tags": {"type": "Array"},
            "copyright": {"type": "String"},
            "license": {"type": "String"},
            "assetUrl": {"type": "String"},
            # 审核相关（用于展示驳回/下架原因与审核记录）
            "reviewedAt": {"type": "String"},
            "reviewedBy": {"type": "String"},
            "reviewNote": {"type": "String"},
            "offlineReason": {"type": "String"},
        },
        # 评论
        "Comment": {
            "productId": {"type": "String"},
            "userId": {"type": "String"},
            "userName": {"type": "String"},
            "userAvatar": {"type": "String"},
            "content": {"type": "String"},
            "rating": {"type": "Number"},
            "parentId": {"type": "String"},
            "replyToId": {"type": "String"},  # 回复的用户ID
            "likeCount": {"type": "Number"},
        },
        # 点赞
        "Like": {
            "productId": {"type": "String"},
            "userId": {"type": "String"},
        },
        # 收藏
        "Favorite": {
            "productId": {"type": "String"},
            "userId": {"type": "String"},
        },
        # 关注
        "Follow": {
            "followerId": {"type": "String"},
            "followingId": {"type": "String"},
        },
        # 提现申请
        "WithdrawRequest": {
            "userId": {"type": "String"},
            "amount": {"type": "Number"},
            "method": {"type": "String"},
            "account": {"type": "String"},
            "accountName": {"type": "String"},
            "status": {"type": "String"},
        },
        # 收益记录
        "EarningRecord": {
            "userId": {"type": "String"},
            "type": {"type": "String"},
            "amount": {"type": "Number"},
            "description": {"type": "String"},
            "status": {"type": "String"},
        },
        # 券码
        "Coupon": {
            "code": {"type": "String"},
            "type": {"type": "String"},
            "value": {"type": "Number"},
            "minAmount": {"type": "Number"},
            "scope": {"type": "String"},
            "scopeDetail": {"type": "String"},
            "startDate": {"type": "String"},
            "endDate": {"type": "String"},
            "totalCount": {"type": "Number"},
            "usedCount": {"type": "Number"},
            "status": {"type": "String"},
            "createdBy": {"type": "String"},
        },
        # 促销活动
        "Promotion": {
            "name": {"type": "String"},
            "type": {"type": "String"},
            "status": {"type": "String"},
            "discount": {"type": "Number"},
            "minAmount": {"type": "Number"},
            "giftProduct": {"type": "String"},
            "startDate": {"type": "String"},
            "endDate": {"type": "String"},
            "productCount": {"type": "Number"},
            "orderCount": {"type": "Number"},
            "revenue": {"type": "Number"},
            "createdBy": {"type": "String"},
        },
        # 充值方案
        "RechargePlan": {
            "amount": {"type": "Number"},
            "bonus": {"type": "Number"},
            "enabled": {"type": "Boolean"},
        },
        # 充值记录
        "RechargeRecord": {
            "userId": {"type": "String"},
            "username": {"type": "String"},
            "amount": {"type": "Number"},
            "bonus": {"type": "Number"},
            "method": {"type": "String"},
            "status": {"type": "String"},
        },
        # 平台账户明细（所有 totalIncentive 变动都必须在此留痕）
        "AccountRecord": {
            "userId": {"type": "String"},
            "username": {"type": "String"},
            "type": {"type": "String"},          # recharge/purchase/refund/reward/exchange/consume/settlement
            "category": {"type": "String"},      # admin_recharge/product_purchase/product_income/task_cost/task_refund/daily_sign/exchange_to_web3/exchange_to_balance 等
            "amount": {"type": "Number"},        # 变动金额（正加负减）
            "balance_before": {"type": "Number"},
            "balance_after": {"type": "Number"},
            "balance": {"type": "Number"},       # 兼容旧字段，=balance_after
            "description": {"type": "String"},
            "relatedOrderNo": {"type": "String"},
            "relatedId": {"type": "String"},     # 幂等/去重 key（product_id/task_id/order_id/tx_hash 等）
            "status": {"type": "String"},         # success/failed/pending
            "operator_id": {"type": "String"},
            "operator_name": {"type": "String"},
        },
        # 支付/结算失败补偿表（卖家入账失败、兑换失败等）
        "FailedSettlement": {
            "scene": {"type": "String"},         # seller_income / exchange_to_balance / exchange_to_web3 等
            "userId": {"type": "String"},
            "amount": {"type": "Number"},
            "relatedId": {"type": "String"},     # order_id / tx_hash
            "status": {"type": "String"},         # pending/retrying/success/failed
            "retryCount": {"type": "Number"},
            "maxRetry": {"type": "Number"},
            "errorMessage": {"type": "String"},
            "lastRetryAt": {"type": "String"},
            "resolvedAt": {"type": "String"},
        },
        # 每日签到记录（以日期防重）
        "DailySign": {
            "userId": {"type": "String"},
            "signDate": {"type": "String"},      # YYYY-MM-DD
            "amount": {"type": "Number"},
            "memberLevel": {"type": "String"},
            "continuousDays": {"type": "Number"},
        },
        # 系统配置
        "SystemConfig": {
            "category": {"type": "String"},
            "settings": {"type": "Object"},
            "updatedBy": {"type": "String"},
            "key": {"type": "String"},
            "value": {"type": "String"},
            "label": {"type": "String"},
            "group": {"type": "String"},
        },
        # 商品举报
        "ProductReport": {
            "productId": {"type": "String"},
            "userId": {"type": "String"},
            "reason": {"type": "String"},
            "description": {"type": "String"},
            "status": {"type": "String"},
        },
        # 管理/运营操作日志（登录登出、用户增删改、充值等）
        "OperationLog": {
            "operatorId": {"type": "String"},
            "operatorName": {"type": "String"},
            "operatorRole": {"type": "String"},
            "action": {"type": "String"},
            "module": {"type": "String"},
            "targetClass": {"type": "String"},
            "targetId": {"type": "String"},
            "targetName": {"type": "String"},
            "description": {"type": "String"},
            "detail": {"type": "Object"},
            "ipAddress": {"type": "String"},
            "userAgent": {"type": "String"},
            "status": {"type": "String"},
            "errorMessage": {"type": "String"},
        },
    }
    
    # CLP 权限配置：根据环境区分
    # 开发环境：全开放；生产环境：限制 addField 仅 Master Key
    @property
    def DEFAULT_CLP(self) -> dict:
        if settings.debug:
            return {
                "find": {"*": True},
                "count": {"*": True},
                "get": {"*": True},
                "create": {"*": True},
                "update": {"*": True},
                "delete": {"*": True},
                "addField": {"*": True},
            }
        return {
            "find": {"*": True},
            "count": {"*": True},
            "get": {"*": True},
            "create": {"*": True},
            "update": {"*": True},
            "delete": {"requiresAuthentication": True},
            "addField": {},  # 仅 Master Key 可添加字段
        }
    
    async def ensure_schema(self):
        """确保所有业务类在 Parse Server 中存在并包含正确类型的字段（Schema API + Master Key）"""
        client = await self._get_master_client()
        for class_name, fields in self.SCHEMA_DEFINITIONS.items():
            try:
                # 用 Schema API 检查类是否存在
                resp = await client.get(
                    f"{self.base_url}/schemas/{class_name}",
                )
                if resp.status_code == 200:
                    existing = resp.json().get("fields", {})
                    missing = {}
                    type_mismatch = {}
                    for k, v in fields.items():
                        if k not in existing:
                            missing[k] = v
                        elif existing[k].get("type") != v.get("type"):
                            type_mismatch[k] = v
                    
                    # 先删除类型不匹配的字段，再重新添加
                    if type_mismatch:
                        logger.warning(f"[Parse] 字段类型不匹配 {class_name}: {list(type_mismatch.keys())}")
                        delete_fields = {k: {"__op": "Delete"} for k in type_mismatch}
                        del_resp = await client.put(
                            f"{self.base_url}/schemas/{class_name}",
                            json={"className": class_name, "fields": delete_fields},
                        )
                        if del_resp.status_code == 200:
                            missing.update(type_mismatch)
                            logger.info(f"[Parse] 旧字段已删除，将重建: {list(type_mismatch.keys())}")
                        else:
                            logger.error(f"[Parse] 删除字段失败: {class_name} - {del_resp.text}")
                    
                    # 添加缺少的字段（含类型修复后重建的）
                    if missing:
                        logger.info(f"[Parse] 补充字段 {class_name}: {list(missing.keys())}")
                        add_resp = await client.put(
                            f"{self.base_url}/schemas/{class_name}",
                            json={"className": class_name, "fields": missing},
                        )
                        if add_resp.status_code == 200:
                            logger.info(f"[Parse] 字段补充成功: {class_name}")
                        else:
                            logger.error(f"[Parse] 字段补充失败: {class_name} - {add_resp.text}")
                    else:
                        logger.debug(f"[Parse] Schema OK: {class_name}")
                    
                    # 确保 CLP 正确
                    await self._ensure_clp(client, class_name)
                    continue
                
                # 类不存在，创建并带上字段定义和 CLP
                logger.info(f"[Parse] 创建 Schema: {class_name}")
                create_resp = await client.post(
                    f"{self.base_url}/schemas/{class_name}",
                    json={
                        "className": class_name,
                        "fields": fields,
                        "classLevelPermissions": self.DEFAULT_CLP,
                    },
                )
                if create_resp.status_code in (200, 201):
                    logger.info(f"[Parse] Schema 创建成功: {class_name}")
                else:
                    logger.error(f"[Parse] Schema 创建失败: {class_name} - {create_resp.text}")
            except Exception as e:
                logger.error(f"[Parse] ensure_schema 异常({class_name}): {e}")
    
    async def _ensure_clp(self, client, class_name: str):
        """确保指定类的 CLP 允许客户端访问"""
        try:
            resp = await client.put(
                f"{self.base_url}/schemas/{class_name}",
                json={"className": class_name, "classLevelPermissions": self.DEFAULT_CLP},
            )
            if resp.status_code == 200:
                logger.debug(f"[Parse] CLP 已更新: {class_name}")
            else:
                logger.warning(f"[Parse] CLP 更新失败: {class_name} - {resp.text}")
        except Exception as e:
            logger.warning(f"[Parse] CLP 更新异常({class_name}): {e}")
    
    # ============ 辅助方法 ============
    
    @staticmethod
    def pointer(class_name: str, object_id: str) -> Dict[str, str]:
        """创建指针引用"""
        return {
            "__type": "Pointer",
            "className": class_name,
            "objectId": object_id
        }
    
    @staticmethod
    def increment(amount: int = 1) -> Dict[str, Any]:
        """创建自增操作"""
        return {
            "__op": "Increment",
            "amount": amount
        }
    
    @staticmethod
    def add_relation(objects: List[Dict]) -> Dict[str, Any]:
        """创建添加关系操作"""
        return {
            "__op": "AddRelation",
            "objects": objects
        }
    
    @staticmethod
    def remove_relation(objects: List[Dict]) -> Dict[str, Any]:
        """创建移除关系操作"""
        return {
            "__op": "RemoveRelation",
            "objects": objects
        }

    async def ensure_default_users(self):
        """确保默认管理员和运营用户存在"""
        default_users = [
            {"username": "admin", "password": "Admin@123456", "email": "admin@aigccloud.com", "role": "admin", "level": 99, "emailVerified": True},
            {"username": "operator", "password": "Operator@123456", "email": "operator@aigccloud.com", "role": "operator", "level": 50, "emailVerified": True},
            {"username": "testuser", "password": "Test@123456", "email": "testuser@aigccloud.com", "role": "user", "level": 1, "emailVerified": True},
        ]
        client = await self._get_master_client()
        for user_data in default_users:
            try:
                # 检查用户是否已存在（分别按 username 和 email 查，PostgreSQL 不支持 $or）
                import json as _json
                existing = None
                for field in ("username", "email"):
                    value = user_data.get("username") if field == "username" else user_data.get("email")
                    resp = await client.get(
                        f"{self.base_url}/users",
                        params={"where": _json.dumps({field: value}), "limit": "1"},
                    )
                    results = resp.json().get("results", [])
                    if results:
                        existing = results[0]
                        break
                if existing:
                    # 已存在：校正 username / role / level / password，确保文档化的登录凭证始终有效
                    patch: dict = {}
                    if existing.get("username") != user_data["username"]:
                        patch["username"] = user_data["username"]
                    if existing.get("email") != user_data["email"]:
                        patch["email"] = user_data["email"]
                    if existing.get("role") != user_data["role"]:
                        patch["role"] = user_data["role"]
                    if existing.get("level") != user_data["level"]:
                        patch["level"] = user_data["level"]
                    # 总是重置默认密码，保证与 README 一致
                    patch["password"] = user_data["password"]
                    patch["emailVerified"] = True
                    try:
                        await client.put(
                            f"{self.base_url}/users/{existing['objectId']}",
                            json=patch,
                        )
                        logger.info(f"[Parse] 校正默认用户: {user_data['username']} (role={user_data['role']})")
                    except Exception as ue:
                        logger.error(f"[Parse] 更新默认用户失败 {user_data['username']}: {ue}")
                else:
                    # 创建用户
                    create_resp = await client.post(
                        f"{self.base_url}/users",
                        json=user_data,
                    )
                    if create_resp.status_code in (200, 201):
                        logger.info(f"[Parse] 创建默认用户成功: {user_data['username']} (role={user_data['role']})")
                    else:
                        logger.error(f"[Parse] 创建用户失败: {user_data['username']} - {create_resp.text}")
            except Exception as e:
                logger.error(f"[Parse] 创建默认用户异常({user_data['username']}): {e}")

    async def ensure_default_roles(self):
        """确保内置角色存在：admin / operator / user"""
        default_roles = [
            {
                "name": "admin",
                "label": "管理员",
                "description": "拥有系统全部权限",
                "permissions": ["*"],
            },
            {
                "name": "operator",
                "label": "运营管理员",
                "description": "商品审批、券码促销、充值、报表查看",
                "permissions": [
                    "products.review",
                    "products.manage",
                    "orders.manage",
                    "coupons",
                    "promotions",
                    "recharge",
                    "statistics",
                ],
            },
            {
                "name": "user",
                "label": "普通用户",
                "description": "基础用户功能",
                "permissions": ["user.basic"],
            },
        ]
        client = await self._get_master_client()
        import json as _json
        for role_data in default_roles:
            try:
                resp = await client.get(
                    f"{self.base_url}/classes/_Role",
                    params={"where": _json.dumps({"name": role_data["name"]}), "limit": "1"},
                )
                results = resp.json().get("results", [])
                if results:
                    existing = results[0]
                    # 按需补齐 label/description/permissions（不覆盖管理员已改的权限）
                    patch = {}
                    if not existing.get("label"):
                        patch["label"] = role_data["label"]
                    if not existing.get("description"):
                        patch["description"] = role_data["description"]
                    if not existing.get("permissions"):
                        patch["permissions"] = role_data["permissions"]
                    if patch:
                        await client.put(
                            f"{self.base_url}/classes/_Role/{existing['objectId']}",
                            json=patch,
                        )
                        logger.info(f"[Parse] 补齐内置角色字段: {role_data['name']}")
                    else:
                        logger.debug(f"[Parse] 内置角色已存在: {role_data['name']}")
                else:
                    create_resp = await client.post(
                        f"{self.base_url}/classes/_Role",
                        json={
                            "name": role_data["name"],
                            "label": role_data["label"],
                            "description": role_data["description"],
                            "permissions": role_data["permissions"],
                            "ACL": {"*": {"read": True}},
                        },
                    )
                    if create_resp.status_code in (200, 201):
                        logger.info(f"[Parse] 创建内置角色成功: {role_data['name']}")
                    else:
                        logger.error(f"[Parse] 创建内置角色失败: {role_data['name']} - {create_resp.text}")
            except Exception as e:
                logger.error(f"[Parse] 创建内置角色异常({role_data['name']}): {e}")


# 全局单例
parse_client = ParseClient()
