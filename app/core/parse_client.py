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
    
    async def _request(
        self, 
        method: str, 
        endpoint: str, 
        data: Optional[Dict] = None,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """发送请求到 Parse Server"""
        url = f"{self.base_url}{endpoint}"
        
        # 调试日志：请求信息
        logger.debug(f"[Parse] 请求: {method} {url}")
        logger.debug(f"[Parse] Headers: App-Id={self.app_id[:8]}..., REST-Key={self.rest_api_key[:8] if self.rest_api_key else 'N/A'}...")
        if data:
            # 隐藏敏感字段
            safe_data = {k: ('***' if k in ['password'] else v) for k, v in data.items()}
            logger.debug(f"[Parse] Body: {json_lib.dumps(safe_data, ensure_ascii=False)}")
        if params:
            logger.debug(f"[Parse] Params: {params}")
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.request(
                    method=method,
                    url=url,
                    headers=self.headers,
                    json=data,
                    params=params,
                    timeout=30.0
                )
                
                # 调试日志：响应信息
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
        url = f"{self.base_url}/users/{user_id}"
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    url,
                    headers=self.master_headers,
                    timeout=30.0
                )
                if response.status_code >= 400:
                    logger.error(f"[Parse] 获取用户失败: {response.text}")
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.error(f"[Parse] 获取用户异常: {str(e)}")
                raise
    
    async def get_current_user(self, session_token: str) -> Dict[str, Any]:
        """通过 session token 获取当前用户信息"""
        headers = {
            **self.headers,
            "X-Parse-Session-Token": session_token,
        }
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/users/me",
                headers=headers,
                timeout=30.0
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
    
    async def update_user_with_master_key(self, user_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """使用 Master Key 更新用户信息（用于 emailVerified 等敏感字段）"""
        url = f"{self.base_url}/users/{user_id}"
        logger.info(f"[Parse] 更新用户(Master): {user_id}, 数据: {data}")
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.put(
                    url,
                    headers=self.master_headers,
                    json=data,
                    timeout=30.0
                )
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
        headers = {
            **self.headers,
            "X-Parse-Session-Token": session_token,
        }
        url = f"{self.base_url}/users/{user_id}"
        logger.info(f"[Parse] 更新用户(session): {user_id}, 数据: {data}")
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.put(
                    url,
                    headers=headers,
                    json=data,
                    timeout=30.0
                )
                logger.info(f"[Parse] 更新用户响应: {response.status_code}")
                if response.status_code >= 400:
                    logger.error(f"[Parse] 更新用户失败: {response.text}")
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.error(f"[Parse] 更新用户异常: {e}")
                raise
    
    async def query_users(
        self, 
        where: Optional[Dict] = None,
        order: Optional[str] = None,
        limit: int = 100,
        skip: int = 0
    ) -> Dict[str, Any]:
        """查询用户列表
        
        使用 Master Key 查询 /classes/_User
        """
        import json
        params = {"limit": limit, "skip": skip}
        if where:
            params["where"] = json.dumps(where)
        if order:
            params["order"] = order
        
        # 使用 Master Key 查询
        url = f"{self.base_url}/classes/_User"
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    url,
                    headers=self.master_headers,
                    params=params,
                    timeout=30.0
                )
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
            "owner": {"type": "String"},
            "status": {"type": "String"},
            "category": {"type": "String"},
            "creatorId": {"type": "String"},
            "reportCount": {"type": "Number"},
            "reviewedAt": {"type": "String"},
            "reviewedBy": {"type": "String"},
            "reviewNote": {"type": "String"},
            "offlineReason": {"type": "String"}
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
            "status": {"type": "String"},
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
            "relatedId": {"type": "String"}
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
        }
    }
    
    async def ensure_schema(self):
        """确保所有业务类在 Parse Server 中存在并包含正确类型的字段（Schema API + Master Key）"""
        async with httpx.AsyncClient() as client:
            for class_name, fields in self.SCHEMA_DEFINITIONS.items():
                try:
                    # 用 Schema API 检查类是否存在
                    resp = await client.get(
                        f"{self.base_url}/schemas/{class_name}",
                        headers=self.master_headers,
                        timeout=10.0,
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
                                headers=self.master_headers,
                                json={"className": class_name, "fields": delete_fields},
                                timeout=10.0,
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
                                headers=self.master_headers,
                                json={"className": class_name, "fields": missing},
                                timeout=10.0,
                            )
                            if add_resp.status_code == 200:
                                logger.info(f"[Parse] 字段补充成功: {class_name}")
                            else:
                                logger.error(f"[Parse] 字段补充失败: {class_name} - {add_resp.text}")
                        else:
                            logger.debug(f"[Parse] Schema OK: {class_name}")
                        continue
                    
                    # 类不存在，创建并带上字段定义
                    logger.info(f"[Parse] 创建 Schema: {class_name}")
                    create_resp = await client.post(
                        f"{self.base_url}/schemas/{class_name}",
                        headers=self.master_headers,
                        json={"className": class_name, "fields": fields},
                        timeout=10.0,
                    )
                    if create_resp.status_code in (200, 201):
                        logger.info(f"[Parse] Schema 创建成功: {class_name}")
                    else:
                        logger.error(f"[Parse] Schema 创建失败: {class_name} - {create_resp.text}")
                except Exception as e:
                    logger.error(f"[Parse] ensure_schema 异常({class_name}): {e}")
    
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


# 全局单例
parse_client = ParseClient()
