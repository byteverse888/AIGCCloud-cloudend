"""
Web3 联盟链客户端（同步版）
主币直转，支持批量转账、余额查询，用于积分清算时从运营账号转账到节点账户。
"""
import uuid
from typing import Any, Dict, List, Optional, Tuple

import httpx
from eth_account import Account
from eth_utils import to_checksum_address

from incentive.config import config
from incentive.logger import logger


def _derive_address(private_key: str) -> str:
    """从私钥派生地址"""
    if not private_key:
        return ""
    try:
        acct = Account.from_key(private_key)
        return acct.address
    except Exception as e:
        logger.error(f"[Web3] 私钥派生地址失败: {e}")
        return ""


class Web3Client:
    """Web3 JSON-RPC 同步客户端"""

    def __init__(self):
        self.rpc_url = config.web3_rpc_url
        self.chain_id = config.web3_chain_id
        self.operator_private_key = config.incentive_wallet_private_key
        self.operator_address = _derive_address(self.operator_private_key)
        self._nonce: Optional[int] = None  # 当前交易 nonce 缓存

    @property
    def is_available(self) -> bool:
        return bool(self.rpc_url)

    def _call_rpc(self, method: str, params: list) -> dict:
        if not self.rpc_url:
            return {"result": "0x0"}
        with httpx.Client(timeout=30.0) as client:
            resp = client.post(
                self.rpc_url,
                json={"jsonrpc": "2.0", "method": method, "params": params, "id": 1},
            )
            return resp.json()

    def _get_nonce(self) -> int:
        """获取当前 nonce，批量转账时自动递增"""
        if self._nonce is not None:
            self._nonce += 1
            return self._nonce
        result = self._call_rpc("eth_getTransactionCount", [self.operator_address, "pending"])
        self._nonce = int(result.get("result", "0x0"), 16)
        return self._nonce

    def _get_gas_price(self) -> int:
        """获取当前 gas price"""
        result = self._call_rpc("eth_gasPrice", [])
        return int(result.get("result", "0x3B9ACA00"), 16)  # 默认 1 Gwei

    def get_balance(self, address: str) -> int:
        """获取用户主币余额（从链上）"""
        if not address:
            return 0
        if not self.rpc_url:
            return 0
        try:
            result = self._call_rpc("eth_getBalance", [address, "latest"])
            return int(result.get("result", "0x0"), 16)
        except Exception as e:
            logger.error(f"[Web3] 获取余额失败 {address}: {e}")
            return 0

    def transfer(self, to_address: str, amount: int) -> Dict[str, Any]:
        """
        从运营账号转主币到指定地址。
        使用本地私钥签名 + eth_sendRawTransaction。
        返回 {"success": bool, "tx_hash": str, "error": str}
        """
        if not self.rpc_url:
            mock_hash = f"mock_tx_{uuid.uuid4().hex[:12]}"
            logger.info(f"[Web3] 模拟转账: -> {to_address}, amount={amount}, tx={mock_hash}")
            return {"success": True, "tx_hash": mock_hash}

        try:
            nonce = self._get_nonce()
            gas_price = self._get_gas_price()

            # 构建交易对象
            tx = {
                "to": to_checksum_address(to_address),
                "value": amount,
                "nonce": nonce,
                "gasPrice": gas_price,
                "gas": 21000,  # 主币转账标准 gas limit
                "chainId": self.chain_id,
            }

            # 本地签名
            signed = Account.sign_transaction(tx, self.operator_private_key)
            raw_tx = signed.raw_transaction.hex()
            if not raw_tx.startswith("0x"):
                raw_tx = "0x" + raw_tx

            # 发送签名交易
            result = self._call_rpc("eth_sendRawTransaction", [raw_tx])
            tx_hash = result.get("result")
            error_obj = result.get("error")

            if tx_hash and not error_obj:
                logger.info(f"[Web3] 转账成功: -> {to_address}, amount={amount}, tx={tx_hash}")
                return {"success": True, "tx_hash": tx_hash}

            error = error_obj.get("message", "未知错误") if error_obj else "无 tx_hash"
            logger.error(f"[Web3] 转账失败: -> {to_address}, amount={amount}, error={error}")
            return {"success": False, "tx_hash": "", "error": error}
        except Exception as e:
            logger.error(f"[Web3] 转账异常: -> {to_address}, amount={amount}, {e}")
            return {"success": False, "tx_hash": "", "error": str(e)}

    def batch_transfer(self, transfers: List[Tuple[str, int]]) -> List[Dict[str, Any]]:
        """
        批量转账。
        transfers: [(to_address, amount), ...]
        返回每笔转账的结果列表。
        """
        self._nonce = None  # 重置 nonce 缓存，每批重新获取
        results = []
        for to_addr, amount in transfers:
            result = self.transfer(to_addr, amount)
            results.append({
                "address": to_addr,
                "amount": amount,
                **result,
            })
        return results



# 全局单例
web3_client = Web3Client()
