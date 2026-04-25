"""
K8s API 客户端
支持 kubeconfig 文件或 token 连接，分页查询 Node 和 Pod，避免大集群 apiserver OOM。
使用同步 httpx（cron 脚本无需异步）。
"""
import base64
import atexit
import os
import ssl
import tempfile
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

import httpx

from incentive.config import config
from incentive.logger import logger

# 跟踪临时证书文件，进程退出时清理
_temp_files: List[str] = []


def _cleanup_temp_files():
    for f in _temp_files:
        try:
            if os.path.exists(f):
                os.unlink(f)
        except OSError:
            pass


atexit.register(_cleanup_temp_files)


def _parse_kubeconfig(path: str) -> Dict[str, str]:
    """
    简易 kubeconfig 解析（避免依赖 PyYAML）。
    返回 {server, token, ca_cert, client_cert, client_key}。
    支持 certificate-authority-data / token 内嵌模式。
    """
    result: Dict[str, str] = {}
    try:
        import yaml  # 如果有 PyYAML 则使用
        with open(path, "r", encoding="utf-8") as f:
            kc = yaml.safe_load(f)

        # 取当前 context
        current_ctx_name = kc.get("current-context", "")
        ctx = None
        for c in kc.get("contexts", []):
            if c.get("name") == current_ctx_name:
                ctx = c.get("context", {})
                break
        if not ctx:
            ctx = kc.get("contexts", [{}])[0].get("context", {}) if kc.get("contexts") else {}

        cluster_name = ctx.get("cluster", "")
        user_name = ctx.get("user", "")

        # 解析 cluster
        for cl in kc.get("clusters", []):
            if cl.get("name") == cluster_name:
                cluster_data = cl.get("cluster", {})
                result["server"] = cluster_data.get("server", "")
                # CA 证书
                if cluster_data.get("certificate-authority"):
                    result["ca_cert"] = cluster_data["certificate-authority"]
                elif cluster_data.get("certificate-authority-data"):
                    ca_bytes = base64.b64decode(cluster_data["certificate-authority-data"])
                    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".crt")
                    tmp.write(ca_bytes)
                    tmp.close()
                    result["ca_cert"] = tmp.name
                    _temp_files.append(tmp.name)
                if cluster_data.get("insecure-skip-tls-verify"):
                    result["insecure"] = "true"
                break

        # 解析 user
        for u in kc.get("users", []):
            if u.get("name") == user_name:
                user_data = u.get("user", {})
                if user_data.get("token"):
                    result["token"] = user_data["token"]
                # 客户端证书认证
                if user_data.get("client-certificate"):
                    result["client_cert"] = user_data["client-certificate"]
                elif user_data.get("client-certificate-data"):
                    cert_bytes = base64.b64decode(user_data["client-certificate-data"])
                    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".crt")
                    tmp.write(cert_bytes)
                    tmp.close()
                    result["client_cert"] = tmp.name
                    _temp_files.append(tmp.name)
                if user_data.get("client-key"):
                    result["client_key"] = user_data["client-key"]
                elif user_data.get("client-key-data"):
                    key_bytes = base64.b64decode(user_data["client-key-data"])
                    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".key")
                    tmp.write(key_bytes)
                    tmp.close()
                    result["client_key"] = tmp.name
                    _temp_files.append(tmp.name)
                break

        logger.info(f"[K8s] 从 kubeconfig 加载: server={result.get('server', '')}, "
                    f"auth={'token' if result.get('token') else 'client-cert'}")
    except ImportError:
        # 无 PyYAML，简易解析
        logger.warning("[K8s] 未安装 PyYAML，尝试简易解析 kubeconfig")
        result = _parse_kubeconfig_simple(path)
    except Exception as e:
        logger.error(f"[K8s] kubeconfig 解析失败: {e}")

    return result


def _parse_kubeconfig_simple(path: str) -> Dict[str, str]:
    """无 PyYAML 时的简易行解析（支持 token / client-certificate-data 认证）"""
    result: Dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        for line in content.splitlines():
            stripped = line.strip()
            if stripped.startswith("server:"):
                result["server"] = stripped.split("server:", 1)[1].strip().strip('"').strip("'")
            elif stripped.startswith("token:"):
                result["token"] = stripped.split("token:", 1)[1].strip().strip('"').strip("'")
            elif stripped.startswith("certificate-authority:"):
                result["ca_cert"] = stripped.split("certificate-authority:", 1)[1].strip().strip('"').strip("'")
            elif stripped.startswith("certificate-authority-data:"):
                ca_b64 = stripped.split("certificate-authority-data:", 1)[1].strip()
                if ca_b64:
                    ca_bytes = base64.b64decode(ca_b64)
                    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".crt")
                    tmp.write(ca_bytes)
                    tmp.close()
                    result["ca_cert"] = tmp.name
                    _temp_files.append(tmp.name)
            elif stripped.startswith("client-certificate-data:"):
                cert_b64 = stripped.split("client-certificate-data:", 1)[1].strip()
                if cert_b64:
                    cert_bytes = base64.b64decode(cert_b64)
                    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".crt")
                    tmp.write(cert_bytes)
                    tmp.close()
                    result["client_cert"] = tmp.name
                    _temp_files.append(tmp.name)
            elif stripped.startswith("client-key-data:"):
                key_b64 = stripped.split("client-key-data:", 1)[1].strip()
                if key_b64:
                    key_bytes = base64.b64decode(key_b64)
                    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".key")
                    tmp.write(key_bytes)
                    tmp.close()
                    result["client_key"] = tmp.name
                    _temp_files.append(tmp.name)
            elif stripped.startswith("client-certificate:"):
                result["client_cert"] = stripped.split("client-certificate:", 1)[1].strip().strip('"').strip("'")
            elif stripped.startswith("client-key:"):
                result["client_key"] = stripped.split("client-key:", 1)[1].strip().strip('"').strip("'")

        auth = "token" if result.get("token") else ("client-cert" if result.get("client_cert") else "none")
        logger.info(f"[K8s] 简易解析 kubeconfig: server={result.get('server', '')}, auth={auth}")
    except Exception as e:
        logger.error(f"[K8s] 简易解析 kubeconfig 失败: {e}")
    return result


class K8sClient:
    """K8s REST API 客户端（支持 kubeconfig / token 认证）"""

    def __init__(self):
        self._kc_info: Dict[str, str] = {}

        # 优先从 kubeconfig 文件加载
        kubeconfig_path = config.k8s_kubeconfig
        if not kubeconfig_path:
            # 尝试默认路径 ~/.kube/config
            default_path = os.path.expanduser("~/.kube/config")
            if os.path.isfile(default_path):
                kubeconfig_path = default_path
                logger.info(f"[K8s] 使用默认 kubeconfig: {default_path}")

        if kubeconfig_path and os.path.isfile(kubeconfig_path):
            self._kc_info = _parse_kubeconfig(kubeconfig_path)

        # kubeconfig 优先，环境变量回退
        self.api_server = (self._kc_info.get("server") or config.k8s_api_server).rstrip("/")
        self.token = self._kc_info.get("token") or config.k8s_token
        self._ca_cert = self._kc_info.get("ca_cert") or config.k8s_ca_cert
        self._client_cert = self._kc_info.get("client_cert", "")
        self._client_key = self._kc_info.get("client_key", "")
        self._insecure = self._kc_info.get("insecure") == "true"
        self.page_size = config.k8s_page_size

        auth_method = "kubeconfig" if self._kc_info else "token/env"
        logger.info(f"[K8s] 连接: {self.api_server} (认证: {auth_method})")
        if self._client_cert:
            cert_exists = os.path.isfile(self._client_cert)
            cert_size = os.path.getsize(self._client_cert) if cert_exists else 0
            key_exists = os.path.isfile(self._client_key) if self._client_key else False
            key_size = os.path.getsize(self._client_key) if key_exists else 0
            logger.info(f"[K8s] 客户端证书: cert={self._client_cert} (exists={cert_exists}, size={cert_size}), "
                        f"key={self._client_key} (exists={key_exists}, size={key_size})")
            logger.info(f"[K8s] CA证书: {self._ca_cert}, insecure={self._insecure}")
        if self.token:
            logger.info(f"[K8s] Token: {self.token[:20]}...")

    def _get_verify(self):
        """SSL 验证参数"""
        if self._insecure:
            return False
        if self._ca_cert:
            return self._ca_cert
        return False  # 开发环境跳过证书验证

    def _headers(self) -> Dict[str, str]:
        h = {"Accept": "application/json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        return h

    def _build_ssl_context(self):
        """构建 SSL 上下文（同时加载 CA 证书和客户端证书）"""
        ctx = ssl.create_default_context()

        if self._insecure:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        elif self._ca_cert and os.path.isfile(self._ca_cert):
            ctx.load_verify_locations(self._ca_cert)
        else:
            # 无 CA 证书时跳过服务端验证
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

        if self._client_cert and self._client_key:
            ctx.load_cert_chain(certfile=self._client_cert, keyfile=self._client_key)

        return ctx

    def _client(self) -> httpx.Client:
        kwargs: Dict[str, Any] = {
            "verify": self._build_ssl_context(),
            "timeout": 30.0,
            "headers": self._headers(),
        }
        return httpx.Client(**kwargs)

    # ========== 分页列表 ==========

    def list_nodes(self) -> List[Dict[str, Any]]:
        """分页获取所有 Node"""
        all_nodes: List[Dict[str, Any]] = []
        continue_token: Optional[str] = None

        with self._client() as client:
            while True:
                params: Dict[str, Any] = {"limit": self.page_size}
                if continue_token:
                    params["continue"] = continue_token

                url = f"{self.api_server}/api/v1/nodes"
                try:
                    resp = client.get(url, params=params)
                    if resp.status_code == 403:
                        logger.error(f"[K8s] list_nodes 403 详情: {resp.text[:500]}")
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as e:
                    logger.error(f"[K8s] list_nodes 失败: {e}")
                    break

                items = data.get("items", [])
                all_nodes.extend(items)
                logger.info(f"[K8s] 获取节点: 本页 {len(items)}, 累计 {len(all_nodes)}")

                continue_token = data.get("metadata", {}).get("continue")
                if not continue_token:
                    break

        return all_nodes

    def list_pods(self, field_selector: str = "", namespace: str = "") -> List[Dict[str, Any]]:
        """分页获取 Pod（可按命名空间和字段过滤）"""
        all_pods: List[Dict[str, Any]] = []
        continue_token: Optional[str] = None

        ns_path = f"/api/v1/namespaces/{namespace}/pods" if namespace else "/api/v1/pods"
        url = f"{self.api_server}{ns_path}"

        with self._client() as client:
            while True:
                params: Dict[str, Any] = {"limit": self.page_size}
                if continue_token:
                    params["continue"] = continue_token
                if field_selector:
                    params["fieldSelector"] = field_selector

                try:
                    resp = client.get(url, params=params)
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as e:
                    logger.error(f"[K8s] list_pods 失败: {e}")
                    break

                items = data.get("items", [])
                all_pods.extend(items)

                continue_token = data.get("metadata", {}).get("continue")
                if not continue_token:
                    break

        logger.info(f"[K8s] 获取 Pod: 总计 {len(all_pods)}")
        return all_pods

    # ========== 节点信息解析 ==========

    @staticmethod
    def parse_node_name(node: Dict[str, Any]) -> str:
        """获取节点名称（通常为 ETH地址+注册时间 格式）"""
        return node.get("metadata", {}).get("name", "")

    @staticmethod
    def extract_eth_address(node_name: str) -> str:
        """
        从节点名中提取 ETH 地址。
        节点名格式: 0x1234abcd...ef-1700000000
        """
        if not node_name:
            return ""
        # 取第一个 '-' 之前的部分（如果有），否则取整个名称
        parts = node_name.split("-")
        addr = parts[0]
        if addr.startswith("0x") and len(addr) >= 42:
            return addr[:42].lower()
        return addr.lower()

    @staticmethod
    def is_node_ready(node: Dict[str, Any]) -> bool:
        """判断节点是否 Ready"""
        conditions = node.get("status", {}).get("conditions", [])
        for cond in conditions:
            if cond.get("type") == "Ready":
                return cond.get("status") == "True"
        return False

    @staticmethod
    def is_edge_node(node: Dict[str, Any]) -> bool:
        """判断是否为边缘节点（roles 含 edge 或有 node-role.kubernetes.io/edge 标签）"""
        labels = node.get("metadata", {}).get("labels", {})
        for key in labels:
            if key.startswith("node-role.kubernetes.io/"):
                role = key.split("/")[-1]
                if role == "edge":
                    return True
        return False

    @staticmethod
    def get_node_gpu_info(node: Dict[str, Any]) -> str:
        """从节点标签/注解中获取 GPU 型号信息"""
        labels = node.get("metadata", {}).get("labels", {})
        annotations = node.get("metadata", {}).get("annotations", {})

        # 优先从标签获取
        for key in ["nvidia.com/gpu.product", "gpu-type", "gpu-model", "gpu-info"]:
            val = labels.get(key) or annotations.get(key, "")
            if val:
                return val

        # 检查 capacity 中是否有 GPU
        capacity = node.get("status", {}).get("capacity", {})
        gpu_count = int(float(capacity.get("nvidia.com/gpu", "0")))
        if gpu_count == 0:
            return "CPU"

        return "Unknown GPU"

    @staticmethod
    def get_node_gpu_count(node: Dict[str, Any]) -> int:
        """获取节点 GPU 数量"""
        capacity = node.get("status", {}).get("capacity", {})
        return int(float(capacity.get("nvidia.com/gpu", "0")))

    @staticmethod
    def get_last_heartbeat(node: Dict[str, Any]) -> Optional[datetime]:
        """获取最后心跳时间"""
        conditions = node.get("status", {}).get("conditions", [])
        for cond in conditions:
            if cond.get("type") == "Ready":
                ts = cond.get("lastHeartbeatTime", "")
                if ts:
                    try:
                        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    except ValueError:
                        pass
        return None

    @staticmethod
    def get_node_creation_time(node: Dict[str, Any]) -> Optional[datetime]:
        """获取节点创建时间"""
        ts = node.get("metadata", {}).get("creationTimestamp", "")
        if ts:
            try:
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                pass
        return None

    @staticmethod
    def count_running_pods_on_node(pods: List[Dict[str, Any]], node_name: str) -> int:
        """统计节点上运行中的 Pod 数量"""
        count = 0
        for pod in pods:
            spec_node = pod.get("spec", {}).get("nodeName", "")
            phase = pod.get("status", {}).get("phase", "")
            if spec_node == node_name and phase == "Running":
                count += 1
        return count
