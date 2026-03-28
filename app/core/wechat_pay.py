"""
微信支付封装模块
"""
import hashlib
import hmac
import time
import uuid
import httpx
from typing import Optional
from xml.etree import ElementTree

from app.core.config import settings
from app.core.logger import logger


# 会员套餐配置
# VIP定价: 1月9.9元, 半年9折, 一年85折, 3年8折, 5年75折
# SVIP: VIP价格的2倍
MEMBER_PLANS = {
    # VIP 套餐
    "vip_day": {"level": "vip", "days": 1, "price": 0.20, "original_price": 0.20, "discount": 100, "bonus": 5, "name": "VIP体验1天"},
    "vip_month": {"level": "vip", "days": 30, "price": 9.90, "original_price": 9.90, "discount": 100, "bonus": 50, "name": "VIP月度会员"},
    "vip_half": {"level": "vip", "days": 180, "price": 53.50, "original_price": 59.40, "discount": 90, "bonus": 300, "name": "VIP半年会员"},
    "vip_year": {"level": "vip", "days": 365, "price": 101.00, "original_price": 118.80, "discount": 85, "bonus": 800, "name": "VIP年度会员"},
    "vip_3year": {"level": "vip", "days": 1095, "price": 285.00, "original_price": 356.40, "discount": 80, "bonus": 3000, "name": "VIP三年会员"},
    "vip_5year": {"level": "vip", "days": 1825, "price": 445.50, "original_price": 594.00, "discount": 75, "bonus": 6000, "name": "VIP五年会员"},
    # SVIP 套餐 (VIP价格的2倍)
    "svip_day": {"level": "svip", "days": 1, "price": 0.30, "original_price": 0.30, "discount": 100, "bonus": 10, "name": "SVIP体验1天"},
    "svip_month": {"level": "svip", "days": 30, "price": 19.80, "original_price": 19.80, "discount": 100, "bonus": 100, "name": "SVIP月度会员"},
    "svip_half": {"level": "svip", "days": 180, "price": 107.00, "original_price": 118.80, "discount": 90, "bonus": 600, "name": "SVIP半年会员"},
    "svip_year": {"level": "svip", "days": 365, "price": 202.00, "original_price": 237.60, "discount": 85, "bonus": 1600, "name": "SVIP年度会员"},
    "svip_3year": {"level": "svip", "days": 1095, "price": 570.00, "original_price": 712.80, "discount": 80, "bonus": 6000, "name": "SVIP三年会员"},
    "svip_5year": {"level": "svip", "days": 1825, "price": 891.00, "original_price": 1188.00, "discount": 75, "bonus": 12000, "name": "SVIP五年会员"},
}


def generate_nonce_str(length: int = 32) -> str:
    """生成随机字符串"""
    return uuid.uuid4().hex[:length]


def generate_sign(params: dict, api_key: str) -> str:
    """生成微信支付签名"""
    # 按字典序排序
    sorted_params = sorted(params.items(), key=lambda x: x[0])
    # 拼接字符串
    string_a = "&".join([f"{k}={v}" for k, v in sorted_params if v])
    # 拼接API密钥
    string_sign_temp = f"{string_a}&key={api_key}"
    # MD5加密并转大写
    sign = hashlib.md5(string_sign_temp.encode("utf-8")).hexdigest().upper()
    return sign


def dict_to_xml(data: dict) -> str:
    """字典转XML"""
    xml_parts = ["<xml>"]
    for key, value in data.items():
        if value is not None:
            xml_parts.append(f"<{key}><![CDATA[{value}]]></{key}>")
    xml_parts.append("</xml>")
    return "".join(xml_parts)


def xml_to_dict(xml_str: str) -> dict:
    """XML转字典"""
    root = ElementTree.fromstring(xml_str)
    return {child.tag: child.text for child in root}


class WechatPay:
    """微信支付客户端"""
    
    UNIFIED_ORDER_URL = "https://api.mch.weixin.qq.com/pay/unifiedorder"
    ORDER_QUERY_URL = "https://api.mch.weixin.qq.com/pay/orderquery"
    
    def __init__(self):
        self.app_id = settings.wechat_app_id
        self.mch_id = settings.wechat_mch_id
        self.api_key = settings.wechat_api_key
        self.notify_url = settings.wechat_notify_url
        self.test_mode = settings.wechat_test_mode
        # 启动时记录配置，便于调试
        logger.info(f"[微信支付] 初始化: test_mode={self.test_mode}, mch_id={self.mch_id[:4]}***")
    
    async def create_order(
        self,
        out_trade_no: str,
        total_fee: int,  # 单位：分
        body: str,
        openid: str,
        trade_type: str = "JSAPI",
        attach: Optional[str] = None,
    ) -> dict:
        """
        创建微信支付订单
        
        Args:
            out_trade_no: 商户订单号
            total_fee: 金额（分）
            body: 商品描述
            openid: 用户openid（JSAPI必填）
            trade_type: 交易类型 JSAPI/NATIVE/APP
            attach: 附加数据
        
        Returns:
            prepay_id 等支付参数
        """
        # 调试日志
        logger.info(f"[DEBUG] test_mode={self.test_mode}, type={type(self.test_mode)}")
        logger.info(f"[DEBUG] app_id={self.app_id[:10]}..., mch_id={self.mch_id}")
        
        # 测试模式：返回模拟数据
        if self.test_mode:
            logger.info(f"[微信支付-测试模式] 创建订单: {out_trade_no}, 金额: {total_fee}分")
            return {
                "success": True,
                "prepay_id": f"test_prepay_{out_trade_no}",
                "code_url": f"weixin://test/pay/{out_trade_no}",
                "out_trade_no": out_trade_no,
                "test_mode": True,
            }
        
        params = {
            "appid": self.app_id,
            "mch_id": self.mch_id,
            "nonce_str": generate_nonce_str(),
            "body": body,
            "out_trade_no": out_trade_no,
            "total_fee": str(total_fee),
            "spbill_create_ip": "127.0.0.1",
            "notify_url": self.notify_url,
            "trade_type": trade_type,
        }
        
        if openid and trade_type == "JSAPI":
            params["openid"] = openid
        
        if attach:
            params["attach"] = attach
        
        # 生成签名
        params["sign"] = generate_sign(params, self.api_key)
        
        # 发起请求
        xml_data = dict_to_xml(params)
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.UNIFIED_ORDER_URL,
                content=xml_data,
                headers={"Content-Type": "application/xml"},
            )
            result = xml_to_dict(response.text)
        
        if result.get("return_code") == "SUCCESS" and result.get("result_code") == "SUCCESS":
            return {
                "success": True,
                "prepay_id": result.get("prepay_id"),
                "code_url": result.get("code_url"),  # NATIVE支付二维码
                "out_trade_no": out_trade_no,
            }
        else:
            logger.error(f"[微信支付] 创建订单失败: {result}")
            return {
                "success": False,
                "error": result.get("err_code_des") or result.get("return_msg"),
            }
    
    async def query_order(self, out_trade_no: str) -> dict:
        """
        查询订单状态
        
        Args:
            out_trade_no: 商户订单号
        
        Returns:
            订单状态信息
        """
        # 测试模式
        if self.test_mode:
            logger.info(f"[微信支付-测试模式] 查询订单: {out_trade_no}")
            return {
                "success": True,
                "trade_state": "SUCCESS",
                "out_trade_no": out_trade_no,
                "test_mode": True,
            }
        
        params = {
            "appid": self.app_id,
            "mch_id": self.mch_id,
            "out_trade_no": out_trade_no,
            "nonce_str": generate_nonce_str(),
        }
        params["sign"] = generate_sign(params, self.api_key)
        
        xml_data = dict_to_xml(params)
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.ORDER_QUERY_URL,
                content=xml_data,
                headers={"Content-Type": "application/xml"},
            )
            result = xml_to_dict(response.text)
        
        if result.get("return_code") == "SUCCESS":
            return {
                "success": True,
                "trade_state": result.get("trade_state"),
                "out_trade_no": out_trade_no,
                "transaction_id": result.get("transaction_id"),
            }
        else:
            return {
                "success": False,
                "error": result.get("return_msg"),
            }
    
    def verify_callback(self, xml_data: str) -> dict:
        """
        验证回调签名
        
        Args:
            xml_data: 回调XML数据
        
        Returns:
            验证结果和解析后的数据
        """
        data = xml_to_dict(xml_data)
        
        # 测试模式
        if self.test_mode:
            return {"success": True, "data": data}
        
        # 验证签名
        sign = data.pop("sign", None)
        expected_sign = generate_sign(data, self.api_key)
        
        if sign != expected_sign:
            logger.error(f"[微信支付] 回调签名验证失败")
            return {"success": False, "error": "签名验证失败"}
        
        return {"success": True, "data": data}
    
    def generate_jsapi_params(self, prepay_id: str) -> dict:
        """
        生成JSAPI调起支付的参数
        
        Args:
            prepay_id: 预支付交易会话标识
        
        Returns:
            前端调起支付需要的参数
        """
        params = {
            "appId": self.app_id,
            "timeStamp": str(int(time.time())),
            "nonceStr": generate_nonce_str(),
            "package": f"prepay_id={prepay_id}",
            "signType": "MD5",
        }
        params["paySign"] = generate_sign(params, self.api_key)
        return params


# 单例
wechat_pay = WechatPay()
