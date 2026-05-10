"""
图片验证码服务
"""
import base64
import io
import random
import string
import uuid
from PIL import Image, ImageDraw, ImageFont
from typing import Tuple

from app.core.redis_client import redis_client
from app.core.logger import logger


# 验证码配置
CAPTCHA_LENGTH = 4  # 验证码长度
CAPTCHA_EXPIRE = 300  # 验证码过期时间(秒)
CAPTCHA_WIDTH = 120  # 图片宽度
CAPTCHA_HEIGHT = 40  # 图片高度


def generate_captcha_text() -> str:
    """生成4位小写字母验证码"""
    return ''.join(random.choices(string.ascii_lowercase, k=CAPTCHA_LENGTH))


def generate_captcha_image(text: str) -> bytes:
    """
    生成验证码图片
    
    Args:
        text: 验证码文本
        
    Returns:
        PNG 图片字节数据
    """
    # 创建图片
    image = Image.new('RGB', (CAPTCHA_WIDTH, CAPTCHA_HEIGHT), color='white')
    draw = ImageDraw.Draw(image)
    
    # 尝试使用系统字体，否则使用默认字体
    try:
        font = ImageFont.truetype("arial.ttf", 28)
    except:
        try:
            font = ImageFont.truetype("DejaVuSans.ttf", 28)
        except:
            font = ImageFont.load_default()
    
    # 添加干扰线
    for _ in range(5):
        x1 = random.randint(0, CAPTCHA_WIDTH)
        y1 = random.randint(0, CAPTCHA_HEIGHT)
        x2 = random.randint(0, CAPTCHA_WIDTH)
        y2 = random.randint(0, CAPTCHA_HEIGHT)
        color = (random.randint(100, 200), random.randint(100, 200), random.randint(100, 200))
        draw.line([(x1, y1), (x2, y2)], fill=color, width=1)
    
    # 添加干扰点
    for _ in range(50):
        x = random.randint(0, CAPTCHA_WIDTH)
        y = random.randint(0, CAPTCHA_HEIGHT)
        color = (random.randint(100, 200), random.randint(100, 200), random.randint(100, 200))
        draw.point((x, y), fill=color)
    
    # 绘制文字
    x_offset = 10
    for i, char in enumerate(text):
        # 随机颜色
        color = (random.randint(0, 100), random.randint(0, 100), random.randint(0, 100))
        # 随机偏移
        y_offset = random.randint(2, 8)
        draw.text((x_offset + i * 25, y_offset), char, font=font, fill=color)
    
    # 转换为字节
    buffer = io.BytesIO()
    image.save(buffer, format='PNG')
    return buffer.getvalue()


class CaptchaService:
    """验证码服务"""
    
    @staticmethod
    async def generate() -> Tuple[str, str]:
        """生成验证码
        
        Returns:
            (captcha_id, image_base64)
        """
        # 生成验证码ID和文本
        captcha_id = str(uuid.uuid4())
        captcha_text = generate_captcha_text()
        
        # 生成图片
        image_bytes = generate_captcha_image(captcha_text)
        
        # 转换为 base64
        image_base64 = base64.b64encode(image_bytes).decode()
        image_data = f"data:image/png;base64,{image_base64}"
        
        # 存储验证码文本到 Redis
        key = f"captcha:{captcha_id}"
        await redis_client.set(key, captcha_text, ex=CAPTCHA_EXPIRE)
        
        logger.info(f"[Captcha] 生成验证码: id={captcha_id}, text={captcha_text}")
        
        return captcha_id, image_data
    
    @staticmethod
    async def verify(captcha_id: str, captcha_text: str) -> bool:
        """
        验证验证码
        
        Args:
            captcha_id: 验证码ID
            captcha_text: 用户输入的验证码
            
        Returns:
            是否验证成功
        """
        if not captcha_id or not captcha_text:
            return False
        
        key = f"captcha:{captcha_id}"
        stored_text = await redis_client.get(key)
        
        if not stored_text:
            logger.warning(f"[Captcha] 验证码不存在或已过期: id={captcha_id}")
            return False
        
        # 验证（不区分大小写）
        is_valid = stored_text.lower() == captcha_text.lower()
        
        # 验证后删除（一次性使用）
        await redis_client.delete(key)
        
        logger.info(f"[Captcha] 验证结果: id={captcha_id}, valid={is_valid}")
        
        return is_valid


# 单例
captcha_service = CaptchaService()
