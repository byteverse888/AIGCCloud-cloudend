"""
图片验证码服务单元测试
"""
import base64
import pytest
import pytest_asyncio
import unittest.mock
import uuid

from app.core.captcha import (
    CaptchaService,
    generate_captcha_text,
    generate_captcha_image,
    CAPTCHA_LENGTH,
)


class TestGenerateCaptchaText:
    """测试生成验证码文本"""

    def test_length_is_4(self):
        """验证码长度为4"""
        code = generate_captcha_text()
        assert len(code) == CAPTCHA_LENGTH
        assert len(code) == 4

    def test_is_lowercase(self):
        """只包含小写字母"""
        code = generate_captcha_text()
        assert code.islower()
        assert code.isalpha()


class TestGenerateCaptchaImage:
    """测试生成验证码图片"""

    def test_returns_bytes(self):
        """返回bytes类型"""
        text = "abcd"
        image_bytes = generate_captcha_image(text)
        assert isinstance(image_bytes, bytes)

    def test_is_png_format(self):
        """PNG格式头"""
        text = "abcd"
        image_bytes = generate_captcha_image(text)
        assert image_bytes[:8] == b'\x89PNG\r\n\x1a\n'


class TestCaptchaServiceGenerate:
    """测试CaptchaService.generate新接口"""

    @pytest.mark.asyncio
    async def test_generate_returns_id_and_image(self):
        """返回UUID id和base64图片"""
        with unittest.mock.patch('app.core.captcha.redis_client') as mock_redis:
            mock_redis.set = unittest.mock.AsyncMock(return_value=True)
            
            captcha_id, image_base64 = await CaptchaService.generate()
            
            assert uuid.UUID(captcha_id)
            assert image_base64.startswith("data:image/png;base64,")
            assert len(image_base64) > len("data:image/png;base64,")

    @pytest.mark.asyncio
    async def test_code_stored_in_redis(self):
        """code存入Redis"""
        with unittest.mock.patch('app.core.captcha.redis_client') as mock_redis:
            mock_redis.set = unittest.mock.AsyncMock(return_value=True)
            
            captcha_id, _ = await CaptchaService.generate()
            
            mock_redis.set.assert_called()
            call_args = mock_redis.set.call_args
            key = call_args[0][0]
            assert key == f"captcha:{captcha_id}"


class TestCaptchaServiceVerify:
    """测试CaptchaService.verify"""

    @pytest.mark.asyncio
    async def test_verify_success(self):
        """验证成功返回True"""
        test_id = str(uuid.uuid4())
        test_code = "abcd"
        
        with unittest.mock.patch('app.core.captcha.redis_client') as mock_redis:
            mock_redis.get = unittest.mock.AsyncMock(return_value=test_code)
            mock_redis.delete = unittest.mock.AsyncMock(return_value=1)
            
            result = await CaptchaService.verify(test_id, test_code)
            
            assert result is True

    @pytest.mark.asyncio
    async def test_verify_case_insensitive(self):
        """不区分大小写"""
        test_id = str(uuid.uuid4())
        test_code = "AbCd"
        
        with unittest.mock.patch('app.core.captcha.redis_client') as mock_redis:
            mock_redis.get = unittest.mock.AsyncMock(return_value="abcd")
            mock_redis.delete = unittest.mock.AsyncMock(return_value=1)
            
            result = await CaptchaService.verify(test_id, test_code.upper())
            
            assert result is True

    @pytest.mark.asyncio
    async def test_verify_wrong_code(self):
        """错误验证码返回False"""
        test_id = str(uuid.uuid4())
        
        with unittest.mock.patch('app.core.captcha.redis_client') as mock_redis:
            mock_redis.get = unittest.mock.AsyncMock(return_value="abcd")
            mock_redis.delete = unittest.mock.AsyncMock(return_value=1)
            
            result = await CaptchaService.verify(test_id, "wrong")
            
            assert result is False

    @pytest.mark.asyncio
    async def test_verify_expired(self):
        """过期验证码返回False"""
        test_id = str(uuid.uuid4())
        
        with unittest.mock.patch('app.core.captcha.redis_client') as mock_redis:
            mock_redis.get = unittest.mock.AsyncMock(return_value=None)
            
            result = await CaptchaService.verify(test_id, "abcd")
            
            assert result is False

    @pytest.mark.asyncio
    async def test_verify_empty_input(self):
        """空输入返回False"""
        test_id = str(uuid.uuid4())
        
        result = await CaptchaService.verify(test_id, "")
        
        assert result is False

        result = await CaptchaService.verify("", "abcd")
        
        assert result is False

        result = await CaptchaService.verify("", "")
        
        assert result is False