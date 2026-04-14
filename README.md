# CloudendAPI

FastAPI后端服务，为AIGC云平台提供核心业务逻辑处理。

## 技术栈

- FastAPI + uvicorn
- Pydantic
- PostgreSQL
- Redis
- Parse SDK

## 启动方式

```bash
# Create virtual environment
sudo apt install python3.12-venv -y

# 创建虚拟环境
python3 -m venv .venv
# 激活虚拟环境
# Linux/macOS
source .venv/bin/activate

# 安装依赖
pip install -r requirements.txt

# 修改.env文件

# 启动服务
uvicorn app.main:app --reload --host 0.0.0.0 --port 8882
或
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8882
```

## API文档

启动服务后访问：
- Swagger UI: http://localhost:8882/docs
- ReDoc: http://localhost:8882/redoc

## 其他

手动触发定时任务：POST http://cloudend地址/api/v1/incentive/settle