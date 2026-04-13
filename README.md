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
# 安装依赖
pip install -r requirements.txt

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