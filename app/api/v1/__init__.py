from fastapi import APIRouter

from app.api.v1.endpoints import users, payment, tasks, incentive, promotion, products, auth, storage, member, admin, assets, operator, operation_logs

router = APIRouter()

# 认证相关
router.include_router(auth.router, prefix="/auth", tags=["Auth"])

# 业务端点
router.include_router(users.router, prefix="/users", tags=["Users"])
router.include_router(payment.router, prefix="/payment", tags=["Payment"])
router.include_router(tasks.router, prefix="/tasks", tags=["Tasks"])
router.include_router(incentive.router, prefix="/incentive", tags=["Incentive"])
router.include_router(promotion.router, prefix="/promotion", tags=["Promotion"])
router.include_router(products.router, prefix="/products", tags=["Products"])

# 文件存储（预签名URL）
router.include_router(storage.router, prefix="/storage", tags=["Storage"])

# 会员订阅
router.include_router(member.router, prefix="/member", tags=["Member"])

# AI资产
router.include_router(assets.router, prefix="/assets", tags=["Assets"])

# 运营端
router.include_router(operator.router, prefix="/operator", tags=["Operator"])

# 管理端
router.include_router(admin.router, prefix="/admin", tags=["Admin"])

# 操作日志（admin/operator 共用）
router.include_router(operation_logs.router, prefix="/operation-logs", tags=["OperationLog"])
