"""
AI任务管理端点
"""
from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from enum import Enum
from datetime import datetime, timezone
import httpx
import uuid
import boto3
from botocore.config import Config

from app.core.parse_client import parse_client
from app.core.web3_client import web3_client
from app.core.security import generate_task_id
from app.core.deps import get_current_user_id
from app.core.deps import get_admin_user_id, get_operator_user_id
from app.core.config import settings
from app.core.incentive_service import incentive_service
from app.core.operation_log import log_operation
from app.core.logger import logger

router = APIRouter()


# ============ 枚举与模型 ============

class TaskType(str, Enum):
    TXT2IMG = "txt2img"
    IMG2IMG = "img2img"
    TXT2SPEECH = "txt2speech"
    SPEECH2TXT = "speech2txt"
    TXT2MUSIC = "txt2music"
    TXT2VIDEO = "txt2video"


class TaskStatus(int, Enum):
    PENDING = 0  # 排队中
    PROCESSING = 1  # 处理中
    COMPLETED = 2  # 完成
    FAILED = 3  # 失败
    REWARDED = 4  # 已发放奖励


class SubmitTaskRequest(BaseModel):
    type: TaskType
    model: str
    data: Dict[str, Any]


class TaskResult(BaseModel):
    CID: Optional[str] = None
    url: str
    thumbnail: Optional[str] = None


class TaskResponse(BaseModel):
    task_id: str
    type: TaskType
    model: str
    status: TaskStatus
    results: Optional[List[TaskResult]] = None
    created_at: datetime
    updated_at: Optional[datetime] = None


class UpdateTaskStatusRequest(BaseModel):
    status: TaskStatus
    results: Optional[List[TaskResult]] = None
    error_message: Optional[str] = None


# ============ 后台任务处理 ============

async def process_ai_task(task_id: str, task_type: str, model: str, data: Dict[str, Any]):
    """
    后台处理AI任务
    TODO: 实际对接AI服务(ComfyUI/Stable Diffusion等)
    """
    try:
        # 更新状态为处理中
        await parse_client.update_object("AITask", task_id, {
            "status": TaskStatus.PROCESSING,
        })
        
        # TODO: 根据任务类型调用不同的AI服务
        # 这里是模拟处理
        import asyncio
        await asyncio.sleep(2)  # 模拟处理时间
        
        # 模拟生成结果
        result_url = f"https://storage.example.com/results/{task_id}.png"
        
        # 更新任务结果
        await parse_client.update_object("AITask", task_id, {
            "status": TaskStatus.COMPLETED,
            "results": [{
                "url": result_url,
                "thumbnail": result_url,
            }],
        })
        
    except Exception as e:
        # 任务失败
        await parse_client.update_object("AITask", task_id, {
            "status": TaskStatus.FAILED,
            "errorMessage": str(e),
        })


# ============ 端点 ============

@router.post("/submit", response_model=TaskResponse)
async def submit_task(
    request: SubmitTaskRequest,
    background_tasks: BackgroundTasks,
    user_id: str = Depends(get_current_user_id)
):
    """
    提交AI生成任务
    """
    # 1. 验证用户状态
    try:
        user = await parse_client.get_user(user_id)
    except Exception:
        raise HTTPException(status_code=404, detail="用户不存在")
    
    # 2. 检查用户余额或会员状态
    member_level = user.get("memberLevel", "normal")
    is_vip = member_level in ("vip", "svip")
    
    # 任务消耗配置
    task_costs = {
        "txt2img": 10,
        "img2img": 15,
        "txt2speech": 5,
        "speech2txt": 5,
        "txt2music": 20,
        "txt2video": 50,
    }
    
    cost = task_costs.get(request.type, 10)
    
    # 3. 生成任务ID（需要先拿 task_id 作为账本关联）
    task_id = generate_task_id()
    
    # 付费用户免费，普通用户扣费（走统一账本 adjust_user_balance）
    if not is_vip and cost > 0:
        deduct_result = await incentive_service.adjust_user_balance(
            user_id=user_id,
            delta=-float(cost),
            type_="consume",
            category="task_cost",
            description=f"提交 {request.type} 任务扣费",
            related_id=f"task_cost_{task_id}",
            check_idempotent=False,
        )
        if not deduct_result.get("success"):
            raise HTTPException(status_code=400, detail=deduct_result.get("error") or f"余额不足，需要 {cost} 金币")
    
    # 4. 创建任务记录
    task_data = {
        "taskId": task_id,
        "designer": user_id,
        "executor": None,  # Worker 接取任务时填入 Web3 地址
        "type": request.type,
        "model": request.model,
        "data": request.data,
        "status": TaskStatus.PENDING,
        "cost": cost if not is_vip else 0,
        "retryCount": 0,
    }
    
    result = await parse_client.create_object("AITask", task_data)
    
    # 5. 加入后台处理队列
    background_tasks.add_task(
        process_ai_task,
        result["objectId"],
        request.type,
        request.model,
        request.data
    )
    
    return TaskResponse(
        task_id=task_id,
        type=request.type,
        model=request.model,
        status=TaskStatus.PENDING,
        created_at=datetime.now(timezone.utc),
    )


@router.get("/{task_id}", response_model=TaskResponse)
async def get_task(task_id: str, user_id: str = Depends(get_current_user_id)):
    """
    获取任务状态
    """
    # 查询任务
    tasks = await parse_client.query_objects("AITask", where={"taskId": task_id})
    
    if not tasks.get("results"):
        raise HTTPException(status_code=404, detail="任务不存在")
    
    task = tasks["results"][0]
    
    # 验证任务归属
    if task.get("designer") != user_id:
        raise HTTPException(status_code=403, detail="无权访问此任务")
    
    results = None
    if task.get("results"):
        results = [TaskResult(**r) for r in task["results"]]
    
    return TaskResponse(
        task_id=task["taskId"],
        type=task["type"],
        model=task["model"],
        status=task["status"],
        results=results,
        created_at=datetime.fromisoformat(task["createdAt"].replace("Z", "+00:00")),
        updated_at=datetime.fromisoformat(task["updatedAt"].replace("Z", "+00:00")) if task.get("updatedAt") else None,
    )


@router.get("/user/list")
async def get_user_tasks(
    page: int = 1,
    limit: int = 20,
    type: Optional[str] = None,
    status: Optional[int] = None,
    user_id: str = Depends(get_current_user_id)
):
    """
    获取用户的任务列表
    """
    where = {"designer": user_id}
    if type:
        where["type"] = type
    if status is not None:
        where["status"] = status
    
    skip = (page - 1) * limit
    
    result = await parse_client.query_objects(
        "AITask",
        where=where,
        order="-createdAt",
        limit=limit,
        skip=skip
    )
    
    total = await parse_client.count_objects("AITask", where)
    
    tasks = []
    for task in result.get("results", []):
        tasks.append({
            "task_id": task["taskId"],
            "type": task["type"],
            "model": task["model"],
            "status": task["status"],
            "results": task.get("results"),
            "created_at": task["createdAt"],
            "updated_at": task.get("updatedAt"),
        })
    
    return {
        "data": tasks,
        "total": total,
        "page": page,
        "limit": limit,
    }


@router.get("/admin/list")
async def get_admin_tasks(
    page: int = 1,
    limit: int = 20,
    type: Optional[str] = None,
    status: Optional[int] = None,
    designer: Optional[str] = None,
    user_id: str = Depends(get_operator_user_id),
):
    """管理员/运营查看全部 AI 任务列表（支持按提交者 designer userId 过滤）"""
    where: dict = {}
    if type:
        where["type"] = type
    if status is not None:
        where["status"] = status
    if designer:
        # 支持按 userId 精确 或 用户名 模糊搜索
        user_kw = designer.strip()
        candidate_ids = {user_kw}
        try:
            u_res = await parse_client.query_users(
                where={"username": {"$regex": user_kw, "$options": "i"}}, limit=50
            )
            for u in u_res.get("results", []):
                if u.get("objectId"):
                    candidate_ids.add(u["objectId"])
        except Exception:
            pass
        where["designer"] = {"$in": list(candidate_ids)} if len(candidate_ids) > 1 else user_kw

    skip = (page - 1) * limit
    result = await parse_client.query_objects(
        "AITask",
        where=where if where else None,
        order="-createdAt",
        limit=limit,
        skip=skip,
    )
    total = await parse_client.count_objects("AITask", where if where else None)

    tasks = []
    for task in result.get("results", []):
        # 附带提交人用户名
        designer_id = task.get("designer", "")
        username = ""
        if designer_id:
            try:
                u = await parse_client.get_user(designer_id)
                username = u.get("username", "")
            except Exception:
                pass
        tasks.append({
            "objectId": task.get("objectId"),
            "task_id": task.get("taskId"),
            "type": task.get("type"),
            "model": task.get("model"),
            "status": task.get("status"),
            "designer": designer_id,
            "username": username,
            "results": task.get("results"),
            "errorMessage": task.get("errorMessage"),
            "data": task.get("data") or {},
            "cost": task.get("cost") or 0,
            "created_at": task.get("createdAt"),
            "updated_at": task.get("updatedAt"),
        })

    return {
        "data": tasks,
        "total": total,
        "page": page,
        "limit": limit,
    }


@router.delete("/admin/{task_object_id}")
async def admin_delete_task(
    task_object_id: str,
    user_id: str = Depends(get_operator_user_id),
):
    """管理员/运营删除 AI 任务（不限制状态）"""
    try:
        task = await parse_client.get_object("AITask", task_object_id)
    except Exception:
        raise HTTPException(status_code=404, detail="任务不存在")
    try:
        await parse_client.delete_object("AITask", task_object_id)
    except Exception as e:
        logger.error(f"[AdminTasks] 删除任务失败 task_object_id={task_object_id}: {e}")
        raise HTTPException(status_code=500, detail="删除失败")
    return {"success": True, "task_id": task.get("taskId")}


class AdminTaskResultItem(BaseModel):
    url: str
    thumbnail: Optional[str] = None
    type: Optional[str] = None


class AdminUpdateTaskRequest(BaseModel):
    description: Optional[str] = None
    status: Optional[int] = None
    results: Optional[List[AdminTaskResultItem]] = None
    error_message: Optional[str] = None


@router.put("/admin/{task_object_id}")
async def admin_update_task(
    task_object_id: str,
    payload: AdminUpdateTaskRequest,
    user_id: str = Depends(get_operator_user_id),
):
    """管理员/运营编辑 AI 任务（描述 / 状态 / 上传结果文件）"""
    try:
        task = await parse_client.get_object("AITask", task_object_id)
    except Exception:
        raise HTTPException(status_code=404, detail="任务不存在")

    prev_status = task.get("status")
    prev_data = task.get("data") or {}

    update_data: Dict[str, Any] = {}

    # 描述：合并更新到 data.description（同时兼容 prompt卡口）
    if payload.description is not None:
        merged = dict(prev_data) if isinstance(prev_data, dict) else {}
        merged["description"] = payload.description
        update_data["data"] = merged

    # 状态
    if payload.status is not None:
        update_data["status"] = payload.status
        if payload.status == TaskStatus.COMPLETED and prev_status != TaskStatus.COMPLETED:
            update_data["completedAt"] = datetime.now(timezone.utc).isoformat()

    # 结果
    if payload.results is not None:
        update_data["results"] = [r.model_dump() for r in payload.results]

    if payload.error_message is not None:
        update_data["errorMessage"] = payload.error_message

    if not update_data:
        raise HTTPException(status_code=400, detail="未指定任何更新字段")

    try:
        await parse_client.update_object("AITask", task_object_id, update_data)
    except Exception as e:
        logger.error(f"[AdminTasks] 更新任务失败 task_object_id={task_object_id}: {e}")
        raise HTTPException(status_code=500, detail="更新失败")

    # 写操作日志（不阻断主流程）
    try:
        await log_operation(
            operator_id=user_id,
            action="update",
            module="tasks",
            target_class="AITask",
            target_id=task_object_id,
            target_name=task.get("taskId") or task_object_id,
            description="管理员编辑 AI 任务",
            detail={
                "fields": list(update_data.keys()),
                "prev_status": prev_status,
                "new_status": payload.status,
            },
        )
    except Exception:
        pass

    return {"success": True, "task_id": task.get("taskId"), "updated_fields": list(update_data.keys())}


@router.post("/{task_object_id}/update-status")
async def update_task_status(
    task_object_id: str,
    request: UpdateTaskStatusRequest
):
    """
    更新任务状态(内部调用/Worker回调)
    """
    # 获取任务
    try:
        task = await parse_client.get_object("AITask", task_object_id)
    except Exception:
        raise HTTPException(status_code=404, detail="任务不存在")
    
    update_data = {
        "status": request.status,
    }
    
    if request.results:
        update_data["results"] = [r.model_dump() for r in request.results]
    
    if request.error_message:
        update_data["errorMessage"] = request.error_message
    
    # 更新任务
    await parse_client.update_object("AITask", task_object_id, update_data)
    
    # 如果任务完成，发放任务完成奖励
    if request.status == TaskStatus.COMPLETED:
        user_id = task.get("designer")
        if user_id:
            # 获取用户 Web3 地址
            try:
                user = await parse_client.get_user(user_id)
                web3_address = user.get("web3Address")
                reward_amount = 1  # 任务完成奖励1金币
                
                if web3_address:
                    # 通过 Web3 接口发放金币
                    mint_result = await web3_client.mint(web3_address, reward_amount)
                    await parse_client.create_object("IncentiveLog", {
                        "userId": user_id,
                        "web3Address": web3_address,
                        "type": "task",
                        "amount": reward_amount,
                        "txHash": mint_result.get("tx_hash"),
                        "description": f"完成{task['type']}任务奖励"
                    })
            except Exception as e:
                print(f"发放任务奖励失败: {e}")
    
    return {
        "success": True,
        "task_id": task.get("taskId"),
        "status": request.status,
    }


@router.delete("/{task_id}")
async def cancel_task(task_id: str, user_id: str = Depends(get_current_user_id)):
    """
    取消任务(仅排队中的任务可取消)
    """
    # 查询任务
    tasks = await parse_client.query_objects("AITask", where={"taskId": task_id})
    
    if not tasks.get("results"):
        raise HTTPException(status_code=404, detail="任务不存在")
    
    task = tasks["results"][0]
    
    # 验证任务归属
    if task.get("designer") != user_id:
        raise HTTPException(status_code=403, detail="无权操作此任务")
    
    # 只有排队中的任务可以取消
    if task.get("status") != TaskStatus.PENDING:
        raise HTTPException(status_code=400, detail="只有排队中的任务可以取消")
    
    # 退还金币（走统一账本，idempotent by task_id）
    cost = task.get("cost", 0)
    if cost > 0:
        refund_result = await incentive_service.adjust_user_balance(
            user_id=user_id,
            delta=float(cost),
            type_="refund",
            category="task_refund",
            description=f"取消 {task.get('type', '')} 任务退费",
            related_id=f"task_refund_{task_id}",
            check_idempotent=True,
        )
        if not refund_result.get("success"):
            logger.error(f"[任务取消] 退费失败 task_id={task_id}: {refund_result.get('error')}")
            raise HTTPException(status_code=500, detail=f"退费失败: {refund_result.get('error', '')}")
    
    # 删除任务
    await parse_client.delete_object("AITask", task["objectId"])
    
    return {
        "success": True,
        "message": "任务已取消",
        "refund": cost
    }


@router.post("/{task_id}/retry")
async def retry_task(
    task_id: str,
    background_tasks: BackgroundTasks,
    user_id: str = Depends(get_current_user_id),
):
    """
    重试失败任务
    - 仅失败状态(3)可重试
    - 重置状态为待执行(0)
    - 不退还金币，需重新支付
    """
    # 查询任务
    tasks = await parse_client.query_objects("AITask", where={"taskId": task_id})
    
    if not tasks.get("results"):
        raise HTTPException(status_code=404, detail="任务不存在")
    
    task = tasks["results"][0]
    task_object_id = task["objectId"]
    
    # 验证任务归属
    if task.get("designer") != user_id:
        raise HTTPException(status_code=403, detail="无权操作此任务")
    
    # 只有失败状态可以重试
    if task.get("status") != TaskStatus.FAILED:
        raise HTTPException(status_code=400, detail="只有失败任务可以重试")
    
    # 检查重试次数
    retry_count = task.get("retryCount", 0)
    if retry_count >= 3:
        raise HTTPException(status_code=400, detail="已达最大重试次数(3次)")
    
    # 获取原任务成本
    cost = task.get("cost", 0)
    if cost > 0:
        # 走统一账本扣费（每次重试独立 relatedId）
        deduct_result = await incentive_service.adjust_user_balance(
            user_id=user_id,
            delta=-float(cost),
            type_="consume",
            category="task_retry_cost",
            description=f"重试 {task.get('type', '')} 任务扣费（第{retry_count + 1}次）",
            related_id=f"task_retry_{task_id}_{retry_count + 1}",
            check_idempotent=False,
        )
        if not deduct_result.get("success"):
            raise HTTPException(status_code=400, detail=deduct_result.get("error") or f"金币不足，需要 {cost} 金币")
    
    # 重置任务状态
    await parse_client.update_object("AITask", task_object_id, {
        "status": TaskStatus.PENDING,
        "retryCount": retry_count + 1,
        "error": None,
    })
    
    # 重新添加到处理队列
    background_tasks.add_task(
        process_ai_task,
        task_object_id,
        task.get("type"),
        task.get("model"),
        task.get("data") or {},
    )
    
    logger.info(f"[任务重试] task_id={task_id}, retry={retry_count + 1}")
    
    return {
        "success": True,
        "message": f"任务已重试({retry_count + 1}/3)",
        "task_id": task_id,
        "cost": cost,
    }


# ============ 任务完成验证与激励发放 ============

class CompleteTaskRequest(BaseModel):
    """Worker完成任务请求"""
    task_id: str                # 任务ID
    executor: str               # 执行者Web3地址
    results: List[TaskResult]   # 任务结果


class TaskCompleteResponse(BaseModel):
    success: bool
    message: str
    task_id: str
    status: int
    reward_amount: Optional[float] = None
    reward_tx_hash: Optional[str] = None


def get_s3_client():
    """获取 S3 客户端"""
    return boto3.client(
        's3',
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
        config=Config(
            signature_version='s3v4',
            s3={'addressing_style': 'path'}
        )
    )


async def fetch_from_ipfs(cid: str) -> Optional[bytes]:
    """
    从IPFS获取文件
    
    Args:
        cid: IPFS CID
        
    Returns:
        文件内容或None
    """
    # 尝试多个公共IPFS网关
    gateways = [
        f"https://ipfs.io/ipfs/{cid}",
        f"https://gateway.pinata.cloud/ipfs/{cid}",
        f"https://cloudflare-ipfs.com/ipfs/{cid}",
        f"https://dweb.link/ipfs/{cid}",
    ]
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        for gateway in gateways:
            try:
                logger.info(f"[任务验证] 尝试从IPFS获取: {gateway}")
                resp = await client.get(gateway)
                if resp.status_code == 200:
                    logger.info(f"[任务验证] IPFS获取成功, 大小: {len(resp.content)} bytes")
                    return resp.content
            except Exception as e:
                logger.warning(f"[任务验证] IPFS网关失败 {gateway}: {e}")
                continue
    
    return None


async def verify_url_file(url: str) -> dict:
    """
    验证URL文件是否有效
    
    Args:
        url: 文件URL
        
    Returns:
        验证结果
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            # 先发HEAD请求检查文件是否存在
            resp = await client.head(url, follow_redirects=True)
            if resp.status_code != 200:
                return {"valid": False, "error": f"URL返回状态码: {resp.status_code}"}
            
            content_type = resp.headers.get("content-type", "")
            content_length = resp.headers.get("content-length", "0")
            
            # 检查文件类型是否合法（图片、音频、视频）
            valid_types = [
                "image/", "audio/", "video/",
                "application/octet-stream",
            ]
            is_valid_type = any(content_type.startswith(t) for t in valid_types)
            
            if not is_valid_type and content_type:
                return {"valid": False, "error": f"不支持的文件类型: {content_type}"}
            
            return {
                "valid": True,
                "content_type": content_type,
                "content_length": int(content_length) if content_length else 0
            }
    except Exception as e:
        return {"valid": False, "error": str(e)}


async def upload_to_rustfs(content: bytes, filename: str, content_type: str) -> Optional[str]:
    """
    上传文件到RustFS
    
    Args:
        content: 文件内容
        filename: 文件名
        content_type: 文件类型
        
    Returns:
        文件URL或None
    """
    try:
        s3 = get_s3_client()
        
        # 生成唯一文件key
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d')
        unique_id = str(uuid.uuid4())[:8]
        ext = filename.split('.')[-1] if '.' in filename else 'bin'
        file_key = f"tasks/{timestamp}/{unique_id}.{ext}"
        
        # 上传文件
        s3.put_object(
            Bucket=settings.s3_bucket,
            Key=file_key,
            Body=content,
            ContentType=content_type
        )
        
        file_url = f"{settings.s3_public_url}/{settings.s3_bucket}/{file_key}"
        logger.info(f"[任务验证] 文件上传成功: {file_url}")
        return file_url
        
    except Exception as e:
        logger.error(f"[任务验证] 上传到RustFS失败: {e}")
        return None


@router.post("/complete", response_model=TaskCompleteResponse)
async def complete_task(request: CompleteTaskRequest):
    """
    Worker完成任务 - 验证结果并发放激励
    
    工作流程:
    1. 查询任务
    2. 验证任务结果（CID或URL）
    3. 如果是CID，从IPFS获取文件并上传到RustFS
    4. 如果是URL，验证文件有效性
    5. 更新任务状态和结果
    6. 发放激励
    """
    logger.info(f"[任务完成] 开始处理: task_id={request.task_id}, executor={request.executor}")
    
    # 1. 查询任务
    tasks = await parse_client.query_objects("AITask", where={"taskId": request.task_id})
    if not tasks.get("results"):
        raise HTTPException(status_code=404, detail="任务不存在")
    
    task = tasks["results"][0]
    task_object_id = task["objectId"]
    
    # 检查任务状态
    if task.get("status") == TaskStatus.REWARDED:
        return TaskCompleteResponse(
            success=True,
            message="任务已完成并已发放奖励",
            task_id=request.task_id,
            status=TaskStatus.REWARDED
        )
    
    if task.get("status") not in [TaskStatus.PENDING, TaskStatus.PROCESSING]:
        raise HTTPException(status_code=400, detail="任务状态异常")
    
    # 2. 验证任务结果
    if not request.results:
        raise HTTPException(status_code=400, detail="缺少任务结果")
    
    verified_results = []
    
    for result in request.results:
        cid = result.CID
        url = result.url
        
        # 情况1: 结果包含CID
        if cid:
            logger.info(f"[任务验证] 处理CID: {cid}")
            
            # 从IPFS获取文件
            file_content = await fetch_from_ipfs(cid)
            if not file_content:
                raise HTTPException(status_code=400, detail=f"无法从IPFS获取文件: {cid}")
            
            # 检查文件大小（最大100MB）
            if len(file_content) > 100 * 1024 * 1024:
                raise HTTPException(status_code=400, detail="文件过大（超过100MB）")
            
            # 上传到RustFS
            filename = f"{cid}.bin"
            content_type = "application/octet-stream"
            
            # 简单检测文件类型
            if file_content[:8] == b'\x89PNG\r\n\x1a\n':
                content_type = "image/png"
                filename = f"{cid}.png"
            elif file_content[:3] == b'\xff\xd8\xff':
                content_type = "image/jpeg"
                filename = f"{cid}.jpg"
            elif file_content[:4] == b'RIFF':
                content_type = "audio/wav"
                filename = f"{cid}.wav"
            elif file_content[:3] == b'ID3' or file_content[:2] == b'\xff\xfb':
                content_type = "audio/mpeg"
                filename = f"{cid}.mp3"
            
            rustfs_url = await upload_to_rustfs(file_content, filename, content_type)
            if not rustfs_url:
                raise HTTPException(status_code=500, detail="上传文件到RustFS失败")
            
            verified_results.append({
                "CID": cid,
                "url": rustfs_url,
                "thumbnail": rustfs_url if content_type.startswith("image/") else None
            })
        
        # 情况2: 结果包含URL
        elif url:
            logger.info(f"[任务验证] 验证URL: {url}")
            
            verify_result = await verify_url_file(url)
            if not verify_result.get("valid"):
                raise HTTPException(
                    status_code=400, 
                    detail=f"URL文件验证失败: {verify_result.get('error')}"
                )
            
            verified_results.append({
                "url": url,
                "thumbnail": result.thumbnail or url
            })
        else:
            raise HTTPException(status_code=400, detail="结果必须包含CID或URL")
    
    # 3. 更新任务状态和结果
    update_data = {
        "status": TaskStatus.COMPLETED,
        "executor": request.executor,
        "results": verified_results,
        "completedAt": datetime.now(timezone.utc).isoformat(),
    }
    await parse_client.update_object("AITask", task_object_id, update_data)
    logger.info(f"[任务完成] 任务状态已更新: {request.task_id}")
    
    # 4. 发放激励给执行者（账户积分）
    reward_amount = 1  # 默认任务奖励
    reward_granted = False
    
    # 获取执行者用户信息（通过Web3地址查找）
    executor_users = await parse_client.query_users(
        where={"web3Address": {"$regex": f"(?i)^{request.executor}$"}}
    )
    
    if executor_users.get("results"):
        executor_user = executor_users["results"][0]
        executor_user_id = executor_user["objectId"]
        
        # 发放任务奖励（写入 totalIncentive + AccountRecord）
        reward_result = await incentive_service.grant_task_reward(
            user_id=executor_user_id,
            task_id=request.task_id,
            task_type=task.get("type", "unknown"),
            amount=reward_amount
        )
        
        if reward_result.get("success"):
            reward_granted = True
            # 更新任务状态为已发放奖励
            await parse_client.update_object("AITask", task_object_id, {
                "status": TaskStatus.REWARDED,
                "rewardAmount": reward_amount,
            })
            logger.info(f"[任务完成] 激励已发放: {reward_amount} 积分 → {executor_user_id}")
        else:
            logger.warning(f"[任务完成] 激励发放失败: {reward_result.get('error')}")
    else:
        logger.warning(f"[任务完成] 未找到执行者用户: {request.executor}")
    
    return TaskCompleteResponse(
        success=True,
        message="任务完成，奖励已发放" if reward_granted else "任务完成",
        task_id=request.task_id,
        status=TaskStatus.REWARDED if reward_granted else TaskStatus.COMPLETED,
        reward_amount=reward_amount if reward_granted else None,
        reward_tx_hash=None
    )


@router.get("/pending")
async def get_pending_tasks(limit: int = 10):
    """
    获取待处理任务列表（供Worker查询）
    """
    result = await parse_client.query_objects(
        "AITask",
        where={"status": TaskStatus.PENDING},
        order="createdAt",
        limit=limit
    )
    
    tasks = []
    for task in result.get("results", []):
        tasks.append({
            "task_id": task["taskId"],
            "type": task["type"],
            "model": task["model"],
            "data": task.get("data"),
            "created_at": task["createdAt"],
        })
    
    return {"tasks": tasks, "count": len(tasks)}


@router.post("/{task_id}/claim")
async def claim_task(task_id: str, executor: str):
    """
    Worker认领任务
    
    Args:
        task_id: 任务ID
        executor: 执行者Web3地址
    """
    tasks = await parse_client.query_objects("AITask", where={"taskId": task_id})
    if not tasks.get("results"):
        raise HTTPException(status_code=404, detail="任务不存在")
    
    task = tasks["results"][0]
    
    if task.get("status") != TaskStatus.PENDING:
        raise HTTPException(status_code=400, detail="任务已被认领或已完成")
    
    if task.get("executor"):
        raise HTTPException(status_code=400, detail="任务已被其他Worker认领")
    
    await parse_client.update_object("AITask", task["objectId"], {
        "status": TaskStatus.PROCESSING,
        "executor": executor,
        "claimedAt": datetime.now(timezone.utc).isoformat(),
        "updatedAt": datetime.now(timezone.utc).isoformat()
    })
    
    return {
        "success": True,
        "message": "任务认领成功",
        "task_id": task_id,
        "task": {
            "type": task["type"],
            "model": task["model"],
            "data": task.get("data")
        }
    }


def _map_type_to_category(task_type: str) -> str:
    mapping = {
        "txt2img": "image",
        "txt2music": "music",
        "txt2video": "video",
        "txt2speech": "audio",
        "comfyui": "image",
    }
    return mapping.get(task_type, "other")


@router.post("/ai-task/{task_id}/convert")
async def convert_task_to_asset(
    task_id: str,
    user_id: str = Depends(get_current_user_id)
):
    """将AIGC任务的所有已完成结果转换为AI资产（支持多结果 + 幂等）"""
    # 先按业务 taskId 查，查不到再按 Parse objectId 查找（兼容旧数据/taskId 缺失场景）
    tasks = await parse_client.query_objects("AITask", where={"taskId": task_id})
    task = None
    if tasks.get("results"):
        task = tasks["results"][0]
    else:
        try:
            task = await parse_client.get_object("AITask", task_id)
        except Exception:
            task = None
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    # 统一使用任务业务ID作为溯源键（没有则用 objectId）
    source_task_id = task.get("taskId") or task.get("objectId") or task_id

    designer = task.get("designer")
    if designer != user_id:
        raise HTTPException(status_code=403, detail="无权操作")

    if task.get("status") not in [2, 4]:
        raise HTTPException(status_code=400, detail="任务未完成")

    results = task.get("results", [])
    if not results:
        raise HTTPException(status_code=400, detail="无有效结果")

    # 幂等：查询已转换的 result 索引
    existed = await parse_client.query_objects(
        "AIIPAsset",
        where={"sourceTaskId": source_task_id, "ownerId": user_id},
        limit=1000,
    )
    converted_indexes = {
        item.get("sourceResultIndex")
        for item in existed.get("results", [])
        if item.get("sourceResultIndex") is not None
    }

    task_type = task.get("type", "")
    category = _map_type_to_category(task_type)
    prompt = task.get("data", {}).get("prompt", "") or ""

    # 优先用任务记录的 ownerAddress；为空则回查当前用户的 web3Address；
    # 再不行就用 user_id 兑底，确保与用户端查询可匹配
    owner_address = task.get("ownerAddress") or ""
    if not owner_address:
        try:
            user_info = await parse_client.get_user(user_id)
            owner_address = user_info.get("web3Address") or ""
        except Exception:
            owner_address = ""
    if not owner_address:
        owner_address = user_id

    asset_ids: List[str] = []
    skipped_count = 0

    for idx, result in enumerate(results):
        if idx in converted_indexes:
            skipped_count += 1
            continue
        url = result.get("url")
        if not url:
            skipped_count += 1
            continue

        name = f"{task_type}_{source_task_id[:8]}"
        if len(results) > 1:
            name = f"{name}_{idx + 1}"

        asset_data = {
            "name": name,
            "category": category,
            "cover": result.get("thumbnail") or url,
            "assetUrl": url,
            "status": "draft",
            "ownerId": user_id,
            "ownerAddress": owner_address,
            "copyright": owner_address,
            "license": "CC-BY-NC-ND",
            "views": 0,
            "description": prompt,
            "sourceTaskId": source_task_id,
            "sourceResultIndex": idx,
        }

        create_result = await parse_client.create_object("AIIPAsset", asset_data)
        obj_id = create_result.get("objectId")
        if obj_id:
            asset_ids.append(obj_id)

    converted_count = len(asset_ids)
    if converted_count == 0 and skipped_count > 0:
        message = "该任务的结果已全部转换为资产，无需重复操作"
    elif converted_count > 0 and skipped_count > 0:
        message = f"成功转换 {converted_count} 个新资产，已跳过 {skipped_count} 个已转换结果"
    else:
        message = f"成功转换 {converted_count} 个资产"

    return {
        "success": True,
        "asset_ids": asset_ids,
        "converted_count": converted_count,
        "skipped_count": skipped_count,
        "message": message,
    }


# ============ 边缘节点批次化调度 API ============

class BatchClaimRequest(BaseModel):
    task_types: Optional[List[str]] = None  # 为空表示拉取所有类型
    limit: int = 20                          # 默认 20，上限 100
    executor_address: str                    # 执行者 Web3 地址


class BatchClaimedTask(BaseModel):
    task_id: str
    type: str
    model: str
    data: Optional[Dict[str, Any]] = None
    priority: int = 0
    cost: Optional[float] = 0
    created_at: Optional[str] = None


class BatchClaimResponse(BaseModel):
    success: bool
    claimed: int
    tasks: List[BatchClaimedTask]


class BatchCompletionItem(BaseModel):
    task_id: str
    status: str                              # success | failed | timeout
    result_url: Optional[str] = None
    cid: Optional[str] = None
    error_message: Optional[str] = None
    processing_time: Optional[float] = None
    completed_at: Optional[str] = None


class BatchCompleteRequest(BaseModel):
    executor_address: str
    completions: List[BatchCompletionItem]


class BatchCompleteItemResult(BaseModel):
    task_id: str
    ok: bool
    status: str                              # REWARDED | COMPLETED | FAILED | SKIPPED
    reward_amount: Optional[float] = None
    error: Optional[str] = None


class BatchCompleteResponse(BaseModel):
    success: bool
    total: int
    success_count: int
    failed_count: int
    results: List[BatchCompleteItemResult]


@router.post("/node/batch-claim", response_model=BatchClaimResponse)
async def batch_claim_tasks(
    request: BatchClaimRequest,
    user_id: str = Depends(get_current_user_id),
):
    """
    边缘节点批量认领任务

    流程:
    1. 查询 status=PENDING 且未被认领(executor 为空) 的任务
    2. 按 priority DESC, createdAt ASC 排序
    3. 逐个尝试原子更新 (status -> PROCESSING, executor=地址, claimedAt=现在)
       失败的跳过, 避免并发抢单冲突
    4. 返回成功认领的任务数据
    """
    limit = max(1, min(int(request.limit or 20), 100))
    executor_address = (request.executor_address or "").strip()
    if not executor_address:
        raise HTTPException(status_code=400, detail="executor_address 不能为空")

    # Parse 查询只做粗筛：status + 可选 type。
    # executor / excutor 空值判断放到 Python 侧遍历时做，
    # 避免 Parse Server 嵌套 $and+$or 语法在部分版本中的兵容问题
    where: Dict[str, Any] = {
        "status": TaskStatus.PENDING.value,
    }
    if request.task_types:
        where["type"] = {"$in": list(request.task_types)}

    # 多拉取一些作为备选, 避免并发认领冲突 + Python 侧过滤后实际返回不足
    query_result = await parse_client.query_objects(
        "AITask",
        where=where,
        order="-priority,createdAt",
        limit=limit * 5,
    )
    raw_results = query_result.get("results", []) or []

    # 诊断：如果粗筛为 0，额外跑两次排查查询 ——
    # A. 只按 status=PENDING 查，看比 type 过滤后多少，确认 type 值实际是什么
    # B. 不加任何过滤，看表里底岂还有没有数据 + status 实际存储类型
    if not raw_results:
        try:
            probe_status = await parse_client.query_objects(
                "AITask", where={"status": TaskStatus.PENDING.value}, limit=5,
            )
            probe_rows = probe_status.get("results", []) or []
            logger.info(
                f"[batch-claim][probe] status-only hit={len(probe_rows)} "
                f"sample_types={[r.get('type') for r in probe_rows[:5]]}"
            )

            probe_any = await parse_client.query_objects("AITask", where={}, limit=3)
            any_rows = probe_any.get("results", []) or []
            logger.info(
                f"[batch-claim][probe] any hit={len(any_rows)} "
                f"sample=" + "; ".join(
                    f"objectId={r.get('objectId')} "
                    f"status={r.get('status')!r}({type(r.get('status')).__name__}) "
                    f"type={r.get('type')!r}"
                    for r in any_rows[:3]
                )
            )
        except Exception as e:
            logger.warning(f"[batch-claim][probe] 诊断查询失败: {e}")

    # Python 侧过滤：executor 和 excutor 两个字段都必须为空才视为未认领
    def _is_empty(val: Any) -> bool:
        if val is None:
            return True
        if isinstance(val, str):
            return not val.strip()
        return False

    candidates = [
        t for t in raw_results
        if _is_empty(t.get("executor")) and _is_empty(t.get("excutor"))
    ]
    logger.info(
        f"[batch-claim] query types={list(request.task_types) if request.task_types else 'ALL'} "
        f"raw={len(raw_results)} candidates={len(candidates)}"
    )

    claimed: List[BatchClaimedTask] = []
    now_iso = datetime.now(timezone.utc).isoformat()
    # 诊断统计：方便定位 "candidates 不为 0 但 claimed=0" 的真相
    skipped_stale = 0    # 二次校验时状态或 executor 不符合
    skipped_refresh_fail = 0  # get_object 异常
    skipped_update_fail = 0   # update_object 异常
    skipped_missing_id = 0    # objectId / taskId 字段缺失
    sample_logged = 0         # 采样打印前几条详细状态方便诊断

    # 采样打印首条候选的字段名，定位 taskId 实际存储的字段名
    if candidates:
        first = candidates[0]
        logger.info(
            f"[batch-claim] candidate[0] keys={list(first.keys())} "
            f"objectId={first.get('objectId')!r} taskId={first.get('taskId')!r} "
            f"task_id={first.get('task_id')!r}"
        )

    for task in candidates:
        if len(claimed) >= limit:
            break
        object_id = task.get("objectId")
        if not object_id:
            # Parse 返回数据必带 objectId，理论上不会进此分支
            skipped_missing_id += 1
            continue
        # 统一使用 objectId 作为 task_id 向客户端返回：
        # - 新任务不再依赖 generate_task_id() 的业务 ID
        # - 老任务比如有 taskId 也忽略，统一用 Parse 主键，避免两套 ID
        task_id = object_id

        # 再次确认任务仍可被认领, 存在并发已被其他节点拿走的情况
        try:
            fresh = await parse_client.get_object("AITask", object_id)
            fresh_status = fresh.get("status")
            # 同时检查 executor / excutor 两个字段是否都为空
            fresh_exec = fresh.get("executor")
            fresh_exec_legacy = fresh.get("excutor")
            exec_taken = (
                (str(fresh_exec) if fresh_exec is not None else "").strip()
                or (str(fresh_exec_legacy) if fresh_exec_legacy is not None else "").strip()
            )
            if fresh_status != TaskStatus.PENDING.value or exec_taken:
                skipped_stale += 1
                if sample_logged < 3:
                    logger.info(
                        f"[batch-claim] skip stale task_id={task_id} "
                        f"status={fresh_status!r}(type={type(fresh_status).__name__}) "
                        f"executor={fresh_exec!r} excutor={fresh_exec_legacy!r}"
                    )
                    sample_logged += 1
                continue
        except Exception as e:
            skipped_refresh_fail += 1
            logger.warning(f"[batch-claim] 刷新任务失败 {task_id}: {e}")
            continue

        update_payload: Dict[str, Any] = {
            "status": TaskStatus.PROCESSING.value,
            "executor": executor_address,
            "claimedAt": now_iso,
        }
        # 对于历史 excutor="" 的数据，把它额外清为空串，避免前端误读存量旧字段
        if "excutor" in task and not (task.get("excutor") or "").strip():
            update_payload["excutor"] = ""
        try:
            await parse_client.update_object("AITask", object_id, update_payload)
        except Exception as e:
            skipped_update_fail += 1
            logger.warning(f"[batch-claim] 认领失败 {task_id}: {e}")
            continue

        claimed.append(BatchClaimedTask(
            task_id=task_id,
            type=task.get("type", ""),
            model=task.get("model", ""),
            data=task.get("data") or {},
            priority=int(task.get("priority") or 0),
            cost=float(task.get("cost") or 0),
            created_at=task.get("createdAt"),
        ))

    logger.info(
        f"[batch-claim] user={user_id} executor={executor_address} "
        f"requested={limit} candidates={len(candidates)} claimed={len(claimed)} "
        f"skipped_stale={skipped_stale} refresh_fail={skipped_refresh_fail} "
        f"update_fail={skipped_update_fail} missing_id={skipped_missing_id}"
    )

    return BatchClaimResponse(
        success=True,
        claimed=len(claimed),
        tasks=claimed,
    )


@router.post("/node/batch-complete", response_model=BatchCompleteResponse)
async def batch_complete_tasks(
    request: BatchCompleteRequest,
    user_id: str = Depends(get_current_user_id),
):
    """
    边缘节点批量回传任务完成状态

    对每条 completion:
    - success: 更新为 COMPLETED, 发放积分 (grant_task_reward 幂等), 最终 REWARDED
    - failed/timeout: 更新为 FAILED, 记录 errorMessage
    """
    executor_address = (request.executor_address or "").strip()
    if not executor_address:
        raise HTTPException(status_code=400, detail="executor_address 不能为空")

    completions = request.completions or []
    if not completions:
        raise HTTPException(status_code=400, detail="completions 不能为空")

    # 查出执行者用户一次, 后面批量复用
    executor_user_id: Optional[str] = None
    try:
        executor_users = await parse_client.query_users(
            where={"web3Address": {"$regex": f"(?i)^{executor_address}$"}}
        )
        if executor_users.get("results"):
            executor_user_id = executor_users["results"][0].get("objectId")
    except Exception as e:
        logger.warning(f"[batch-complete] 查询执行者用户失败: {e}")

    results: List[BatchCompleteItemResult] = []
    success_count = 0
    failed_count = 0

    for item in completions:
        task_id = item.task_id
        try:
            # 任务查找策略：
            # 1. 优先将 task_id 当作 Parse objectId 直接 get_object
            #    （batch-claim 已统一返回 objectId，主键查询更快且无需扫描）
            # 2. 若当作 objectId 找不到（老客户端已认领且 task_id 是旧的 taskId 字段值），
            #    fallback 到 query_objects(where={"taskId": task_id})
            task: Optional[Dict[str, Any]] = None
            try:
                task = await parse_client.get_object("AITask", task_id)
            except Exception:
                task = None
            if not task:
                query = await parse_client.query_objects(
                    "AITask", where={"taskId": task_id}, limit=1
                )
                task_list = query.get("results", []) or []
                if not task_list:
                    results.append(BatchCompleteItemResult(
                        task_id=task_id, ok=False, status="SKIPPED", error="任务不存在"
                    ))
                    failed_count += 1
                    continue
                task = task_list[0]

            object_id = task["objectId"]
            current_status = task.get("status")

            # 幂等: 已发放奖励直接返回成功
            if current_status == TaskStatus.REWARDED.value:
                results.append(BatchCompleteItemResult(
                    task_id=task_id, ok=True, status="REWARDED",
                    reward_amount=float(task.get("rewardAmount") or 0),
                ))
                success_count += 1
                continue

            completed_at_iso = item.completed_at or datetime.now(timezone.utc).isoformat()

            if item.status == "success":
                # 构造结果 (仅使用 url/cid 记录, 不做 IPFS/Rustfs 二次处理, 由节点自行保证可访问)
                verified_results: List[Dict[str, Any]] = []
                if item.result_url or item.cid:
                    verified_results.append({
                        "CID": item.cid,
                        "url": item.result_url or "",
                        "thumbnail": item.result_url,
                    })
                update_data = {
                    "status": TaskStatus.COMPLETED.value,
                    "executor": executor_address,
                    "results": verified_results,
                    "completedAt": completed_at_iso,
                }
                if item.processing_time is not None:
                    update_data["processingTime"] = float(item.processing_time)
                await parse_client.update_object("AITask", object_id, update_data)

                # 发放积分 (幂等 related_id=task_id)
                reward_amount = 1.0
                rewarded = False
                if executor_user_id:
                    reward_result = await incentive_service.grant_task_reward(
                        user_id=executor_user_id,
                        task_id=task_id,
                        task_type=task.get("type", "unknown"),
                        amount=reward_amount,
                    )
                    if reward_result.get("success"):
                        rewarded = True
                        await parse_client.update_object("AITask", object_id, {
                            "status": TaskStatus.REWARDED.value,
                            "rewardAmount": reward_amount,
                        })

                results.append(BatchCompleteItemResult(
                    task_id=task_id, ok=True,
                    status="REWARDED" if rewarded else "COMPLETED",
                    reward_amount=reward_amount if rewarded else None,
                ))
                success_count += 1
            else:
                # failed / timeout
                update_data = {
                    "status": TaskStatus.FAILED.value,
                    "executor": executor_address,
                    "errorMessage": item.error_message or f"executor reported {item.status}",
                    "completedAt": completed_at_iso,
                }
                if item.processing_time is not None:
                    update_data["processingTime"] = float(item.processing_time)
                await parse_client.update_object("AITask", object_id, update_data)
                results.append(BatchCompleteItemResult(
                    task_id=task_id, ok=True, status="FAILED"
                ))
                failed_count += 1
        except HTTPException:
            raise
        except Exception as e:
            logger.exception(f"[batch-complete] 处理 {task_id} 失败: {e}")
            results.append(BatchCompleteItemResult(
                task_id=task_id, ok=False, status="SKIPPED", error=str(e)
            ))
            failed_count += 1

    logger.info(
        f"[batch-complete] user={user_id} executor={executor_address} "
        f"total={len(completions)} success={success_count} failed={failed_count}"
    )

    return BatchCompleteResponse(
        success=True,
        total=len(completions),
        success_count=success_count,
        failed_count=failed_count,
        results=results,
    )
