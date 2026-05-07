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
from app.core.config import settings
from app.core.incentive_service import incentive_service
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
            "updatedAt": datetime.now(timezone.utc).isoformat()
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
            "updatedAt": datetime.now(timezone.utc).isoformat()
        })
        
    except Exception as e:
        # 任务失败
        await parse_client.update_object("AITask", task_id, {
            "status": TaskStatus.FAILED,
            "errorMessage": str(e),
            "updatedAt": datetime.now(timezone.utc).isoformat()
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
    balance = user.get("totalIncentive", 0)
    
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
    
    # 付费用户免费，普通用户扣费
    if not is_vip:
        if balance < cost:
            raise HTTPException(status_code=400, detail=f"余额不足，需要 {cost} 金币")
        # 扣除金币
        await parse_client.update_user(user_id, {
            "totalIncentive": parse_client.increment(-cost)
        })
    
    # 3. 生成任务ID
    task_id = generate_task_id()
    
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
        "updatedAt": datetime.now(timezone.utc).isoformat()
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
    
    # 退还金币
    cost = task.get("cost", 0)
    if cost > 0:
        await parse_client.update_user(user_id, {
            "totalIncentive": parse_client.increment(cost)
        })
    
    # 删除任务
    await parse_client.delete_object("AITask", task["objectId"])
    
    return {
        "success": True,
        "message": "任务已取消",
        "refund": cost
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
        "updatedAt": datetime.now(timezone.utc).isoformat()
    }
    await parse_client.update_object("AITask", task_object_id, update_data)
    logger.info(f"[任务完成] 任务状态已更新: {request.task_id}")
    
    # 4. 发放激励给执行者
    reward_amount = 1  # 默认任务奖励
    reward_tx_hash = None
    
    # 获取执行者用户信息（通过Web3地址查找）
    executor_users = await parse_client.query_users(
        where={"web3Address": {"$regex": f"(?i)^{request.executor}$"}}
    )
    
    if executor_users.get("results"):
        executor_user = executor_users["results"][0]
        executor_user_id = executor_user["objectId"]
        
        # 发放任务奖励
        reward_result = await incentive_service.grant_task_reward(
            user_id=executor_user_id,
            task_id=request.task_id,
            task_type=task.get("type", "unknown"),
            amount=reward_amount
        )
        
        if reward_result.get("success"):
            reward_tx_hash = reward_result.get("tx_hash")
            # 更新任务状态为已发放奖励
            await parse_client.update_object("AITask", task_object_id, {
                "status": TaskStatus.REWARDED,
                "rewardAmount": reward_amount,
                "rewardTxHash": reward_tx_hash
            })
            logger.info(f"[任务完成] 激励已发放: {reward_amount} 金币, txHash: {reward_tx_hash}")
        else:
            logger.warning(f"[任务完成] 激励发放失败: {reward_result.get('error')}")
    else:
        logger.warning(f"[任务完成] 未找到执行者用户: {request.executor}")
    
    return TaskCompleteResponse(
        success=True,
        message="任务完成，奖励已发放" if reward_tx_hash else "任务完成",
        task_id=request.task_id,
        status=TaskStatus.REWARDED if reward_tx_hash else TaskStatus.COMPLETED,
        reward_amount=reward_amount if reward_tx_hash else None,
        reward_tx_hash=reward_tx_hash
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
