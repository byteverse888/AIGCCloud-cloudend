"""
S3 文件存储服务
生成预签名URL供客户端直接上传/下载，密钥仅存于服务端
"""
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional
import boto3
from botocore.config import Config
from datetime import datetime, timezone
import uuid

from app.core.config import settings
from app.core.deps import get_current_user_id

router = APIRouter()


# S3 客户端配置
def get_s3_client():
    """获取 S3 客户端（兼容 RustFS/MinIO/COS）"""
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


# ============ 请求/响应模型 ============

class PresignedUploadRequest(BaseModel):
    filename: str
    content_type: Optional[str] = "application/octet-stream"
    prefix: Optional[str] = "uploads"  # 存储路径前缀


class PresignedUploadResponse(BaseModel):
    upload_url: str  # 预签名上传URL
    file_url: str    # 文件访问URL（上传成功后）
    file_key: str    # 文件在S3中的key
    expires_in: int  # URL有效期（秒）


class PresignedDownloadRequest(BaseModel):
    file_key: str


class PresignedDownloadResponse(BaseModel):
    download_url: str
    expires_in: int


class BatchPresignedRequest(BaseModel):
    files: list[dict]  # [{"filename": "a.jpg", "content_type": "image/jpeg"}]
    prefix: Optional[str] = "uploads"


# ============ 端点 ============

@router.post("/presign/upload", response_model=PresignedUploadResponse)
async def get_presigned_upload_url(
    request: PresignedUploadRequest,
    user_id: str = Depends(get_current_user_id)
):
    """
    获取预签名上传URL
    客户端使用此URL直接上传文件到S3，无需暴露密钥
    """
    try:
        s3 = get_s3_client()
        
        # 生成唯一文件key
        ext = request.filename.split('.')[-1] if '.' in request.filename else ''
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d')
        unique_id = str(uuid.uuid4())[:8]
        file_key = f"{request.prefix}/{user_id}/{timestamp}/{unique_id}.{ext}" if ext else f"{request.prefix}/{user_id}/{timestamp}/{unique_id}"
        
        # 生成预签名上传URL
        expires_in = 3600  # 1小时有效
        presigned_url = s3.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': settings.s3_bucket,
                'Key': file_key,
                'ContentType': request.content_type,
            },
            ExpiresIn=expires_in
        )
        
        # 文件访问URL
        file_url = f"{settings.s3_public_url}/{settings.s3_bucket}/{file_key}"
        
        return PresignedUploadResponse(
            upload_url=presigned_url,
            file_url=file_url,
            file_key=file_key,
            expires_in=expires_in
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成上传URL失败: {str(e)}")


@router.post("/presign/download", response_model=PresignedDownloadResponse)
async def get_presigned_download_url(
    request: PresignedDownloadRequest,
    user_id: str = Depends(get_current_user_id)
):
    """
    获取预签名下载URL
    用于访问私有文件
    """
    try:
        s3 = get_s3_client()
        
        expires_in = 3600  # 1小时有效
        presigned_url = s3.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': settings.s3_bucket,
                'Key': request.file_key,
            },
            ExpiresIn=expires_in
        )
        
        return PresignedDownloadResponse(
            download_url=presigned_url,
            expires_in=expires_in
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成下载URL失败: {str(e)}")


@router.post("/presign/batch-upload")
async def get_batch_presigned_upload_urls(
    request: BatchPresignedRequest,
    user_id: str = Depends(get_current_user_id)
):
    """
    批量获取预签名上传URL
    """
    try:
        s3 = get_s3_client()
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d')
        expires_in = 3600
        
        results = []
        for file_info in request.files:
            filename = file_info.get("filename", "file")
            content_type = file_info.get("content_type", "application/octet-stream")
            
            ext = filename.split('.')[-1] if '.' in filename else ''
            unique_id = str(uuid.uuid4())[:8]
            file_key = f"{request.prefix}/{user_id}/{timestamp}/{unique_id}.{ext}" if ext else f"{request.prefix}/{user_id}/{timestamp}/{unique_id}"
            
            presigned_url = s3.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': settings.s3_bucket,
                    'Key': file_key,
                    'ContentType': content_type,
                },
                ExpiresIn=expires_in
            )
            
            file_url = f"{settings.s3_public_url}/{settings.s3_bucket}/{file_key}"
            
            results.append({
                "filename": filename,
                "upload_url": presigned_url,
                "file_url": file_url,
                "file_key": file_key,
            })
        
        return {
            "files": results,
            "expires_in": expires_in
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"批量生成上传URL失败: {str(e)}")


@router.delete("/file/{file_key:path}")
async def delete_file(
    file_key: str,
    user_id: str = Depends(get_current_user_id)
):
    """
    删除文件
    仅允许删除自己上传的文件
    """
    # 验证文件归属
    if f"/{user_id}/" not in file_key:
        raise HTTPException(status_code=403, detail="无权删除此文件")
    
    try:
        s3 = get_s3_client()
        s3.delete_object(Bucket=settings.s3_bucket, Key=file_key)
        return {"success": True, "message": "文件已删除"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除文件失败: {str(e)}")
