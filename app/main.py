import os
import secrets
import threading
import base64
import binascii
from typing import List, Optional, Dict
from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException, status, APIRouter
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel, Field
import asyncio

from . import core

# ========== 环境变量配置 ==========
WEB_USER = os.getenv("WEB_USER", "admin")
WEB_PASSWORD = os.getenv("WEB_PASSWORD", "admin")

security = HTTPBasic()


# ========== 认证 ==========
def get_current_username(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    """验证用户名密码"""
    correct_username = secrets.compare_digest(credentials.username, WEB_USER)
    correct_password = secrets.compare_digest(credentials.password, WEB_PASSWORD)
    
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


# ========== 应用初始化 ==========
app = FastAPI(
    title="Music Manager",
    version="2.0",
    description="AI-powered music file management system"
)

# CORS 中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ========== 请求模型 ==========
class ConfigRequest(BaseModel):
    api_key: str = Field(..., description="Gemini API Key")
    model_name: str = Field(..., description="Model name")
    proxy_url: Optional[str] = Field("", description="HTTP proxy URL")
    dedupe_target_path: Optional[str] = Field("/music", description="Deduplication target path")


class DeleteRequest(BaseModel):
    paths: List[str] = Field(..., description="File paths to delete")


class MetadataRequest(BaseModel):
    paths: List[str] = Field(..., description="File paths")
    artist: Optional[str] = None
    album_artist: Optional[str] = None
    title: Optional[str] = None
    album: Optional[str] = None


class RenameRequest(BaseModel):
    paths: List[str] = Field(..., description="File paths to rename")
    pattern: str = Field(..., description="Rename pattern")


class SingleFileRequest(BaseModel):
    path: str = Field(..., description="File path")


class ScanRequest(BaseModel):
    path: Optional[str] = Field(None, description="Directory path to scan")


class TaskConfigRequest(BaseModel):
    tasks: Dict[str, dict] = Field(..., description="Task configurations")
    target_path: str = Field(..., description="Target directory path")


# ========== WebSocket 连接管理 ==========
class ConnectionManager:
    """WebSocket 连接管理器"""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
    
    async def broadcast(self, message: dict):
        """广播消息给所有连接"""
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                disconnected.append(connection)
        
        # 清理断开的连接
        for conn in disconnected:
            self.disconnect(conn)


manager = ConnectionManager()


# ========== 路由配置 ==========
secure_router = APIRouter(dependencies=[Depends(get_current_username)])


@secure_router.get("/", include_in_schema=False)
async def read_root():
    """返回主页"""
    return FileResponse("app/templates/index.html")


@secure_router.get("/api/dirs")
async def get_dirs(path: Optional[str] = None):
    """获取目录结构"""
    try:
        return core.get_dir_structure(path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@secure_router.get("/api/files")
async def get_files():
    """获取所有文件列表"""
    return {"files": core.state.files}


@secure_router.post("/api/scan")
async def scan_files(req: ScanRequest):
    """扫描文件"""
    t = threading.Thread(
        target=core.task_scan_and_group,
        args=(req.path,),
        daemon=True
    )
    t.start()
    return {"status": "started", "path": req.path or core.state.dedupe_target_path}


@secure_router.get("/api/status")
async def get_status():
    """获取当前状态"""
    config_data = {
        "has_key": bool(core.state.api_key),
        "model_name": core.state.model_name,
        "proxy_url": core.state.proxy_url,
        "music_dir": core.state.music_dir,
        "task_target_path": core.state.task_target_path,
        "dedupe_target_path": core.state.dedupe_target_path,
        "tasks_config": core.state.tasks_config
    }
    
    return {
        "status": core.state.status,
        "progress": core.state.progress,
        "total": core.state.total,
        "message": core.state.message,
        "candidates_count": len(core.state.candidates),
        "results_count": len(core.state.results),
        "config": config_data
    }


@secure_router.get("/api/candidates")
async def get_candidates(page: int = 1, page_size: int = 20):
    """获取疑似重复文件列表 (分页)"""
    all_data = core.state.candidates
    total = len(all_data)
    
    # 计算分页
    start = (page - 1) * page_size
    end = start + page_size
    sliced_data = all_data[start:end]
    
    # 格式化数据
    formatted = [
        {
            "files": group,
            "reason": "本地模糊匹配 (疑似)"
        }
        for group in sliced_data
    ]
    
    return {
        "results": formatted,
        "total": total,
        "page": page,
        "total_pages": (total + page_size - 1) // page_size if page_size > 0 else 1
    }


@secure_router.get("/api/results")
async def get_results(page: int = 1, page_size: int = 20):
    """获取 AI 分析结果 (分页)"""
    all_data = core.state.results
    total = len(all_data)
    
    start = (page - 1) * page_size
    end = start + page_size
    sliced_data = all_data[start:end]
    
    return {
        "results": sliced_data,
        "total": total,
        "page": page,
        "total_pages": (total + page_size - 1) // page_size if page_size > 0 else 1
    }


@secure_router.post("/api/config")
async def save_config(req: ConfigRequest):
    """保存配置"""
    core.state.api_key = req.api_key
    core.state.model_name = req.model_name
    core.state.proxy_url = req.proxy_url
    
    if req.dedupe_target_path:
        core.state.dedupe_target_path = req.dedupe_target_path
    
    core.state.save_config()
    return {"status": "ok"}


@secure_router.get("/api/models")
async def list_models():
    """获取可用模型列表"""
    models = core.state.get_available_models()
    return {"models": models}


@secure_router.post("/api/tasks/config")
async def save_tasks_config(req: TaskConfigRequest):
    """保存任务配置"""
    core.state.tasks_config = req.tasks
    core.state.task_target_path = req.target_path
    core.state.save_config()
    return {"status": "ok"}


@secure_router.post("/api/tasks/run/{task_id}")
async def run_manual_task(task_id: str):
    """手动运行任务"""
    t = threading.Thread(
        target=core.run_task_wrapper,
        args=(task_id,),
        daemon=True
    )
    t.start()
    return {"status": "started", "task": task_id}


@secure_router.get("/api/tasks/logs")
async def get_task_logs():
    """获取任务日志"""
    return {"logs": core.state.task_logs}


@secure_router.post("/api/update_meta")
async def update_metadata(req: MetadataRequest):
    """批量更新元数据"""
    count = core.batch_update_metadata(
        req.paths,
        req.artist,
        req.album_artist,
        req.title,
        req.album
    )
    return {"updated": count}


@secure_router.post("/api/rename")
async def rename_files(req: RenameRequest):
    """批量重命名文件"""
    count = core.batch_rename_files(req.paths, req.pattern)
    return {"renamed": count}


@secure_router.post("/api/fix_meta_single")
async def fix_meta_single(req: SingleFileRequest):
    """使用 AI 修复单个文件元数据"""
    result = core.fix_single_metadata_ai(req.path)
    return result


@secure_router.post("/api/analyze")
async def analyze_duplicates():
    """启动 AI 分析"""
    t = threading.Thread(
        target=core.task_analyze_with_gemini,
        daemon=True
    )
    t.start()
    return {"status": "started"}


@secure_router.post("/api/delete")
async def delete_files(req: DeleteRequest):
    """批量删除文件"""
    deleted = []
    failed = []
    
    for path in req.paths:
        if core.delete_file(path):
            deleted.append(path)
        else:
            failed.append(path)
    
    return {"deleted": deleted, "failed": failed}


# 注册安全路由
app.include_router(secure_router)


# ========== WebSocket ==========
@app.websocket("/ws/progress")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket 进度推送"""
    await manager.connect(websocket)
    
    try:
        while True:
            await asyncio.sleep(0.5)
            
            message = {
                "status": core.state.status,
                "progress": core.state.progress,
                "total": core.state.total,
                "message": core.state.message,
                "candidates_count": len(core.state.candidates),
                "results_count": len(core.state.results),
                "timestamp": datetime.now().isoformat()
            }
            
            await websocket.send_json(message)
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        core.logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)


# ========== 健康检查 ==========
@app.get("/api/health")
async def health_check():
    """健康检查端点"""
    return {
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "db_connected": True
    }


# ========== 启动事件 ==========
@app.on_event("startup")
async def startup_event():
    """应用启动时执行"""
    core.logger.info("Music Manager API started")


@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭时执行"""
    core.logger.info("Music Manager API shutting down")
    core.state.scheduler.shutdown()
    core.state.executor.shutdown(wait=False)
