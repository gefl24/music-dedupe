import os
import secrets
import threading
import base64
import binascii
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Depends, HTTPException, status, APIRouter
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime
import json
import asyncio
from . import core

# 环境变量设置账号密码 (默认 admin/admin)
WEB_USER = os.getenv("WEB_USER", "admin")
WEB_PASSWORD = os.getenv("WEB_PASSWORD", "admin")

security = HTTPBasic()

# 认证函数
def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = secrets.compare_digest(credentials.username, WEB_USER)
    correct_password = secrets.compare_digest(credentials.password, WEB_PASSWORD)
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

app = FastAPI(title="Music Manager", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Models ---
class ConfigRequest(BaseModel):
    api_key: str
    model_name: str
    proxy_url: Optional[str] = ""
    dedupe_target_path: Optional[str] = "/music"

class DeleteRequest(BaseModel):
    paths: List[str]

class MetadataRequest(BaseModel):
    paths: List[str]
    artist: Optional[str] = None
    album_artist: Optional[str] = None
    title: Optional[str] = None
    album: Optional[str] = None

class RenameRequest(BaseModel):
    paths: List[str]
    pattern: str

class SingleFileRequest(BaseModel):
    path: str

class ScanRequest(BaseModel):
    path: Optional[str] = None

class TaskConfigRequest(BaseModel):
    tasks: Dict[str, dict]
    target_path: str

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

manager = ConnectionManager()

# --- 路由配置 ---
secure_router = APIRouter(dependencies=[Depends(get_current_username)])

@secure_router.get("/")
async def read_root():
    return FileResponse("app/templates/index.html")

@secure_router.get("/api/dirs")
async def get_dirs(path: Optional[str] = None):
    return core.get_dir_structure(path)

@secure_router.get("/api/files")
async def get_files():
    return {"files": core.state.files}

@secure_router.post("/api/scan")
async def scan_files(req: ScanRequest):
    t = threading.Thread(target=core.task_scan_and_group, args=(req.path,))
    t.start()
    return {"status": "started"}

@secure_router.get("/api/status")
async def get_status():
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

# ✅ 修复: 增加分页 + 格式化输出 (解决列表显示空白问题)
@secure_router.get("/api/candidates")
async def get_candidates(page: int = 1, page_size: int = 20):
    all_data = core.state.candidates
    total = len(all_data)
    
    # 计算分页切片
    start = (page - 1) * page_size
    end = start + page_size
    sliced_data = all_data[start:end]
    
    # 格式化数据以匹配前端结构
    formatted = []
    for group in sliced_data:
        formatted.append({
            "files": group,
            "reason": "本地模糊匹配 (疑似)"
        })
        
    return {
        "results": formatted,
        "total": total,
        "page": page,
        "total_pages": (total + page_size - 1) // page_size if page_size > 0 else 1
    }

@secure_router.get("/api/results")
async def get_results(page: int = 1, page_size: int = 20):
    all_data = core.state.results
    total = len(all_data)
    
    start = (page - 1) * page_size
    end = start + page_size
    sliced_data = all_data[start:end]
    
    # AI 结果通常已经是格式化好的，但也支持分页
    return {
        "results": sliced_data,
        "total": total,
        "page": page,
        "total_pages": (total + page_size - 1) // page_size if page_size > 0 else 1
    }

@secure_router.post("/api/config")
async def save_config(req: ConfigRequest):
    core.state.api_key = req.api_key
    core.state.model_name = req.model_name
    core.state.proxy_url = req.proxy_url
    if req.dedupe_target_path:
        core.state.dedupe_target_path = req.dedupe_target_path
    core.state.save_config()
    return {"status": "ok"}

@secure_router.get("/api/models")
async def list_models():
    return {"models": core.state.get_available_models()}

@secure_router.post("/api/tasks/config")
async def save_tasks_config(req: TaskConfigRequest):
    core.state.tasks_config = req.tasks
    core.state.task_target_path = req.target_path
    core.state.save_config()
    core.state.update_scheduler()
    return {"status": "ok"}

@secure_router.post("/api/tasks/run/{task_id}")
async def run_manual_task(task_id: str):
    t = threading.Thread(target=core.run_task_wrapper, args=(task_id,))
    t.start()
    return {"status": "started", "task": task_id}

@secure_router.get("/api/tasks/logs")
async def get_task_logs():
    return {"logs": core.state.task_logs}

@secure_router.post("/api/update_meta")
async def update_metadata(req: MetadataRequest):
    count = core.batch_update_metadata(
        req.paths, req.artist, req.album_artist, req.title, req.album
    )
    return {"updated": count}

@secure_router.post("/api/rename")
async def rename_files(req: RenameRequest):
    count = core.batch_rename_files(req.paths, req.pattern)
    return {"renamed": count}

@secure_router.post("/api/fix_meta_single")
async def fix_meta_single(req: SingleFileRequest):
    res = core.fix_single_metadata_ai(req.path)
    return res

@secure_router.post("/api/analyze")
async def analyze_duplicates():
    t = threading.Thread(target=core.task_analyze_with_gemini)
    t.start()
    return {"status": "started"}

@secure_router.post("/api/delete")
async def delete_files(req: DeleteRequest):
    deleted = []
    failed = []
    for path in req.paths:
        if core.delete_file(path):
            deleted.append(path)
        else:
            failed.append(path)
    return {"deleted": deleted, "failed": failed}

app.include_router(secure_router)

# WebSocket 辅助函数
def check_websocket_auth(websocket: WebSocket) -> bool:
    auth_header = websocket.headers.get("authorization")
    if not auth_header or not auth_header.startswith("Basic "):
        return False
    try:
        encoded_creds = auth_header.split(" ")[1]
        decoded_bytes = base64.b64decode(encoded_creds)
        decoded_str = decoded_bytes.decode("utf-8")
        username, password = decoded_str.split(":", 1)
        
        is_user_ok = secrets.compare_digest(username, WEB_USER)
        is_pass_ok = secrets.compare_digest(password, WEB_PASSWORD)
        return is_user_ok and is_pass_ok
    except (binascii.Error, ValueError, UnicodeDecodeError):
        return False

@app.websocket("/ws/progress")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await asyncio.sleep(0.5)
            await websocket.send_json({
                "status": core.state.status,
                "progress": core.state.progress,
                "total": core.state.total,
                "message": core.state.message,
                "candidates_count": len(core.state.candidates),
                "results_count": len(core.state.results),
                "timestamp": datetime.now().isoformat()
            })
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        manager.disconnect(websocket)

@app.get("/api/health")
async def health_check():
    return {"status": "ok"}
