import os
import secrets
import threading # ✅ 1. 修复: 补全 threading 引用
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

# ✅ 2. 认证函数 (仅用于 HTTP 请求)
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

# ✅ 3. 创建受保护的 API 路由组
# 所有 API 和静态页面都通过此 router 加载，自动应用 Basic Auth
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

@secure_router.get("/api/candidates")
async def get_candidates():
    return {"results": core.state.candidates}

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

@secure_router.get("/api/results")
async def get_results():
    return {"results": core.state.results}

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

# 将受保护的路由注册到 App
app.include_router(secure_router)

# ✅ 4. WebSocket 路由 (不添加 Depends 依赖，解决 TypeError 和 闪烁)
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
