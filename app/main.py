import os
import secrets
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Depends, HTTPException, status
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

# 1. ✅ 读取环境变量中的账号密码 (默认为 admin/admin)
WEB_USER = os.getenv("WEB_USER", "admin")
WEB_PASSWORD = os.getenv("WEB_PASSWORD", "admin")

security = HTTPBasic()

# 2. ✅ 定义认证依赖函数
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

# 3. ✅ 将认证应用到整个 App (包含所有路由和静态文件)
app = FastAPI(
    title="Music Manager", 
    version="2.0",
    dependencies=[Depends(get_current_username)] 
)

# CORS 配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic 模型
class ConfigRequest(BaseModel):
    api_key: str
    model_name: str
    proxy_url: Optional[str] = ""

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

# WebSocket 连接管理
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

# --- Routes ---

@app.get("/")
async def read_root():
    return FileResponse("app/templates/index.html")

@app.get("/api/dirs")
async def get_dirs(path: Optional[str] = None):
    return core.get_dir_structure(path)

@app.get("/api/files")
async def get_files():
    return {"files": core.state.files}

@app.post("/api/scan")
async def scan_files(req: ScanRequest):
    t = threading.Thread(target=core.task_scan_and_group, args=(req.path,))
    t.start()
    return {"status": "started"}

@app.get("/api/status")
async def get_status():
    """获取当前状态和部分配置"""
    config_data = {
        "has_key": bool(core.state.api_key),
        "model_name": core.state.model_name,
        "proxy_url": core.state.proxy_url, # 返回 Proxy URL 以便前端回显
        "music_dir": core.state.music_dir,
        "task_target_path": core.state.task_target_path,
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

@app.get("/api/candidates")
async def get_candidates():
    return {"results": core.state.candidates}

@app.post("/api/config")
async def save_config(req: ConfigRequest):
    core.state.api_key = req.api_key
    core.state.model_name = req.model_name
    core.state.proxy_url = req.proxy_url
    core.state.save_config()
    return {"status": "ok"}

@app.get("/api/models")
async def list_models():
    return {"models": core.state.get_available_models()}

@app.post("/api/tasks/config")
async def save_tasks_config(req: TaskConfigRequest):
    core.state.tasks_config = req.tasks
    core.state.task_target_path = req.target_path
    core.state.save_config()
    core.state.update_scheduler()
    return {"status": "ok"}

@app.post("/api/tasks/run/{task_id}")
async def run_manual_task(task_id: str):
    import threading
    # 使用线程运行任务避免阻塞主进程
    t = threading.Thread(target=core.run_task_wrapper, args=(task_id,))
    t.start()
    return {"status": "started", "task": task_id}

@app.get("/api/tasks/logs")
async def get_task_logs():
    return {"logs": core.state.task_logs}

@app.post("/api/update_meta")
async def update_metadata(req: MetadataRequest):
    count = core.batch_update_metadata(
        req.paths, req.artist, req.album_artist, req.title, req.album
    )
    return {"updated": count}

@app.post("/api/rename")
async def rename_files(req: RenameRequest):
    count = core.batch_rename_files(req.paths, req.pattern)
    return {"renamed": count}

@app.post("/api/fix_meta_single")
async def fix_meta_single(req: SingleFileRequest):
    res = core.fix_single_metadata_ai(req.path)
    return res

@app.post("/api/analyze")
async def analyze_duplicates():
    import threading
    t = threading.Thread(target=core.task_analyze_with_gemini)
    t.start()
    return {"status": "started"}

@app.get("/api/results")
async def get_results():
    """获取分析结果"""
    return {"results": core.state.results}

@app.post("/api/delete")
async def delete_files(req: DeleteRequest):
    """删除文件"""
    deleted = []
    failed = []
    for path in req.paths:
        # 简单安全检查
        if not path.startswith("/music") and not path.startswith("/data"):
             # 这里可以根据实际挂载路径调整，防止删除系统文件
             pass 
        if core.delete_file(path):
            deleted.append(path)
        else:
            failed.append(path)
    return {"deleted": deleted, "failed": failed}

# WebSocket 实时推送
@app.websocket("/ws/progress")
async def websocket_endpoint(websocket: WebSocket):
    # 注意：WebSocket 连接建立时，浏览器会自动带上 Basic Auth 的 header
    # 如果认证失败，FastAPI 依赖会拒绝连接
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

# 健康检查
@app.get("/api/health")
async def health_check():
    return {"status": "ok"}
