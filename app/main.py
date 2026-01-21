import os
import secrets
import threading
import base64
import binascii
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

# 读取环境变量
WEB_USER = os.getenv("WEB_USER", "admin")
WEB_PASSWORD = os.getenv("WEB_PASSWORD", "admin")

security = HTTPBasic()

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
    dedupe_target_path: Optional[str] = "/music"  # ✅ 新增配置项

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

# WebSocket
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

@app.get("/", dependencies=[Depends(get_current_username)])
async def read_root():
    return FileResponse("app/templates/index.html")

@app.get("/api/dirs", dependencies=[Depends(get_current_username)])
async def get_dirs(path: Optional[str] = None):
    return core.get_dir_structure(path)

@app.get("/api/files", dependencies=[Depends(get_current_username)])
async def get_files():
    return {"files": core.state.files}

@app.post("/api/scan", dependencies=[Depends(get_current_username)])
async def scan_files(req: ScanRequest):
    t = threading.Thread(target=core.task_scan_and_group, args=(req.path,))
    t.start()
    return {"status": "started"}

@app.get("/api/status", dependencies=[Depends(get_current_username)])
async def get_status():
    config_data = {
        "has_key": bool(core.state.api_key),
        "model_name": core.state.model_name,
        "proxy_url": core.state.proxy_url,
        "music_dir": core.state.music_dir,
        "task_target_path": core.state.task_target_path,
        "dedupe_target_path": core.state.dedupe_target_path, # ✅ 返回去重路径
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

@app.get("/api/candidates", dependencies=[Depends(get_current_username)])
async def get_candidates():
    return {"results": core.state.candidates}

@app.post("/api/config", dependencies=[Depends(get_current_username)])
async def save_config(req: ConfigRequest):
    core.state.api_key = req.api_key
    core.state.model_name = req.model_name
    core.state.proxy_url = req.proxy_url
    if req.dedupe_target_path:
        core.state.dedupe_target_path = req.dedupe_target_path
    core.state.save_config()
    return {"status": "ok"}

@app.get("/api/models", dependencies=[Depends(get_current_username)])
async def list_models():
    return {"models": core.state.get_available_models()}

@app.post("/api/tasks/config", dependencies=[Depends(get_current_username)])
async def save_tasks_config(req: TaskConfigRequest):
    core.state.tasks_config = req.tasks
    core.state.task_target_path = req.target_path
    core.state.save_config()
    core.state.update_scheduler()
    return {"status": "ok"}

@app.post("/api/tasks/run/{task_id}", dependencies=[Depends(get_current_username)])
async def run_manual_task(task_id: str):
    t = threading.Thread(target=core.run_task_wrapper, args=(task_id,))
    t.start()
    return {"status": "started", "task": task_id}

@app.get("/api/tasks/logs", dependencies=[Depends(get_current_username)])
async def get_task_logs():
    return {"logs": core.state.task_logs}

@app.post("/api/update_meta", dependencies=[Depends(get_current_username)])
async def update_metadata(req: MetadataRequest):
    count = core.batch_update_metadata(
        req.paths, req.artist, req.album_artist, req.title, req.album
    )
    return {"updated": count}

@app.post("/api/rename", dependencies=[Depends(get_current_username)])
async def rename_files(req: RenameRequest):
    count = core.batch_rename_files(req.paths, req.pattern)
    return {"renamed": count}

@app.post("/api/fix_meta_single", dependencies=[Depends(get_current_username)])
async def fix_meta_single(req: SingleFileRequest):
    res = core.fix_single_metadata_ai(req.path)
    return res

@app.post("/api/analyze", dependencies=[Depends(get_current_username)])
async def analyze_duplicates():
    t = threading.Thread(target=core.task_analyze_with_gemini)
    t.start()
    return {"status": "started"}

@app.get("/api/results", dependencies=[Depends(get_current_username)])
async def get_results():
    return {"results": core.state.results}

@app.post("/api/delete", dependencies=[Depends(get_current_username)])
async def delete_files(req: DeleteRequest):
    deleted = []
    failed = []
    for path in req.paths:
        if core.delete_file(path):
            deleted.append(path)
        else:
            failed.append(path)
    return {"deleted": deleted, "failed": failed}

@app.websocket("/ws/progress")
async def websocket_endpoint(websocket: WebSocket):
    if not check_websocket_auth(websocket):
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

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
