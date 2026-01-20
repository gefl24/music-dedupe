from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime
import json
import asyncio
from . import core

app = FastAPI(title="Music Manager", version="2.0")

# ✅ CORS 配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Pydantic 模型
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

# ✅ WebSocket 连接管理
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
    
    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass

manager = ConnectionManager()

# ✅ 路由
@app.get("/")
async def index():
    return FileResponse("app/templates/index.html")

@app.get("/api/status")
async def get_status():
    """获取应用状态"""
    return {
        "status": core.state.status,
        "progress": core.state.progress,
        "total": core.state.total,
        "message": core.state.message,
        "candidates_count": len(core.state.candidates),
        "results_count": len(core.state.results),
        "config": {
            "has_key": bool(core.state.api_key),
            "masked_key": (core.state.api_key[:4] + "***" + core.state.api_key[-4:]) if core.state.api_key else "",
            "model_name": core.state.model_name,
            "proxy_url": core.state.proxy_url,
            "music_dir": core.state.music_dir,
            "tasks_config": core.state.tasks_config,
            "task_target_path": core.state.task_target_path
        }
    }

@app.get("/api/models")
async def list_models():
    """列出可用的 AI 模型"""
    models = core.state.get_available_models()
    return {"models": models}

@app.get("/api/files")
async def get_all_files():
    """获取所有文件（全量）"""
    return {"files": core.state.files}

@app.get("/api/files/page")
async def get_files_paginated(
    page: int = 1,
    page_size: int = 50,
    search: Optional[str] = None,
    folder: Optional[str] = None
):
    """✅ 分页获取文件列表，支持搜索和过滤"""
    filtered = core.state.files
    
    if folder:
        filtered = [f for f in filtered if f['path'].startswith(folder)]
    
    if search:
        q = search.lower()
        filtered = [f for f in filtered 
                   if q in f['filename'].lower() 
                   or q in f['artist'].lower() 
                   or q in f['title'].lower()]
    
    total = len(filtered)
    start = (page - 1) * page_size
    end = start + page_size
    
    return {
        "files": filtered[start:end],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size
    }

@app.get("/api/dirs")
async def get_dirs(path: Optional[str] = None):
    """获取目录结构"""
    dirs = core.get_dir_structure(path)
    return dirs

@app.get("/api/candidates")
async def get_candidates():
    """获取疑似重复"""
    formatted = []
    for group in core.state.candidates:
        formatted.append({"files": group, "reason": "本地模糊匹配 (疑似)"})
    return {"results": formatted}

@app.post("/api/tasks/config")
async def update_tasks_config(req: TaskConfigRequest):
    """更新任务配置"""
    core.state.tasks_config.update(req.tasks)
    core.state.task_target_path = req.target_path
    core.state.save_config()
    return {"status": "ok"}

@app.post("/api/tasks/run/{task_id}")
async def run_task_manually(task_id: str):
    """手动运行任务"""
    import threading
    threading.Thread(target=core.run_task_wrapper, args=(task_id,)).start()
    return {"status": "started", "task": task_id}

@app.get("/api/tasks/logs")
async def get_task_logs():
    """获取任务日志"""
    return {"logs": core.state.task_logs}

@app.post("/api/update_meta")
async def update_metadata(req: MetadataRequest):
    """批量更新元数据"""
    count = core.batch_update_metadata(req.paths, req.artist, req.album_artist, req.title, req.album)
    return {"status": "ok", "updated": count}

@app.post("/api/fix_meta_single")
async def fix_meta_single(req: SingleFileRequest):
    """使用 AI 修复单个文件的元数据"""
    result = core.fix_single_metadata_ai(req.path)
    if "error" in result:
        return JSONResponse(status_code=500, content=result)
    return result

@app.post("/api/rename")
async def rename_files(req: RenameRequest):
    """批量重命名"""
    count = core.batch_rename_files(req.paths, req.pattern)
    return {"status": "ok", "renamed": count}

@app.post("/api/config")
async def set_config(config: ConfigRequest):
    """保存配置"""
    core.state.api_key = config.api_key.strip()
    core.state.model_name = config.model_name.strip()
    core.state.proxy_url = config.proxy_url.strip() if config.proxy_url else ""
    core.state.save_config()
    return {"status": "ok"}

@app.post("/api/scan")
async def start_scan(req: ScanRequest):
    """启动扫描"""
    if core.state.status != "idle" and core.state.status != "done":
        return JSONResponse(status_code=400, content={"error": "Busy"})
    
    t = __import__('threading').Thread(target=core.task_scan_and_group, args=(req.path,))
    t.start()
    return {"status": "started"}

@app.post("/api/analyze")
async def start_analyze():
    """启动 AI 分析"""
    if not core.state.api_key:
        return JSONResponse(status_code=400, content={"error": "API Key not set"})
    
    t = __import__('threading').Thread(target=core.task_analyze_with_gemini)
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
        if not path.startswith("/music"):
            failed.append(path)
            continue
        if core.delete_file(path):
            deleted.append(path)
        else:
            failed.append(path)
    return {"deleted": deleted, "failed": failed}

# ✅ WebSocket 实时推送
@app.websocket("/ws/progress")
async def websocket_endpoint(websocket: WebSocket):
    """✅ WebSocket 实时推送进度"""
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

# ✅ 健康检查
@app.get("/api/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}
