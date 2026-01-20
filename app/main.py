from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from typing import List, Optional
from . import core

app = FastAPI()

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

@app.get("/")
async def index():
    return FileResponse("app/templates/index.html")

@app.get("/api/status")
async def get_status():
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
            "proxy_url": core.state.proxy_url
        }
    }

@app.get("/api/models")
async def list_models():
    models = core.state.get_available_models()
    return {"models": models}

@app.get("/api/files")
async def get_all_files():
    return {"files": core.state.files}

# ✅ 新增：获取扫描后的疑似列表
@app.get("/api/candidates")
async def get_candidates():
    # 为了让前端统一格式展示，我们将 candidates (list of lists) 包装成 results 的格式
    formatted = []
    for group in core.state.candidates:
        formatted.append({
            "files": group,
            "reason": "本地模糊匹配 (疑似)"
        })
    return {"results": formatted}

@app.post("/api/update_meta")
async def update_metadata(req: MetadataRequest):
    count = core.batch_update_metadata(req.paths, req.artist, req.album_artist, req.title, req.album)
    return {"status": "ok", "updated": count}

@app.post("/api/fix_meta_single")
async def fix_meta_single(req: SingleFileRequest):
    result = core.fix_single_metadata_ai(req.path)
    if "error" in result:
        return JSONResponse(status_code=500, content=result)
    return result

@app.post("/api/rename")
async def rename_files(req: RenameRequest):
    count = core.batch_rename_files(req.paths, req.pattern)
    return {"status": "ok", "renamed": count}

@app.post("/api/config")
async def set_config(config: ConfigRequest):
    core.state.api_key = config.api_key.strip()
    core.state.model_name = config.model_name.strip()
    core.state.proxy_url = config.proxy_url.strip() if config.proxy_url else ""
    core.state.save_config()
    return {"status": "ok"}

@app.post("/api/scan")
async def start_scan():
    if core.state.status != "idle" and core.state.status != "done":
        return JSONResponse(status_code=400, content={"error": "Busy"})
    core.start_scan_thread()
    return {"status": "started"}

@app.post("/api/analyze")
async def start_analyze():
    if not core.state.api_key:
        return JSONResponse(status_code=400, content={"error": "API Key not set"})
    core.start_analyze_thread()
    return {"status": "started"}

@app.get("/api/results")
async def get_results():
    return {"results": core.state.results}

@app.post("/api/delete")
async def delete_files(req: DeleteRequest):
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
