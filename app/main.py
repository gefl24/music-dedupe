from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from typing import List, Optional
# 使用相对引用防止 ModuleNotFoundError
from . import core

app = FastAPI()

class ConfigRequest(BaseModel):
    api_key: str
    model_name: str
    proxy_url: Optional[str] = ""

class DeleteRequest(BaseModel):
    paths: List[str]

@app.get("/")
async def index():
    # 使用 FileResponse 避免 Jinja2 与 Vue 冲突
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
            # 这里的 Key 是脱敏的，仅用于前端显示状态
            "masked_key": (core.state.api_key[:4] + "***" + core.state.api_key[-4:]) if core.state.api_key else "",
            "model_name": core.state.model_name,
            "proxy_url": core.state.proxy_url
        }
    }

@app.post("/api/config")
async def set_config(config: ConfigRequest):
    # 自动去除首尾空格
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
        # 安全检查
        if not path.startswith("/music"):
            failed.append(path)
            continue
        if core.delete_file(path):
            deleted.append(path)
        else:
            failed.append(path)
    return {"deleted": deleted, "failed": failed}
