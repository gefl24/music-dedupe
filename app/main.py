from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List
import core

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")

# Pydantic models
class ConfigRequest(BaseModel):
    api_key: str

class DeleteRequest(BaseModel):
    paths: List[str]

@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/status")
async def get_status():
    return {
        "status": core.state.status,
        "progress": core.state.progress,
        "total": core.state.total,
        "message": core.state.message,
        "candidates_count": len(core.state.candidates),
        "results_count": len(core.state.results),
        "has_key": bool(core.state.api_key)
    }

@app.post("/api/config")
async def set_config(config: ConfigRequest):
    core.state.api_key = config.api_key
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
        # 安全检查：确保路径在 /music 下
        if not path.startswith("/music"):
            failed.append(path)
            continue
            
        if core.delete_file(path):
            deleted.append(path)
        else:
            failed.append(path)
    return {"deleted": deleted, "failed": failed}
