from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from typing import List

# 修复点 1: 使用相对引用导入同目录下的 core 模块
# 解决 "ModuleNotFoundError: No module named 'core'"
from . import core

app = FastAPI()

# 定义请求数据模型
class ConfigRequest(BaseModel):
    api_key: str

class DeleteRequest(BaseModel):
    paths: List[str]

@app.get("/")
async def index():
    # 修复点 2: 使用 FileResponse 直接返回 HTML 文件
    # 解决 "jinja2.exceptions.UndefinedError: 'status' is undefined"
    # 因为这是一个 Vue 单页应用，不需要后端 Jinja2 渲染，直接当静态文件返回即可
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
        # 安全检查：确保路径在 /music 下，防止误删系统文件
        if not path.startswith("/music"):
            failed.append(path)
            continue
            
        if core.delete_file(path):
            deleted.append(path)
        else:
            failed.append(path)
    return {"deleted": deleted, "failed": failed}
