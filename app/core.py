import os
import json
import threading
import asyncio
import time
import sqlite3
import gc
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Generator
from dataclasses import dataclass, asdict
from datetime import datetime
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from logging.handlers import RotatingFileHandler

warnings.filterwarnings('ignore', category=FutureWarning, module='google.generativeai')
warnings.filterwarnings('ignore', category=UserWarning, module='google.generativeai')

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import google.generativeai as genai

from mutagen.easyid3 import EasyID3
from mutagen.mp3 import MP3
from mutagen.flac import FLAC
from mutagen.id3 import ID3NoHeaderError
from thefuzz import fuzz

# ========== 常量定义 ==========
DATA_DIR = Path("/data")
CONFIG_FILE = DATA_DIR / "config.json"
DB_FILE = DATA_DIR / "metadata.db"
LOG_FILE = DATA_DIR / "app.log"

SUPPORTED_FORMATS = ('.mp3', '.flac', '.m4a', '.wma')
MUSIC_EXTS = {'.mp3', '.flac', '.wav', '.m4a', '.wma', '.ape', '.ogg'}
JUNK_EXTS = {'.nfo', '.jpg', '.jpeg', '.png', '.lrc', '.txt'}

BATCH_SIZE = 100
MAX_WORKERS = 4
AI_BATCH_SIZE = 3
AI_SLEEP_INTERVAL = 1


# ========== 数据类 ==========
@dataclass
class FileMetadata:
    """文件元数据数据类"""
    path: str
    filename: str
    artist: str = ""
    title: str = ""
    album: str = ""
    album_artist: str = ""
    duration: int = 0
    size_mb: float = 0.0
    bitrate: int = 0
    search_text: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


# ========== 日志管理器 ==========
class Logger:
    """统一日志管理器"""
    
    def __init__(self, log_file: Path):
        self.logger = logging.getLogger("MusicManager")
        self.logger.setLevel(logging.INFO)
        
        handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding='utf-8'
        )
        formatter = logging.Formatter(
            '[%(asctime)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
    
    def info(self, msg: str):
        self.logger.info(msg)
    
    def error(self, msg: str):
        self.logger.error(msg)
    
    def warning(self, msg: str):
        self.logger.warning(msg)


logger = Logger(LOG_FILE)


# ========== 数据库管理器 ==========
class MetadataDB:
    """元数据数据库管理器 - 优化版"""
    
    def __init__(self, db_path: Path = DB_FILE):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """初始化数据库表和索引"""
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata (
                    path TEXT PRIMARY KEY,
                    filename TEXT NOT NULL,
                    artist TEXT,
                    title TEXT,
                    album TEXT,
                    album_artist TEXT,
                    duration INTEGER DEFAULT 0,
                    size_mb REAL DEFAULT 0,
                    bitrate INTEGER DEFAULT 0,
                    search_text TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 创建索引
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_filename ON metadata(filename)",
                "CREATE INDEX IF NOT EXISTS idx_artist ON metadata(artist)",
                "CREATE INDEX IF NOT EXISTS idx_search ON metadata(search_text)",
                "CREATE INDEX IF NOT EXISTS idx_updated ON metadata(updated_at)"
            ]
            for idx_sql in indexes:
                conn.execute(idx_sql)
            
            conn.commit()
    
    @contextmanager
    def _get_conn(self):
        """数据库连接上下文管理器"""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA cache_size = -64000")  # 64MB cache
        try:
            yield conn
        finally:
            conn.close()
    
    def save_metadata(self, meta: Dict):
        """保存单个元数据"""
        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO metadata 
                (path, filename, artist, title, album, album_artist, 
                 duration, size_mb, bitrate, search_text, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                meta['path'], meta['filename'], meta['artist'], meta['title'],
                meta['album'], meta['album_artist'], meta['duration'],
                meta['size_mb'], meta['bitrate'], meta['search_text']
            ))
            conn.commit()
    
    def batch_save(self, metadata_list: List[Dict]):
        """批量保存元数据 - 使用事务优化"""
        if not metadata_list:
            return
        
        with self._get_conn() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                for meta in metadata_list:
                    conn.execute("""
                        INSERT OR REPLACE INTO metadata 
                        (path, filename, artist, title, album, album_artist, 
                         duration, size_mb, bitrate, search_text, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (
                        meta['path'], meta['filename'], meta['artist'], meta['title'],
                        meta['album'], meta['album_artist'], meta['duration'],
                        meta['size_mb'], meta['bitrate'], meta['search_text']
                    ))
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Batch save failed: {e}")
                raise
    
    def get_all(self, limit: Optional[int] = None, offset: int = 0) -> List[dict]:
        """获取所有元数据"""
        with self._get_conn() as conn:
            if limit:
                sql = "SELECT * FROM metadata ORDER BY filename LIMIT ? OFFSET ?"
                rows = conn.execute(sql, (limit, offset)).fetchall()
            else:
                sql = "SELECT * FROM metadata ORDER BY filename"
                rows = conn.execute(sql).fetchall()
            return [dict(row) for row in rows]
    
    def get_count(self) -> int:
        """获取总数"""
        with self._get_conn() as conn:
            return conn.execute("SELECT COUNT(*) as cnt FROM metadata").fetchone()['cnt']
    
    def search(self, query: str, limit: int = 50, offset: int = 0) -> List[dict]:
        """搜索元数据"""
        with self._get_conn() as conn:
            q = f"%{query.lower()}%"
            sql = """
                SELECT * FROM metadata 
                WHERE filename LIKE ? OR artist LIKE ? OR title LIKE ? 
                ORDER BY filename LIMIT ? OFFSET ?
            """
            rows = conn.execute(sql, (q, q, q, limit, offset)).fetchall()
            return [dict(row) for row in rows]
    
    def delete_by_path(self, path: str):
        """删除指定路径的记录"""
        with self._get_conn() as conn:
            conn.execute("DELETE FROM metadata WHERE path = ?", (path,))
            conn.commit()
    
    def delete_batch(self, paths: List[str]):
        """批量删除"""
        if not paths:
            return
        
        with self._get_conn() as conn:
            placeholders = ','.join('?' * len(paths))
            conn.execute(f"DELETE FROM metadata WHERE path IN ({placeholders})", paths)
            conn.commit()
    
    def clear_all(self):
        """清空所有数据"""
        with self._get_conn() as conn:
            conn.execute("DELETE FROM metadata")
            conn.commit()
    
    def optimize(self):
        """优化数据库"""
        with self._get_conn() as conn:
            conn.execute("VACUUM")
            conn.execute("ANALYZE")


meta_db = MetadataDB()


# ========== 配置管理器 ==========
class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config_file: Path):
        self.config_file = config_file
        self.config = self._load_default_config()
    
    def _load_default_config(self) -> dict:
        """加载默认配置"""
        return {
            "api_key": "",
            "model_name": "gemini-1.5-flash",
            "proxy_url": "",
            "music_dir": "/music",
            "task_target_path": "/music",
            "dedupe_target_path": "/music",
            "tasks_config": {
                "dedupe_quality": {
                    "enabled": False,
                    "cron": "0 2 * * *",
                    "last_run": None
                },
                "clean_short": {
                    "enabled": False,
                    "cron": "0 3 * * *",
                    "min_duration": 60,
                    "last_run": None
                },
                "extract_meta": {
                    "enabled": False,
                    "cron": "0 4 * * *",
                    "last_run": None
                },
                "clean_junk": {
                    "enabled": False,
                    "cron": "0 5 * * *",
                    "last_run": None
                }
            }
        }
    
    def load(self):
        """从文件加载配置"""
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                    # 深度合并配置
                    self._merge_config(self.config, loaded)
            except Exception as e:
                logger.error(f"Error loading config: {e}")
    
    def _merge_config(self, base: dict, update: dict):
        """递归合并配置"""
        for key, value in update.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._merge_config(base[key], value)
            else:
                base[key] = value
    
    def save(self):
        """保存配置到文件"""
        try:
            self.config_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error saving config: {e}")
    
    def get(self, key: str, default=None):
        """获取配置项"""
        return self.config.get(key, default)
    
    def set(self, key: str, value):
        """设置配置项"""
        self.config[key] = value


# ========== 音频元数据提取器 ==========
class AudioMetadataExtractor:
    """音频元数据提取器"""
    
    @staticmethod
    def extract(path: str) -> FileMetadata:
        """提取音频文件元数据"""
        filename = os.path.basename(path)
        
        # 获取文件大小
        try:
            size_mb = round(os.path.getsize(path) / (1024 * 1024), 2)
        except:
            size_mb = 0.0
        
        # 初始化
        tags = {}
        duration = 0
        bitrate = 0
        
        # 提取音频信息
        try:
            ext = path.lower()
            if ext.endswith('.mp3'):
                audio = AudioMetadataExtractor._read_mp3(path)
                tags = audio
                if audio.info:
                    duration = int(audio.info.length) if audio.info.length else 0
                    bitrate = int(audio.info.bitrate / 1000) if audio.info.bitrate else 0
            elif ext.endswith('.flac'):
                audio = FLAC(path)
                tags = audio
                if audio.info:
                    duration = int(audio.info.length) if audio.info.length else 0
                    bitrate = int(audio.info.bitrate / 1000) if audio.info.bitrate else 0
        except Exception as e:
            logger.warning(f"Failed to read tags from {filename}: {e}")
        
        # 提取标签
        artist = AudioMetadataExtractor._get_tag(tags, 'artist')
        title = AudioMetadataExtractor._get_tag(tags, 'title')
        album = AudioMetadataExtractor._get_tag(tags, 'album')
        album_artist = AudioMetadataExtractor._get_tag(tags, 'albumartist')
        
        # 从文件名推断标题
        if not title:
            title = AudioMetadataExtractor._infer_title_from_filename(filename, artist)
        
        # 构建搜索文本
        search_text = f"{artist} {title} {filename}".lower()
        
        return FileMetadata(
            path=path,
            filename=filename,
            artist=artist.strip(),
            title=title.strip(),
            album=album.strip(),
            album_artist=album_artist.strip(),
            duration=duration,
            size_mb=size_mb,
            bitrate=bitrate,
            search_text=search_text
        )
    
    @staticmethod
    def _read_mp3(path: str):
        """读取 MP3 文件"""
        try:
            return MP3(path, ID3=EasyID3)
        except ID3NoHeaderError:
            audio = MP3(path)
            audio.add_tags()
            return audio
    
    @staticmethod
    def _get_tag(tags: dict, key: str) -> str:
        """从标签字典中获取标签值"""
        values = tags.get(key, [])
        valid_values = [str(v).strip() for v in values if v]
        return " / ".join(valid_values) if valid_values else ""
    
    @staticmethod
    def _infer_title_from_filename(filename: str, artist: str) -> str:
        """从文件名推断标题"""
        base = os.path.splitext(filename)[0]
        if " - " in base:
            parts = base.split(" - ", 1)
            return parts[1] if artist else parts[1]
        return base
        # ========== 应用状态管理器 ==========
class AppState:
    """应用状态管理器 - 优化版"""
    
    def __init__(self):
        # 配置管理
        self.config_manager = ConfigManager(CONFIG_FILE)
        self.config_manager.load()
        
        # 状态属性
        self.status = "idle"
        self.progress = 0
        self.total = 0
        self.message = "准备就绪"
        
        # 数据存储
        self.files: List[dict] = []
        self.candidates: List[List[dict]] = []
        self.results: List[dict] = []
        self.task_logs: List[str] = []
        
        # 应用代理设置
        self._apply_proxy()
        
        # 初始化调度器
        self.scheduler = BackgroundScheduler()
        self._update_scheduler()
        self.scheduler.start()
        
        # 线程池
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        
        logger.info("AppState initialized successfully")
    
    @property
    def api_key(self) -> str:
        return self.config_manager.get("api_key", "")
    
    @api_key.setter
    def api_key(self, value: str):
        self.config_manager.set("api_key", value)
    
    @property
    def model_name(self) -> str:
        return self.config_manager.get("model_name", "gemini-1.5-flash")
    
    @model_name.setter
    def model_name(self, value: str):
        self.config_manager.set("model_name", value)
    
    @property
    def proxy_url(self) -> str:
        return self.config_manager.get("proxy_url", "")
    
    @proxy_url.setter
    def proxy_url(self, value: str):
        self.config_manager.set("proxy_url", value)
    
    @property
    def music_dir(self) -> str:
        return self.config_manager.get("music_dir", "/music")
    
    @property
    def task_target_path(self) -> str:
        return self.config_manager.get("task_target_path", "/music")
    
    @task_target_path.setter
    def task_target_path(self, value: str):
        self.config_manager.set("task_target_path", value)
    
    @property
    def dedupe_target_path(self) -> str:
        return self.config_manager.get("dedupe_target_path", "/music")
    
    @dedupe_target_path.setter
    def dedupe_target_path(self, value: str):
        self.config_manager.set("dedupe_target_path", value)
    
    @property
    def tasks_config(self) -> dict:
        return self.config_manager.get("tasks_config", {})
    
    @tasks_config.setter
    def tasks_config(self, value: dict):
        self.config_manager.set("tasks_config", value)
    
    def log(self, msg: str):
        """添加日志"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        entry = f"[{timestamp}] {msg}"
        print(entry)
        logger.info(msg)
        
        self.task_logs.insert(0, entry)
        if len(self.task_logs) > 200:
            self.task_logs = self.task_logs[:200]
    
    def save_config(self):
        """保存配置"""
        self.config_manager.save()
        self._apply_proxy()
        self._update_scheduler()
    
    def _apply_proxy(self):
        """应用代理设置"""
        proxy = self.proxy_url
        env_keys = ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']
        
        if proxy:
            for key in env_keys:
                os.environ[key] = proxy
        else:
            for key in env_keys:
                os.environ.pop(key, None)
    
    def get_available_models(self) -> List[str]:
        """获取可用模型列表"""
        if not self.api_key:
            return []
        
        self._apply_proxy()
        try:
            genai.configure(api_key=self.api_key)
            models = []
            for m in genai.list_models():
                if 'generateContent' in m.supported_generation_methods:
                    name = m.name.replace('models/', '')
                    models.append(name)
            return sorted(models)
        except Exception as e:
            self.log(f"List models error: {e}")
            return []
    
    def _update_scheduler(self):
        """更新调度任务"""
        self.scheduler.remove_all_jobs()
        
        for task_id, conf in self.tasks_config.items():
            if conf.get("enabled"):
                try:
                    cron_parts = conf["cron"].split()
                    if len(cron_parts) == 5:
                        self.scheduler.add_job(
                            lambda tid=task_id: run_task_wrapper(tid),
                            CronTrigger(
                                minute=cron_parts[0],
                                hour=cron_parts[1],
                                day=cron_parts[2],
                                month=cron_parts[3],
                                day_of_week=cron_parts[4]
                            ),
                            id=task_id,
                            replace_existing=True
                        )
                        self.log(f"Scheduled task {task_id} at {conf['cron']}")
                except Exception as e:
                    self.log(f"Failed to schedule {task_id}: {e}")


state = AppState()


# ========== 工具函数 ==========

def file_generator(start_dir: str) -> Generator[str, None, None]:
    """生成音频文件路径"""
    for root, _, filenames in os.walk(start_dir):
        for filename in filenames:
            if filename.lower().endswith(SUPPORTED_FORMATS):
                yield os.path.join(root, filename)


def get_metadata(path: str) -> dict:
    """获取文件元数据"""
    meta = AudioMetadataExtractor.extract(path)
    return meta.to_dict()


def get_dir_structure(current_path: Optional[str] = None) -> dict:
    """获取目录结构"""
    if not current_path:
        target_dir = state.music_dir
    else:
        target_dir = current_path
    
    # 安全检查
    if not os.path.abspath(target_dir).startswith(os.path.abspath(state.music_dir)):
        target_dir = state.music_dir
    
    dirs = []
    try:
        with os.scandir(target_dir) as it:
            for entry in it:
                if entry.is_dir() and not entry.name.startswith('.'):
                    dirs.append({"path": entry.path, "name": entry.name})
    except Exception as e:
        state.log(f"Dir scan error: {e}")
    
    dirs.sort(key=lambda x: x['name'].lower())
    
    return {
        "current_path": target_dir,
        "is_root": os.path.abspath(target_dir) == os.path.abspath(state.music_dir),
        "parent_path": os.path.dirname(target_dir),
        "subdirs": dirs
    }


def cleanup_memory():
    """清理内存"""
    gc.collect()
    state.log("Memory cleanup completed")


# ========== 核心任务函数 ==========

def task_scan_and_group(target_path: Optional[str] = None):
    """扫描并分组文件"""
    state.status = "scanning"
    scan_dir = target_path or state.dedupe_target_path
    
    # 清理旧数据
    state.files = [f for f in state.files if not f['path'].startswith(scan_dir)]
    state.candidates = []
    state.results = []
    
    batch = []
    file_count = 0
    
    state.message = f"正在扫描: {scan_dir} ..."
    
    for f_path in file_generator(scan_dir):
        try:
            meta = get_metadata(f_path)
            batch.append(meta)
            file_count += 1
            
            if len(batch) >= BATCH_SIZE:
                state.files.extend(batch)
                meta_db.batch_save(batch)
                state.progress = file_count
                state.message = f"已扫描 {file_count} 个文件..."
                batch = []
        except Exception as e:
            state.log(f"Error processing {f_path}: {e}")
    
    # 保存剩余批次
    if batch:
        state.files.extend(batch)
        meta_db.batch_save(batch)
    
    state.total = len(state.files)
    state.message = f"扫描完成, 正在按标题进行模糊匹配..."
    
    # 模糊匹配分组
    candidates = []
    files_to_check = [f for f in state.files if f['path'].startswith(scan_dir)]
    
    if files_to_check:
        sorted_files = sorted(
            files_to_check,
            key=lambda x: (x.get('title') or os.path.splitext(x['filename'])[0]).lower().strip()
        )
        
        current_group = [sorted_files[0]]
        
        for i in range(1, len(sorted_files)):
            state.progress = i
            prev = current_group[0]
            curr = sorted_files[i]
            
            prev_key = (prev.get('title') or os.path.splitext(prev['filename'])[0]).lower().strip()
            curr_key = (curr.get('title') or os.path.splitext(curr['filename'])[0]).lower().strip()
            
            similarity = fuzz.ratio(prev_key, curr_key)
            
            if similarity > 85:
                current_group.append(curr)
            else:
                if len(current_group) > 1:
                    candidates.append(current_group)
                current_group = [curr]
        
        if len(current_group) > 1:
            candidates.append(current_group)
    
    state.candidates = candidates
    state.status = "idle"
    state.message = f"扫描完成, 在 {scan_dir} 中发现 {len(state.candidates)} 组疑似重复。"
    cleanup_memory()


def task_analyze_with_gemini():
    """使用 Gemini AI 分析重复文件"""
    if not state.api_key:
        state.status = "error"
        state.message = "API Key 未配置"
        return
    
    state._apply_proxy()
    state.status = "analyzing"
    state.results = []
    
    try:
        genai.configure(api_key=state.api_key)
        model = genai.GenerativeModel(state.model_name)
        
        total_groups = len(state.candidates)
        
        for i in range(0, total_groups, AI_BATCH_SIZE):
            batch = state.candidates[i:i + AI_BATCH_SIZE]
            state.progress = i
            state.total = total_groups
            state.message = f"正在请求 AI ({state.model_name})... 进度 {i}/{total_groups}"
            
            prompt_data = [
                {
                    "group_id": i + idx,
                    "files": [{k: v for k, v in f.items() if k not in ['path', 'search_text']}
                              for f in group]
                }
                for idx, group in enumerate(batch)
            ]
            
            try:
                prompt = f"""Identify duplicates in these music file groups. Rules: 
1. Different extensions of same song -> DUPLICATE
2. "Live", "Remix" versions -> DUPLICATE  
3. Completely different songs -> NOT DUPLICATE
Input: {json.dumps(prompt_data)}
Return ONLY JSON: {{"results": [{{"group_id": int, "is_duplicate": bool, "reason": "string"}}]}}"""
                
                resp = model.generate_content(
                    prompt,
                    generation_config={"response_mime_type": "application/json"}
                )
                
                ai_res = json.loads(resp.text)
                for res in ai_res.get("results", []):
                    if res.get("is_duplicate"):
                        gid = res["group_id"]
                        if gid < len(state.candidates):
                            state.results.append({
                                "files": state.candidates[gid],
                                "reason": res.get("reason", "AI判断重复")
                            })
                
                time.sleep(AI_SLEEP_INTERVAL)
                
            except Exception as e:
                state.log(f"AI Batch Error: {e}")
        
        state.status = "done"
        state.message = f"分析完成。共确认 {len(state.results)} 组重复文件。"
    
    except Exception as e:
        state.status = "error"
        state.message = f"AI初始化失败: {str(e)}"
    
    cleanup_memory()


def task_dedupe_quality(target_dir: str):
    """音质去重任务"""
    deleted_count = 0
    
    def quality_score(path: str) -> tuple:
        ext = os.path.splitext(path)[1].lower()
        try:
            size = os.path.getsize(path)
        except:
            size = 0
        
        score = 0
        if ext in ['.flac', '.wav']:
            score = 3
        elif ext in ['.m4a', '.aac']:
            score = 2
        elif ext == '.mp3':
            score = 1
        return (score, size)
    
    def process_group(base_name: str, paths: List[str]) -> int:
        if len(paths) <= 1:
            return 0
        
        paths.sort(key=quality_score)
        keeper = paths[-1]
        count = 0
        
        to_delete = []
        for p in paths[:-1]:
            to_delete.append(p)
            state.log(f"[音质去重] 删除: {os.path.basename(p)}")
            count += 1
        
        # 批量删除
        for p in to_delete:
            try:
                os.remove(p)
            except Exception as e:
                state.log(f"删除失败 {p}: {e}")
                count -= 1
        
        meta_db.delete_batch(to_delete)
        return count
    
    # 分组文件
    groups = {}
    for root, _, files in os.walk(target_dir):
        for f in files:
            if f.lower().endswith(SUPPORTED_FORMATS):
                base_name = os.path.splitext(f)[0]
                full_path = os.path.join(root, f)
                if base_name not in groups:
                    groups[base_name] = []
                groups[base_name].append(full_path)
    
    # 并行处理
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_group, name, paths) 
                   for name, paths in groups.items()]
        for future in as_completed(futures):
            try:
                deleted_count += future.result()
            except Exception as e:
                state.log(f"Error: {e}")
    
    state.log(f"音质去重完成,共删除 {deleted_count} 个文件")
    cleanup_memory()
    def task_clean_short(target_dir: str):
    """清理短音频任务"""
    threshold = state.tasks_config["clean_short"].get("min_duration", 60)
    deleted_count = 0
    to_delete = []
    
    for root, _, files in os.walk(target_dir):
        for f in files:
            if f.lower().endswith(('.mp3', '.flac', '.m4a')):
                path = os.path.join(root, f)
                try:
                    duration = 0
                    if f.lower().endswith('.mp3'):
                        audio = MP3(path)
                        duration = audio.info.length if audio.info else 0
                    elif f.lower().endswith('.flac'):
                        audio = FLAC(path)
                        duration = audio.info.length if audio.info else 0
                    
                    if duration > 0 and duration < threshold:
                        to_delete.append(path)
                        state.log(f"[短音频清理] 删除: {f} (时长: {int(duration)}s)")
                except Exception as e:
                    logger.warning(f"Failed to check duration for {f}: {e}")
    
    # 批量删除
    for path in to_delete:
        try:
            os.remove(path)
            deleted_count += 1
        except Exception as e:
            state.log(f"删除失败 {path}: {e}")
    
    meta_db.delete_batch(to_delete)
    state.log(f"短音频清理完成,共删除 {deleted_count} 个文件")


def task_extract_meta(target_dir: str):
    """提取元数据任务 (NFO + 封面)"""
    processed_count = 0
    
    for root, _, files in os.walk(target_dir):
        for f in files:
            if f.lower().endswith(('.mp3', '.flac')):
                path = os.path.join(root, f)
                base_name = os.path.splitext(f)[0]
                
                try:
                    meta = get_metadata(path)
                    
                    # 生成 NFO
                    nfo_path = os.path.join(root, f"{base_name}.nfo")
                    if not os.path.exists(nfo_path):
                        duration_str = f"{int(meta['duration']//60)}:{meta['duration']%60:02d}"
                        nfo_content = f"""<?xml version="1.0" encoding="utf-8" standalone="yes"?>
<musicvideo>
  <title>{meta['title'] or base_name}</title>
  <artist>{meta['artist']}</artist>
  <album>{meta['album']}</album>
  <plot></plot>
  <runtime>{duration_str}</runtime>
</musicvideo>"""
                        with open(nfo_path, "w", encoding="utf-8") as nfo_file:
                            nfo_file.write(nfo_content)
                        processed_count += 1
                    
                    # 提取封面
                    song_cover_path = os.path.join(root, f"{base_name}.jpg")
                    folder_cover_path = os.path.join(root, "folder.jpg")
                    
                    if not os.path.exists(song_cover_path) or not os.path.exists(folder_cover_path):
                        art_data = _extract_album_art(path)
                        
                        if art_data:
                            if not os.path.exists(song_cover_path):
                                with open(song_cover_path, "wb") as img_file:
                                    img_file.write(art_data)
                                state.log(f"[元数据] 生成歌曲封面: {os.path.basename(song_cover_path)}")
                            
                            if not os.path.exists(folder_cover_path):
                                with open(folder_cover_path, "wb") as img_file:
                                    img_file.write(art_data)
                                state.log(f"[元数据] 生成专辑封面: folder.jpg")

                except Exception as e:
                    state.log(f"Error extracting meta from {f}: {e}")
    
    state.log(f"元数据提取完成,共处理 {processed_count} 个文件")


def _extract_album_art(path: str) -> Optional[bytes]:
    """提取专辑封面"""
    try:
        if path.lower().endswith('.mp3'):
            audio = MP3(path, ID3=EasyID3)
            if audio.tags:
                for key in audio.tags.keys():
                    if key.startswith('APIC:'):
                        return audio.tags[key].data
        elif path.lower().endswith('.flac'):
            audio = FLAC(path)
            if audio.pictures:
                return audio.pictures[0].data
    except:
        pass
    return None


def task_clean_junk(target_dir: str):
    """清理垃圾文件和空目录"""
    cleaned_count = 0
    
    for root, dirs, files in os.walk(target_dir, topdown=False):
        has_music = any(
            os.path.splitext(f)[1].lower() in MUSIC_EXTS
            for f in files
        )
        
        if not has_music:
            # 删除孤立文件
            for f in files:
                if os.path.splitext(f)[1].lower() in JUNK_EXTS:
                    path = os.path.join(root, f)
                    try:
                        os.remove(path)
                        state.log(f"[垃圾清理] 删除孤立文件: {path}")
                        cleaned_count += 1
                    except:
                        pass
            
            # 删除空目录
            try:
                if not os.listdir(root):
                    os.rmdir(root)
                    state.log(f"[垃圾清理] 删除空目录: {root}")
            except:
                pass
    
    state.log(f"垃圾清理完成,清理 {cleaned_count} 个文件")


# ========== 批量操作函数 ==========

def batch_update_metadata(
    file_paths: List[str],
    artist: Optional[str] = None,
    album_artist: Optional[str] = None,
    title: Optional[str] = None,
    album: Optional[str] = None
) -> int:
    """批量更新元数据"""
    updated_count = 0
    
    for path in file_paths:
        if not os.path.exists(path):
            continue
        
        try:
            audio = None
            if path.lower().endswith('.mp3'):
                audio = EasyID3(path)
            elif path.lower().endswith('.flac'):
                audio = FLAC(path)
            
            if audio is not None:
                if artist:
                    audio['artist'] = artist
                if album_artist:
                    audio['albumartist'] = album_artist
                if title:
                    audio['title'] = title
                if album:
                    audio['album'] = album
                audio.save()
                updated_count += 1
                
                # 更新内存中的数据
                for f in state.files:
                    if f['path'] == path:
                        if artist:
                            f['artist'] = artist
                        if album_artist:
                            f['album_artist'] = album_artist
                        if title:
                            f['title'] = title
                        if album:
                            f['album'] = album
                        break
        except Exception as e:
            state.log(f"Error updating {path}: {e}")
    
    return updated_count


def batch_rename_files(paths: List[str], pattern: str) -> int:
    """批量重命名文件"""
    renamed_count = 0
    
    for path in paths:
        if not os.path.exists(path):
            continue
        
        try:
            meta = get_metadata(path)
            
            # 清理标签用于文件名
            def clean_tag(text: Optional[str]) -> str:
                if not text:
                    return None
                return text.replace(" / ", " & ").replace("/", " & ").replace(";", " & ")
            
            safe_meta = {
                'artist': clean_tag(meta['artist']) or 'Unknown Artist',
                'title': clean_tag(meta['title']) or meta['filename'],
                'album': clean_tag(meta['album']) or 'Unknown Album',
                'album_artist': clean_tag(meta['album_artist']) or 'Unknown Artist'
            }
            
            # 应用模式
            try:
                new_filename_base = pattern.format(**safe_meta)
            except KeyError as e:
                state.log(f"Rename pattern error: missing key {e}")
                continue
            
            # 移除非法字符
            invalid_chars = '<>:"/\\|?*'
            for char in invalid_chars:
                new_filename_base = new_filename_base.replace(char, '')
            
            new_filename_base = new_filename_base.strip()
            
            # 构建新路径
            ext = os.path.splitext(path)[1]
            new_filename = f"{new_filename_base}{ext}"
            dir_name = os.path.dirname(path)
            new_path = os.path.join(dir_name, new_filename)
            
            # 处理重名
            counter = 1
            while os.path.exists(new_path) and new_path != path:
                new_path = os.path.join(dir_name, f"{new_filename_base} ({counter}){ext}")
                counter += 1
            
            # 重命名
            if new_path != path:
                os.rename(path, new_path)
                meta_db.delete_by_path(path)
                new_meta = get_metadata(new_path)
                meta_db.save_metadata(new_meta)
                
                # 更新内存
                for f in state.files:
                    if f['path'] == path:
                        f.update(new_meta)
                        break
                
                renamed_count += 1
        except Exception as e:
            state.log(f"Rename error {path}: {e}")
    
    return renamed_count


def delete_file(path: str) -> bool:
    """删除单个文件"""
    try:
        if os.path.exists(path):
            os.remove(path)
        meta_db.delete_by_path(path)
        state.files = [f for f in state.files if f['path'] != path]
        return True
    except Exception as e:
        state.log(f"Delete error {path}: {e}")
        return False


def fix_single_metadata_ai(path: str) -> dict:
    """使用 AI 修复单个文件的元数据"""
    if not state.api_key:
        return {"error": "API Key not configured"}
    
    if not os.path.exists(path):
        return {"error": "File not found"}
    
    try:
        state._apply_proxy()
        genai.configure(api_key=state.api_key)
        model = genai.GenerativeModel(state.model_name)
        
        filename = os.path.basename(path)
        current_meta = get_metadata(path)
        
        prompt = f"""Analyze this music file filename and suggest correct metadata tags.
Filename: {filename}
Current Tags: Artist={current_meta['artist']}, Title={current_meta['title']}, Album={current_meta['album']}

Return ONLY JSON:
{{
  "artist": "string",
  "title": "string",
  "album": "string (optional)",
  "album_artist": "string (optional)"
}}
"""
        resp = model.generate_content(
            prompt,
            generation_config={"response_mime_type": "application/json"}
        )
        result = json.loads(resp.text)
        
        # 应用修复
        batch_update_metadata(
            [path],
            artist=result.get("artist"),
            title=result.get("title"),
            album=result.get("album"),
            album_artist=result.get("album_artist")
        )
        
        return {"status": "success", "data": result}
        
    except Exception as e:
        state.log(f"AI Fix Error: {e}")
        return {"error": str(e)}


def run_task_wrapper(task_id: str):
    """任务包装器"""
    target = state.task_target_path
    scan_dir = target if target and os.path.exists(target) else state.music_dir
    
    state.log(f"开始执行任务: {task_id} (目标: {scan_dir})")
    
    try:
        if task_id == "dedupe_quality":
            task_dedupe_quality(scan_dir)
        elif task_id == "clean_short":
            task_clean_short(scan_dir)
        elif task_id == "extract_meta":
            task_extract_meta(scan_dir)
        elif task_id == "clean_junk":
            task_clean_junk(scan_dir)
        
        # 更新最后运行时间
        state.tasks_config[task_id]["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        state.save_config()
        state.log(f"✅ 任务完成: {task_id}")
    except Exception as e:
        state.log(f"❌ 任务 {task_id} 失败: {str(e)}")
        logger.error(f"Task {task_id} failed: {e}", exc_info=True)
