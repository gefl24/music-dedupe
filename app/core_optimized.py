import os
import json
import threading
import asyncio
import time
import sqlite3
import gc
from datetime import datetime
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import google.generativeai as genai
from mutagen.easyid3 import EasyID3
from mutagen.mp3 import MP3
from mutagen.flac import FLAC
from mutagen.id3 import ID3NoHeaderError
from thefuzz import fuzz

DATA_DIR = "/data"
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")
DB_FILE = os.path.join(DATA_DIR, "metadata.db")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MusicManager")

# ✅ 元数据数据库管理
class MetadataDB:
    def __init__(self, db_path=DB_FILE):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        with self.get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata (
                    path TEXT PRIMARY KEY,
                    filename TEXT,
                    artist TEXT,
                    title TEXT,
                    album TEXT,
                    album_artist TEXT,
                    duration INTEGER,
                    size_mb REAL,
                    bitrate INTEGER,
                    search_text TEXT,
                    updated_at TIMESTAMP
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_filename ON metadata(filename)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_artist ON metadata(artist)")
            conn.commit()
    
    @contextmanager
    def get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def save_metadata(self, meta):
        with self.get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO metadata 
                (path, filename, artist, title, album, album_artist, duration, size_mb, bitrate, search_text, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """, (meta['path'], meta['filename'], meta['artist'], meta['title'], 
                  meta['album'], meta['album_artist'], meta['duration'], 
                  meta['size_mb'], meta['bitrate'], meta['search_text']))
            conn.commit()
    
    def get_all(self, limit=None, offset=0):
        with self.get_conn() as conn:
            if limit:
                sql = "SELECT * FROM metadata ORDER BY filename LIMIT ? OFFSET ?"
                rows = conn.execute(sql, (limit, offset)).fetchall()
            else:
                sql = "SELECT * FROM metadata ORDER BY filename"
                rows = conn.execute(sql).fetchall()
            return [dict(row) for row in rows]
    
    def get_count(self):
        with self.get_conn() as conn:
            return conn.execute("SELECT COUNT(*) as cnt FROM metadata").fetchone()['cnt']
    
    def search(self, query, limit=50, offset=0):
        with self.get_conn() as conn:
            q = f"%{query.lower()}%"
            sql = """SELECT * FROM metadata 
                     WHERE filename LIKE ? OR artist LIKE ? OR title LIKE ? 
                     LIMIT ? OFFSET ?"""
            rows = conn.execute(sql, (q, q, q, limit, offset)).fetchall()
            return [dict(row) for row in rows]
    
    def delete_by_path(self, path):
        with self.get_conn() as conn:
            conn.execute("DELETE FROM metadata WHERE path = ?", (path,))
            conn.commit()
    
    def clear_all(self):
        with self.get_conn() as conn:
            conn.execute("DELETE FROM metadata")
            conn.commit()

meta_db = MetadataDB()

class AppState:
    def __init__(self):
        self.api_key = ""
        self.model_name = "gemini-1.5-flash"
        self.proxy_url = ""
        self.music_dir = "/music"
        self.task_target_path = "/music"
        self.status = "idle"
        self.progress = 0
        self.total = 0
        self.message = "准备就绪"
        self.files = []
        self.candidates = []
        self.results = []
        self.task_logs = []
        
        self.tasks_config = {
            "dedupe_quality": {"enabled": False, "cron": "0 2 * * *", "last_run": None},
            "clean_short": {"enabled": False, "cron": "0 3 * * *", "min_duration": 60, "last_run": None},
            "extract_meta": {"enabled": False, "cron": "0 4 * * *", "last_run": None},
            "clean_junk": {"enabled": False, "cron": "0 5 * * *", "last_run": None}
        }
        
        self.load_config()
        self.apply_proxy()
        
        self.scheduler = BackgroundScheduler()
        self.update_scheduler()
        self.scheduler.start()
        
        # ✅ 并发执行器
        self.executor = ThreadPoolExecutor(max_workers=4)

    def log(self, msg):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        entry = f"[{timestamp}] {msg}"
        print(entry)
        self.task_logs.insert(0, entry)
        if len(self.task_logs) > 200:
            self.task_logs.pop()

    def load_config(self):
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r') as f:
                    config = json.load(f)
                    self.api_key = config.get("api_key", "").strip()
                    self.model_name = config.get("model_name", "gemini-1.5-flash").strip()
                    self.proxy_url = config.get("proxy_url", "").strip()
                    self.music_dir = config.get("music_dir", "/music").strip()
                    self.task_target_path = config.get("task_target_path", self.music_dir).strip()
                    saved_tasks = config.get("tasks_config", {})
                    for key, val in saved_tasks.items():
                        if key in self.tasks_config:
                            self.tasks_config[key].update(val)
            except Exception as e:
                print(f"Error loading config: {e}")

    def save_config(self):
        try:
            if not os.path.exists(DATA_DIR):
                os.makedirs(DATA_DIR)
            with open(CONFIG_FILE, 'w') as f:
                json.dump({
                    "api_key": self.api_key,
                    "model_name": self.model_name,
                    "proxy_url": self.proxy_url,
                    "music_dir": self.music_dir,
                    "task_target_path": self.task_target_path,
                    "tasks_config": self.tasks_config
                }, f)
            self.apply_proxy()
            self.update_scheduler()
        except Exception as e:
            print(f"Error saving config: {e}")

    def apply_proxy(self):
        if self.proxy_url:
            for key in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
                os.environ[key] = self.proxy_url
        else:
            for key in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
                os.environ.pop(key, None)

    def update_scheduler(self):
        self.scheduler.remove_all_jobs()
        for task_id, conf in self.tasks_config.items():
            if conf.get("enabled"):
                try:
                    parts = conf["cron"].split()
                    if len(parts) == 5:
                        self.scheduler.add_job(
                            run_task_wrapper,
                            CronTrigger(minute=parts[0], hour=parts[1], day=parts[2], 
                                      month=parts[3], day_of_week=parts[4]),
                            args=[task_id],
                            id=task_id,
                            replace_existing=True
                        )
                except Exception as e:
                    self.log(f"Failed to schedule {task_id}: {e}")

state = AppState()

# ✅ 流式文件生成器，降低内存使用
def file_generator(start_dir):
    for root, _, filenames in os.walk(start_dir):
        for filename in filenames:
            if filename.lower().endswith(('.mp3', '.flac', '.m4a', '.wma')):
                yield os.path.join(root, filename)

def get_metadata(path):
    filename = os.path.basename(path)
    size_mb = round(os.path.getsize(path) / (1024 * 1024), 2)
    tags = {}
    duration = 0
    bitrate = 0
    
    try:
        if path.lower().endswith('.mp3'):
            try:
                audio = MP3(path, ID3=EasyID3)
            except ID3NoHeaderError:
                audio = MP3(path)
                audio.add_tags()
            tags = audio
            duration = int(audio.info.length)
            bitrate = int(audio.info.bitrate / 1000) if audio.info.bitrate else 0
        elif path.lower().endswith('.flac'):
            audio = FLAC(path)
            tags = audio
            duration = int(audio.info.length)
            bitrate = int(audio.info.bitrate / 1000) if audio.info.bitrate else 0
    except:
        pass
    
    def get_tag_display(key):
        values = tags.get(key, [])
        valid_values = [str(v).strip() for v in values if v]
        return " / ".join(valid_values) if valid_values else ""
    
    artist = get_tag_display('artist')
    title = get_tag_display('title')
    
    if not title:
        base = os.path.splitext(filename)[0]
        if " - " in base:
            parts = base.split(" - ")
            artist = parts[0] if not artist else artist
            title = parts[1]
        else:
            title = base
    
    search_text = f"{artist} {title} {filename}".lower()
    
    return {
        "path": path,
        "filename": filename,
        "artist": artist.strip(),
        "title": title.strip(),
        "album": get_tag_display('album').strip(),
        "album_artist": get_tag_display('albumartist').strip(),
        "duration": duration,
        "size_mb": size_mb,
        "bitrate": bitrate,
        "search_text": search_text
    }

def task_scan_and_group_optimized(target_path=None):
    """优化版扫描，使用流式处理和批量操作"""
    state.status = "scanning"
    scan_dir = target_path or state.music_dir
    
    if target_path:
        state.files = [f for f in state.files if not f['path'].startswith(target_path)]
    else:
        state.files = []
        meta_db.clear_all()
    
    state.candidates = []
    state.results = []
    
    batch_size = 100
    batch = []
    file_count = 0
    
    # ✅ 流式处理文件
    for f_path in file_generator(scan_dir):
        try:
            meta = get_metadata(f_path)
            batch.append(meta)
            file_count += 1
            
            if len(batch) >= batch_size:
                state.files.extend(batch)
                for m in batch:
                    meta_db.save_metadata(m)
                state.progress = file_count
                state.message = f"已扫描 {file_count} 个文件..."
                batch = []
        except:
            pass
    
    if batch:
        state.files.extend(batch)
        for m in batch:
            meta_db.save_metadata(m)
    
    state.total = len(state.files)
    state.message = f"扫描完成，发现 {state.total} 个文件，正在进行模糊分组..."
    
    # ✅ 优化分组逻辑
    sorted_files = sorted(state.files, key=lambda x: x['search_text'])
    candidates = []
    
    if sorted_files:
        current_group = [sorted_files[0]]
        for i in range(1, len(sorted_files)):
            state.progress = i
            prev = current_group[0]
            curr = sorted_files[i]
            
            if fuzz.token_set_ratio(prev['search_text'], curr['search_text']) > 80:
                current_group.append(curr)
            else:
                if len(current_group) > 1:
                    candidates.append(current_group)
                current_group = [curr]
        
        if len(current_group) > 1:
            candidates.append(current_group)
    
    state.candidates = candidates
    state.status = "idle"
    state.message = f"扫描完成，发现 {len(state.candidates)} 组疑似重复。"

def task_dedupe_quality_concurrent(target_dir):
    """并发处理去质量重，提高速度"""
    deleted_count = 0
    
    def quality_score(path):
        ext = os.path.splitext(path)[1].lower()
        size = os.path.getsize(path)
        score = 0
        if ext in ['.flac', '.wav']:
            score = 3
        elif ext in ['.m4a', '.aac']:
            score = 2
        elif ext == '.mp3':
            score = 1
        return (score, size)
    
    def process_group(base_name, paths):
        if len(paths) <= 1:
            return 0
        
        paths.sort(key=quality_score)
        keeper = paths[-1]
        count = 0
        
        for p in paths[:-1]:
            try:
                os.remove(p)
                state.log(f"[音质去重] 删除: {os.path.basename(p)}")
                meta_db.delete_by_path(p)
                count += 1
            except Exception as e:
                state.log(f"删除失败 {p}: {e}")
        
        return count
    
    groups = {}
    for root, _, files in os.walk(target_dir):
        for f in files:
            if f.lower().endswith(('.mp3', '.flac', '.wav', '.m4a', '.wma')):
                base_name = os.path.splitext(f)[0]
                full_path = os.path.join(root, f)
                if base_name not in groups:
                    groups[base_name] = []
                groups[base_name].append(full_path)
    
    # ✅ 并发处理
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_group, name, paths) for name, paths in groups.items()]
        for future in ThreadPoolExecutor.as_completed(futures):
            deleted_count += future.result()
    
    state.log(f"音质去重完成，共删除 {deleted_count} 个文件")
    gc.collect()
    return deleted_count

def run_task_wrapper(task_id):
    target = state.task_target_path
    state.log(f"开始执行任务: {task_id}")
    try:
        if task_id == "dedupe_quality":
            task_dedupe_quality_concurrent(target)
        state.tasks_config[task_id]["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        state.save_config()
        state.log(f"任务完成: {task_id}")
    except Exception as e:
        state.log(f"任务 {task_id} 失败: {str(e)}")

def delete_file(path):
    try:
        if os.path.exists(path):
            os.remove(path)
            meta_db.delete_by_path(path)
            state.files = [f for f in state.files if f['path'] != path]
            return True
    except:
        return False
