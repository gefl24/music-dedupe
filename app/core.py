import os
import json
import threading
import asyncio
import time
import sqlite3
import gc
import shutil
import warnings
from datetime import datetime
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from logging.handlers import RotatingFileHandler

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# Google AI SDK
import google.generativeai as genai

# Audio processing
from mutagen.easyid3 import EasyID3
from mutagen.mp3 import MP3
from mutagen.flac import FLAC
from mutagen.id3 import ID3NoHeaderError
from thefuzz import fuzz
from PIL import Image
import io

# 1. ✅ 屏蔽弃用警告 (Fix: Deprecation Warnings)
warnings.filterwarnings('ignore', category=UserWarning, module='google.generativeai')
warnings.filterwarnings('ignore', category=FutureWarning, module='google.generativeai')

DATA_DIR = "/data"
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")
DB_FILE = os.path.join(DATA_DIR, "metadata.db")
LOG_FILE = os.path.join(DATA_DIR, "app.log")

def setup_logging():
    logger = logging.getLogger("MusicManager")
    logger.setLevel(logging.INFO)
    
    handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=10*1024*1024,
        backupCount=5
    )
    formatter = logging.Formatter('[%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger

logger = setup_logging()

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
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_filename ON metadata(filename)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_artist ON metadata(artist)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_search ON metadata(search_text)")
            conn.commit()
    
    @contextmanager
    def get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        try:
            yield conn
        finally:
            conn.close()
    
    def save_metadata(self, meta):
        with self.get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO metadata 
                (path, filename, artist, title, album, album_artist, duration, size_mb, bitrate, search_text)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (meta['path'], meta['filename'], meta['artist'], meta['title'], 
                  meta['album'], meta['album_artist'], meta['duration'], 
                  meta['size_mb'], meta['bitrate'], meta['search_text']))
            conn.commit()
    
    def batch_save(self, metadata_list):
        with self.get_conn() as conn:
            for meta in metadata_list:
                conn.execute("""
                    INSERT OR REPLACE INTO metadata 
                    (path, filename, artist, title, album, album_artist, duration, size_mb, bitrate, search_text)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                     ORDER BY filename LIMIT ? OFFSET ?"""
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
    
    def optimize(self):
        with self.get_conn() as conn:
            conn.execute("VACUUM")
            conn.execute("ANALYZE")

meta_db = MetadataDB()

# 2. ✅ 前置声明任务执行函数 (Fix: Forward Declaration)
# 这里的定义是为了让 AppState 初始化时不报错，实际逻辑会在文件末尾覆盖
def run_task_wrapper(task_id):
    """
    Placeholder for the actual task runner.
    The real implementation is at the bottom of the file.
    """
    pass 

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
        
        self.executor = ThreadPoolExecutor(max_workers=4)

    def log(self, msg):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        entry = f"[{timestamp}] {msg}"
        print(entry)
        logger.info(msg)
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
                self.log(f"Error loading config: {e}")

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
                }, f, indent=2)
            self.apply_proxy()
            self.update_scheduler()
        except Exception as e:
            self.log(f"Error saving config: {e}")

    def apply_proxy(self):
        if self.proxy_url:
            for key in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
                os.environ[key] = self.proxy_url
        else:
            for key in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
                os.environ.pop(key, None)

    def get_available_models(self):
        if not self.api_key:
            return []
        self.apply_proxy()
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

    def update_scheduler(self):
        self.scheduler.remove_all_jobs()
        for task_id, conf in self.tasks_config.items():
            if conf.get("enabled"):
                try:
                    parts = conf["cron"].split()
                    if len(parts) == 5:
                        # 3. ✅ 使用 lambda 闭包修复循环绑定问题 (Fix: Lambda Binding)
                        self.scheduler.add_job(
                            lambda tid=task_id: run_task_wrapper(tid),
                            CronTrigger(minute=parts[0], hour=parts[1], day=parts[2], 
                                      month=parts[3], day_of_week=parts[4]),
                            id=task_id,
                            replace_existing=True
                        )
                        self.log(f"Scheduled task {task_id} at {conf['cron']}")
                except Exception as e:
                    self.log(f"Failed to schedule {task_id}: {e}")

state = AppState()

def file_generator(start_dir):
    for root, _, filenames in os.walk(start_dir):
        for filename in filenames:
            if filename.lower().endswith(('.mp3', '.flac', '.m4a', '.wma')):
                yield os.path.join(root, filename)

def get_metadata(path):
    filename = os.path.basename(path)
    try:
        size_mb = round(os.path.getsize(path) / (1024 * 1024), 2)
    except:
        size_mb = 0
    
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
            duration = int(audio.info.length) if audio.info.length else 0
            bitrate = int(audio.info.bitrate / 1000) if audio.info.bitrate else 0
        elif path.lower().endswith('.flac'):
            audio = FLAC(path)
            tags = audio
            duration = int(audio.info.length) if audio.info.length else 0
            bitrate = int(audio.info.bitrate / 1000) if audio.info.bitrate else 0
    except Exception as e:
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
            parts = base.split(" - ", 1)
            if not artist:
                artist = parts[0]
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

def get_dir_structure(current_path=None):
    if not current_path:
        target_dir = state.music_dir
    else:
        target_dir = current_path
    
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
    gc.collect()
    state.log("Memory cleanup completed")

def task_scan_and_group(target_path=None):
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
    
    for f_path in file_generator(scan_dir):
        try:
            meta = get_metadata(f_path)
            batch.append(meta)
            file_count += 1
            
            if len(batch) >= batch_size:
                state.files.extend(batch)
                meta_db.batch_save(batch)
                state.progress = file_count
                state.message = f"已扫描 {file_count} 个文件..."
                batch = []
        except Exception as e:
            state.log(f"Error processing {f_path}: {e}")
    
    if batch:
        state.files.extend(batch)
        meta_db.batch_save(batch)
    
    state.total = len(state.files)
    state.message = f"扫描完成,发现 {state.total} 个文件,正在进行模糊分组..."
    
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
    state.message = f"扫描完成,发现 {len(state.candidates)} 组疑似重复。"
    cleanup_memory()

def task_analyze_with_gemini():
    if not state.api_key:
        state.status = "error"
        state.message = "API Key 未配置"
        return
    
    state.apply_proxy()
    state.status = "analyzing"
    state.results = []
    
    try:
        genai.configure(api_key=state.api_key)
        model = genai.GenerativeModel(state.model_name)
        
        batch_size = 3
        total_groups = len(state.candidates)
        
        for i in range(0, total_groups, batch_size):
            batch = state.candidates[i:i+batch_size]
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
                
                time.sleep(1)
                
            except Exception as e:
                state.log(f"AI Batch Error: {e}")
        
        state.status = "done"
        state.message = f"分析完成。共确认 {len(state.results)} 组重复文件。"
    
    except Exception as e:
        state.status = "error"
        state.message = f"AI初始化失败: {str(e)}"
    
    cleanup_memory()

def task_dedupe_quality(target_dir):
    deleted_count = 0
    
    def quality_score(path):
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
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_group, name, paths) for name, paths in groups.items()]
        for future in as_completed(futures):
            try:
                deleted_count += future.result()
            except Exception as e:
                state.log(f"Error: {e}")
    
    state.log(f"音质去重完成,共删除 {deleted_count} 个文件")
    cleanup_memory()

def task_clean_short(target_dir):
    threshold = state.tasks_config["clean_short"].get("min_duration", 60)
    deleted_count = 0
    
    for root, _, files in os.walk(target_dir):
        for f in files:
            if f.lower().endswith(('.mp3', '.flac', '.m4a')):
                path = os.path.join(root, f)
                try:
                    duration = 0
                    if f.lower().endswith('.mp3'):
                        audio = MP3(path)
                        duration = audio.info.length
                    elif f.lower().endswith('.flac'):
                        audio = FLAC(path)
                        duration = audio.info.length
                    
                    if duration > 0 and duration < threshold:
                        os.remove(path)
                        meta_db.delete_by_path(path)
                        state.log(f"[短音频清理] 删除: {f} (时长: {int(duration)}s)")
                        deleted_count += 1
                except Exception as e:
                    pass
    
    state.log(f"短音频清理完成,共删除 {deleted_count} 个文件")

def task_extract_meta(target_dir):
    processed_count = 0
    
    for root, _, files in os.walk(target_dir):
        for f in files:
            if f.lower().endswith(('.mp3', '.flac')):
                path = os.path.join(root, f)
                base_name = os.path.splitext(f)[0]
                
                try:
                    meta = get_metadata(path)
                    
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
                    
                    cover_target = os.path.join(root, "folder.jpg")
                    if os.path.exists(cover_target):
                        cover_target = os.path.join(root, f"{base_name}.jpg")
                    
                    if not os.path.exists(cover_target):
                        art_data = None
                        if f.lower().endswith('.mp3'):
                            try:
                                audio = MP3(path, ID3=EasyID3)
                                if audio.tags:
                                    for key in audio.tags.keys():
                                        if key.startswith('APIC:'):
                                            art_data = audio.tags[key].data
                                            break
                            except:
                                pass
                        elif f.lower().endswith('.flac'):
                            try:
                                audio = FLAC(path)
                                if audio.pictures:
                                    art_data = audio.pictures[0].data
                            except:
                                pass
                        
                        if art_data:
                            with open(cover_target, "wb") as img_file:
                                img_file.write(art_data)
                            state.log(f"[元数据] 提取封面: {os.path.basename(cover_target)}")

                except Exception as e:
                    state.log(f"Error extracting meta from {f}: {e}")
    
    state.log(f"元数据提取完成,共处理 {processed_count} 个文件")

def task_clean_junk(target_dir):
    cleaned_count = 0
    music_exts = {'.mp3', '.flac', '.wav', '.m4a', '.wma', '.ape', '.ogg'}
    junk_exts = {'.nfo', '.jpg', '.jpeg', '.png', '.lrc', '.txt'}
    
    for root, dirs, files in os.walk(target_dir, topdown=False):
        has_music = False
        for f in files:
            if os.path.splitext(f)[1].lower() in music_exts:
                has_music = True
                break
        
        if not has_music:
            for f in files:
                if os.path.splitext(f)[1].lower() in junk_exts:
                    path = os.path.join(root, f)
                    try:
                        os.remove(path)
                        state.log(f"[垃圾清理] 删除孤立文件: {path}")
                        cleaned_count += 1
                    except:
                        pass
            
            try:
                if not os.listdir(root):
                    os.rmdir(root)
                    state.log(f"[垃圾清理] 删除空目录: {root}")
            except:
                pass
    
    state.log(f"垃圾清理完成,清理 {cleaned_count} 个文件")

def batch_update_metadata(file_paths, artist=None, album_artist=None, title=None, album=None):
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
                
                # Update memory cache
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

# 4. ✅ 补全缺失的逻辑 (Fix: Missing Implementations)

def batch_rename_files(paths, pattern):
    """批量重命名文件"""
    renamed_count = 0
    for path in paths:
        if not os.path.exists(path):
            continue
            
        try:
            meta = get_metadata(path)
            
            # ✅ 内部辅助函数：处理多值分隔符，将 " / " 转换为 " & "
            def clean_tag_for_filename(text):
                if not text:
                    return None
                # 1. 将标准分隔符 " / " 替换为 " & "
                # 2. 将可能的路径非法字符 "/" 替换为 " & "
                # 3. 将分号 ";" 替换为 " & "
                return text.replace(" / ", " & ").replace("/", " & ").replace(";", " & ")

            # 安全检查：确保有必要的元数据，并应用格式化
            safe_meta = {
                'artist': clean_tag_for_filename(meta['artist']) or 'Unknown Artist',
                'title': clean_tag_for_filename(meta['title']) or meta['filename'],
                'album': clean_tag_for_filename(meta['album']) or 'Unknown Album',
                'album_artist': clean_tag_for_filename(meta['album_artist']) or 'Unknown Artist'
            }
            
            # 格式化新文件名
            try:
                new_filename_base = pattern.format(**safe_meta)
            except KeyError as e:
                # 防止 pattern 中包含不支持的键
                state.log(f"Rename pattern error: missing key {e}")
                continue

            # 移除非法字符 (Windows/Linux 通用限制)
            invalid_chars = '<>:"/\\|?*'
            for char in invalid_chars:
                new_filename_base = new_filename_base.replace(char, '')
            
            # 去除首尾空格
            new_filename_base = new_filename_base.strip()

            ext = os.path.splitext(path)[1]
            new_filename = f"{new_filename_base}{ext}"
            dir_name = os.path.dirname(path)
            new_path = os.path.join(dir_name, new_filename)
            
            # 处理文件名冲突
            counter = 1
            while os.path.exists(new_path) and new_path != path:
                new_path = os.path.join(dir_name, f"{new_filename_base} ({counter}){ext}")
                counter += 1
            
            if new_path != path:
                os.rename(path, new_path)
                
                # 更新数据库
                meta_db.delete_by_path(path)
                new_meta = get_metadata(new_path)
                meta_db.save_metadata(new_meta)
                
                # 更新内存缓存
                for f in state.files:
                    if f['path'] == path:
                        f.update(new_meta)
                        break
                
                renamed_count += 1
        except Exception as e:
            state.log(f"Rename error {path}: {e}")
            
    return renamed_count

def delete_file(path):
    """删除单个文件及其数据库记录"""
    try:
        if os.path.exists(path):
            os.remove(path)
        
        meta_db.delete_by_path(path)
        state.files = [f for f in state.files if f['path'] != path]
        return True
    except Exception as e:
        state.log(f"Delete error {path}: {e}")
        return False

def fix_single_metadata_ai(path):
    """使用 AI 修复单个文件的元数据"""
    if not state.api_key:
        return {"error": "API Key not configured"}
    
    if not os.path.exists(path):
        return {"error": "File not found"}
        
    try:
        state.apply_proxy()
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
        
        # 应用元数据
        paths = [path]
        batch_update_metadata(
            paths, 
            artist=result.get("artist"),
            title=result.get("title"),
            album=result.get("album"),
            album_artist=result.get("album_artist")
        )
        
        return {"status": "success", "data": result}
        
    except Exception as e:
        state.log(f"AI Fix Error: {e}")
        return {"error": str(e)}

# 5. ✅ 真正的任务执行逻辑 (Fix: Actual Task Execution)
# 这个函数会覆盖之前前置声明的函数
def run_task_wrapper(task_id):
    """
    实际的任务执行器。
    由 AppState 的调度器通过 lambda 调用。
    """
    target = state.task_target_path
    if target and os.path.exists(target):
        scan_dir = target
    else:
        scan_dir = state.music_dir
    
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
        
        if task_id in state.tasks_config:
            state.tasks_config[task_id]["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            state.save_config()
            
        state.log(f"✅ 任务完成: {task_id}")
    except Exception as e:
        state.log(f"❌ 任务 {task_id} 失败: {str(e)}")
