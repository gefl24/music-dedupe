import os
import json
import threading
import time
import glob
import io
import shutil
import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import google.generativeai as genai
from mutagen.easyid3 import EasyID3
from mutagen.mp3 import MP3
from mutagen.flac import FLAC, Picture
from mutagen.id3 import ID3NoHeaderError, APIC
from PIL import Image
from thefuzz import fuzz

DATA_DIR = "/data"
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MusicManager")

class AppState:
    def __init__(self):
        self.api_key = ""
        self.model_name = "gemini-1.5-flash"
        self.proxy_url = ""
        self.music_dir = "/music"
        self.task_target_path = "/music" # ✅ 新增：计划任务的目标文件夹
        self.status = "idle" 
        self.progress = 0
        self.total = 0
        self.message = "准备就绪"
        self.files = []       
        self.candidates = []  
        self.results = []
        
        self.tasks_config = {
            "dedupe_quality": {"enabled": False, "cron": "0 2 * * *", "last_run": None},
            "clean_short": {"enabled": False, "cron": "0 3 * * *", "min_duration": 60, "last_run": None},
            "extract_meta": {"enabled": False, "cron": "0 4 * * *", "last_run": None},
            "clean_junk": {"enabled": False, "cron": "0 5 * * *", "last_run": None}
        }
        self.task_logs = []

        self.load_config()
        self.apply_proxy()
        
        self.scheduler = BackgroundScheduler()
        self.update_scheduler()
        self.scheduler.start()

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
                    # ✅ 加载任务目标路径，默认为音乐根目录
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
                    "task_target_path": self.task_target_path, # ✅ 保存目标路径
                    "tasks_config": self.tasks_config
                }, f)
            self.apply_proxy()
            self.update_scheduler()
        except Exception as e:
            print(f"Error saving config: {e}")

    def update_scheduler(self):
        self.scheduler.remove_all_jobs()
        for task_id, conf in self.tasks_config.items():
            if conf.get("enabled"):
                try:
                    parts = conf["cron"].split()
                    if len(parts) == 5:
                        self.scheduler.add_job(
                            run_task_wrapper, 
                            CronTrigger(minute=parts[0], hour=parts[1], day=parts[2], month=parts[3], day_of_week=parts[4]),
                            args=[task_id],
                            id=task_id,
                            replace_existing=True
                        )
                        print(f"Scheduled task {task_id} at {conf['cron']}")
                except Exception as e:
                    print(f"Failed to schedule {task_id}: {e}")

    # ... (apply_proxy, get_available_models 保持不变) ...
    def apply_proxy(self):
        if self.proxy_url:
            os.environ['http_proxy'] = self.proxy_url
            os.environ['https_proxy'] = self.proxy_url
            os.environ['HTTP_PROXY'] = self.proxy_url
            os.environ['HTTPS_PROXY'] = self.proxy_url
        else:
            for key in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
                os.environ.pop(key, None)

    def get_available_models(self):
        if not self.api_key: return []
        self.apply_proxy()
        genai.configure(api_key=self.api_key)
        models = []
        try:
            for m in genai.list_models():
                if 'generateContent' in m.supported_generation_methods:
                    name = m.name.replace('models/', '')
                    models.append(name)
            return sorted(models)
        except Exception as e:
            print(f"List models error: {e}")
            return []

state = AppState()

# === 核心任务逻辑 ===

# ✅ 辅助：获取当前任务应该扫描的路径
def get_task_scan_dir():
    # 确保路径存在，否则回退到根目录
    if state.task_target_path and os.path.exists(state.task_target_path):
        return state.task_target_path
    return state.music_dir

def run_task_wrapper(task_id):
    """任务运行包装器"""
    target = get_task_scan_dir()
    state.log(f"开始执行任务: {task_id} (目标: {target})")
    try:
        if task_id == "dedupe_quality":
            task_dedupe_quality(target)
        elif task_id == "clean_short":
            task_clean_short(target)
        elif task_id == "extract_meta":
            task_extract_meta(target)
        elif task_id == "clean_junk":
            task_clean_junk(target)
        
        state.tasks_config[task_id]["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        state.save_config()
        state.log(f"任务完成: {task_id}")
    except Exception as e:
        state.log(f"任务 {task_id} 失败: {str(e)}")

# ✅ 修改所有任务函数，接收 target_dir 参数

# 任务1：音质去重
def task_dedupe_quality(target_dir):
    deleted_count = 0
    for root, _, files in os.walk(target_dir):
        groups = {}
        for f in files:
            if f.lower().endswith(('.mp3', '.flac', '.wav', '.m4a', '.wma')):
                base_name = os.path.splitext(f)[0]
                full_path = os.path.join(root, f)
                if base_name not in groups: groups[base_name] = []
                groups[base_name].append(full_path)
        
        for base_name, paths in groups.items():
            if len(paths) > 1:
                def quality_score(path):
                    ext = os.path.splitext(path)[1].lower()
                    size = os.path.getsize(path)
                    score = 0
                    if ext in ['.flac', '.wav']: score = 3
                    elif ext in ['.m4a', '.aac']: score = 2
                    elif ext == '.mp3': score = 1
                    return (score, size)
                
                paths.sort(key=quality_score)
                keeper = paths[-1]
                to_delete = paths[:-1]
                
                for p in to_delete:
                    try:
                        os.remove(p)
                        state.log(f"[音质去重] 删除: {os.path.basename(p)} (保留: {os.path.basename(keeper)})")
                        deleted_count += 1
                    except Exception as e:
                        state.log(f"删除失败 {p}: {e}")
    state.log(f"音质去重完成，共删除 {deleted_count} 个文件")

# 任务2：删除短音频
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
                        state.log(f"[短音频清理] 删除: {f} (时长: {int(duration)}s)")
                        deleted_count += 1
                except Exception as e:
                    pass
    state.log(f"短音频清理完成，共删除 {deleted_count} 个文件")

# 任务3：元数据提取
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
                        nfo_content = f"""<?xml version="1.0" encoding="utf-8" standalone="yes"?>
<musicvideo>
  <title>{meta['title'] or base_name}</title>
  <artist>{meta['artist']}</artist>
  <album>{meta['album']}</album>
  <plot></plot>
  <runtime>{int(meta['duration']/60)}:{meta['duration']%60:02d}</runtime>
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
                            audio = MP3(path, ID3=EasyID3)
                            if audio.tags:
                                for key in audio.tags.keys():
                                    if key.startswith('APIC:'):
                                        art_data = audio.tags[key].data
                                        break
                        elif f.lower().endswith('.flac'):
                            audio = FLAC(path)
                            if audio.pictures:
                                art_data = audio.pictures[0].data

                        if art_data:
                            with open(cover_target, "wb") as img_file:
                                img_file.write(art_data)
                            state.log(f"[元数据] 提取封面: {os.path.basename(cover_target)}")

                except Exception as e:
                    pass
    state.log(f"元数据提取完成")

# 任务4：垃圾清理
def task_clean_junk(target_dir):
    cleaned_count = 0
    music_exts = {'.mp3', '.flac', '.wav', '.m4a', '.wma', '.ape', '.ogg'}
    junk_exts = {'.nfo', '.jpg', '.jpeg', '.png', '.lrc', '.txt'}
    
    for root, dirs, files in os.walk(target_dir):
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
                    except: pass
            
            if not os.listdir(root):
                try:
                    os.rmdir(root)
                    state.log(f"[垃圾清理] 删除空目录: {root}")
                except: pass

    state.log(f"垃圾清理完成，清理 {cleaned_count} 个文件")

# === 辅助函数 (保持不变) ===
def get_dir_structure(current_path=None):
    if not current_path: target_dir = state.music_dir
    else: target_dir = current_path
    if not os.path.abspath(target_dir).startswith(os.path.abspath(state.music_dir)): target_dir = state.music_dir
    dirs = []
    try:
        with os.scandir(target_dir) as it:
            for entry in it:
                if entry.is_dir() and not entry.name.startswith('.'):
                    dirs.append({"path": entry.path, "name": entry.name})
    except Exception as e: print(f"Dir scan error: {e}")
    dirs.sort(key=lambda x: x['name'].lower())
    return {"current_path": target_dir, "is_root": os.path.abspath(target_dir) == os.path.abspath(state.music_dir), "parent_path": os.path.dirname(target_dir), "subdirs": dirs}

def get_metadata(path):
    filename = os.path.basename(path)
    size_mb = round(os.path.getsize(path) / (1024 * 1024), 2)
    tags = {}; duration = 0; bitrate = 0
    try:
        if path.lower().endswith('.mp3'):
            try: audio = MP3(path, ID3=EasyID3)
            except ID3NoHeaderError: audio = MP3(path); audio.add_tags()
            tags = audio; duration = int(audio.info.length); bitrate = int(audio.info.bitrate / 1000)
        elif path.lower().endswith('.flac'):
            audio = FLAC(path); tags = audio; duration = int(audio.info.length); bitrate = int(audio.info.bitrate / 1000)
    except: pass
    def get_tag_display(key):
        values = tags.get(key, []); valid_values = [str(v).strip() for v in values if v]
        return " / ".join(valid_values) if valid_values else ""
    artist = get_tag_display('artist'); album_artist = get_tag_display('albumartist'); title = get_tag_display('title'); album = get_tag_display('album')
    if not title:
        base = os.path.splitext(filename)[0]
        if " - " in base: parts = base.split(" - "); artist = parts[0] if not artist else artist; title = parts[1]
        else: title = base
    search_text = f"{artist} {album_artist} {title} {filename}".lower()
    return {"id": hash(path), "path": path, "filename": filename, "artist": artist.strip(), "album_artist": album_artist.strip(), "title": title.strip(), "album": album.strip(), "duration": duration, "size_mb": size_mb, "bitrate": bitrate, "search_text": search_text}

def batch_update_metadata(file_paths, artist=None, album_artist=None, title=None, album=None):
    updated_count = 0
    for path in file_paths:
        if not os.path.exists(path): continue
        try:
            audio = None
            if path.lower().endswith('.mp3'): audio = EasyID3(path)
            elif path.lower().endswith('.flac'): audio = FLAC(path)
            if audio is not None:
                if artist: audio['artist'] = artist
                if album_artist: audio['albumartist'] = album_artist
                if title: audio['title'] = title
                if album: audio['album'] = album
                audio.save(); updated_count += 1
                for f in state.files:
                    if f['path'] == path:
                        if artist: f['artist'] = artist
                        if album_artist: f['album_artist'] = album_artist
                        if title: f['title'] = title
                        if album: f['album'] = album
                        break
        except: pass
    return updated_count

def batch_rename_files(file_paths, pattern="{artist} - {title}"):
    renamed_count = 0
    for path in file_paths:
        if not os.path.exists(path): continue
        meta = next((f for f in state.files if f['path'] == path), None); 
        if not meta: meta = get_metadata(path)
        def fmt(t): return t.replace(" / ", " & ").replace("/", " & ")
        def sanitize(t): return t.replace("\\","_").replace("/","_").replace(":","-").replace("*","").replace("?","").replace("\"","'").replace("<","(").replace(">",")").replace("|","_")
        safe_artist = sanitize(fmt(meta['artist'])) or "Unknown"
        safe_album_artist = sanitize(fmt(meta['album_artist'])) or "Unknown"
        safe_title = sanitize(meta['title']) or sanitize(meta['filename'])
        safe_album = sanitize(meta['album']) or "Unknown"
        ext = os.path.splitext(path)[1]
        new_name = pattern.replace("{artist}", safe_artist).replace("{album_artist}", safe_album_artist).replace("{title}", safe_title).replace("{album}", safe_album) + ext
        dir_name = os.path.dirname(path); new_path = os.path.join(dir_name, new_name)
        if path != new_path:
            try:
                os.rename(path, new_path); renamed_count += 1
                if meta: meta['path'] = new_path; meta['filename'] = new_name
            except: pass
    return renamed_count

def fix_single_metadata_ai(path):
    if not state.api_key: return {"error": "API Key Missing"}
    if not os.path.exists(path): return {"error": "File not found"}
    state.apply_proxy(); genai.configure(api_key=state.api_key); model = genai.GenerativeModel(state.model_name)
    meta = get_metadata(path)
    prompt = f"""I have a music file: "{meta['filename']}". Tags: Artist="{meta['artist']}", Album Artist="{meta['album_artist']}", Title="{meta['title']}", Album="{meta['album']}". Role: Expert Music Librarian. Task: Infer correct metadata. Return JSON ONLY: {{ "artist": "string", "album_artist": "string", "title": "string", "album": "string" }}"""
    try:
        resp = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
        ai_data = json.loads(resp.text)
        batch_update_metadata([path], ai_data.get('artist'), ai_data.get('album_artist'), ai_data.get('title'), ai_data.get('album'))
        return {"success": True, "data": ai_data}
    except Exception as e: return {"error": str(e)}

def task_scan_and_group(target_path=None):
    state.status = "scanning"
    if target_path is None: state.files = []; scan_dir = state.music_dir
    else: state.files = [f for f in state.files if not f['path'].startswith(target_path)]; scan_dir = target_path
    state.candidates = []; state.results = []; file_list = []
    for root, _, filenames in os.walk(scan_dir):
        for filename in filenames:
            if filename.lower().endswith(('.mp3', '.flac', '.m4a', '.wma')): file_list.append(os.path.join(root, filename))
    state.total = len(file_list); state.message = f"在 {os.path.basename(scan_dir) or '根目录'} 发现 {state.total} 个文件..."
    temp_files = []
    for idx, f_path in enumerate(file_list):
        if idx % 50 == 0: state.progress = idx + 1
        temp_files.append(get_metadata(f_path))
    state.files.extend(temp_files); state.message = "正在进行模糊聚类..."
    sorted_files = sorted(state.files, key=lambda x: x['search_text']); candidates = []
    if not sorted_files: state.status = "idle"; return
    current_group = [sorted_files[0]]
    for i in range(1, len(sorted_files)):
        prev = current_group[0]; curr = sorted_files[i]; state.progress = i
        if fuzz.token_set_ratio(prev['search_text'], curr['search_text']) > 80: current_group.append(curr)
        else:
            if len(current_group) > 1: candidates.append(current_group)
            current_group = [curr]
    if len(current_group) > 1: candidates.append(current_group)
    state.candidates = candidates; state.status = "idle"; state.message = f"扫描完成，发现 {len(state.candidates)} 组疑似重复。"

def task_analyze_with_gemini():
    if not state.api_key: state.status = "error"; state.message = "API Key 未配置"; return
    state.apply_proxy(); state.status = "analyzing"; state.results = []
    try:
        genai.configure(api_key=state.api_key); model = genai.GenerativeModel(state.model_name)
        total_groups = len(state.candidates); batch_size = 5 
        for i in range(0, total_groups, batch_size):
            batch = state.candidates[i:i+batch_size]; state.progress = i; state.total = total_groups
            state.message = f"正在请求 AI ({state.model_name})... 进度 {i}/{total_groups}"
            prompt_data = [{"group_id": i + idx, "files": [{k: v for k, v in f.items() if k not in ['path', 'search_text']} for f in group]} for idx, group in enumerate(batch)]
            prompt = f"""Identify duplicates. Rules: 1. Different extensions -> DUPLICATE. 2. "Live", "Remix" -> DUPLICATE. 3. Different songs -> NOT DUPLICATE. Input: {json.dumps(prompt_data)} Return JSON: {{ "results": [ {{ "group_id": int, "is_duplicate": bool, "reason": "string" }} ] }}"""
            try:
                resp = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
                ai_res = json.loads(resp.text)
                for res in ai_res.get("results", []):
                    if res.get("is_duplicate"):
                        gid = res["group_id"]
                        if gid < len(state.candidates): state.results.append({"files": state.candidates[gid], "reason": res.get("reason", "AI 判定重复")})
                time.sleep(1) 
            except Exception as e: print(f"AI Batch Error: {e}")
        state.status = "done"; state.message = f"分析完成。共确认 {len(state.results)} 组重复文件。"
    except Exception as e: state.status = "error"; state.message = f"AI 初始化失败: {str(e)}"

def start_scan_thread(target_path=None):
    t = threading.Thread(target=task_scan_and_group, args=(target_path,)); t.start()

def start_analyze_thread():
    t = threading.Thread(target=task_analyze_with_gemini); t.start()

def delete_file(path):
    try:
        if os.path.exists(path): os.remove(path); state.files = [f for f in state.files if f['path'] != path]; return True
    except: return False
