import os
import json
import threading
import time
import google.generativeai as genai
from mutagen.easyid3 import EasyID3
from mutagen.mp3 import MP3
from mutagen.flac import FLAC
from mutagen.id3 import ID3NoHeaderError
from thefuzz import fuzz

DATA_DIR = "/data"
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")

class AppState:
    def __init__(self):
        self.api_key = ""
        self.model_name = "gemini-1.5-flash"
        self.proxy_url = ""
        self.music_dir = "/music"
        self.status = "idle" 
        self.progress = 0
        self.total = 0
        self.message = "准备就绪"
        self.files = []       # 内存中的文件缓存
        self.candidates = []  
        self.results = []     
        self.load_config()
        self.apply_proxy()

    def load_config(self):
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r') as f:
                    config = json.load(f)
                    self.api_key = config.get("api_key", "").strip()
                    self.model_name = config.get("model_name", "gemini-1.5-flash").strip()
                    self.proxy_url = config.get("proxy_url", "").strip()
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
                    "proxy_url": self.proxy_url
                }, f)
            self.apply_proxy()
        except Exception as e:
            print(f"Error saving config: {e}")

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

# ✅ 新增：极速获取目录结构（不扫描文件）
def get_dir_structure():
    dirs = []
    base_len = len(state.music_dir)
    try:
        # 使用 os.walk 但只关注文件夹
        for root, subdirs, _ in os.walk(state.music_dir):
            # 获取相对路径名称
            rel_path = root[base_len:]
            if rel_path.startswith('/'): rel_path = rel_path[1:]
            if not rel_path: rel_path = "/ (根目录)"
            
            # 计算层级，简单的缩进显示
            dirs.append({
                "path": root,
                "name": rel_path,
                "short_name": os.path.basename(root) or "根目录"
            })
    except Exception as e:
        print(f"Dir scan error: {e}")
    
    # 按路径排序
    return sorted(dirs, key=lambda x: x['path'])

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
            bitrate = int(audio.info.bitrate / 1000)
        elif path.lower().endswith('.flac'):
            audio = FLAC(path)
            tags = audio
            duration = int(audio.info.length)
            bitrate = int(audio.info.bitrate / 1000)
    except Exception as e:
        pass
    
    def get_tag_display(key):
        values = tags.get(key, [])
        valid_values = [str(v).strip() for v in values if v]
        if not valid_values: return ""
        return " / ".join(valid_values)

    artist = get_tag_display('artist')
    album_artist = get_tag_display('albumartist')
    title = get_tag_display('title')
    album = get_tag_display('album')
    
    if not title:
        base = os.path.splitext(filename)[0]
        if " - " in base:
            parts = base.split(" - ")
            if not artist: artist = parts[0]
            title = parts[1] if len(parts) > 1 else base
        else:
            title = base

    search_text = f"{artist} {album_artist} {title} {filename}".lower()

    return {
        "id": hash(path),
        "path": path,
        "filename": filename,
        "artist": artist.strip(),
        "album_artist": album_artist.strip(),
        "title": title.strip(),
        "album": album.strip(),
        "duration": duration,
        "size_mb": size_mb,
        "bitrate": bitrate,
        "search_text": search_text 
    }

def batch_update_metadata(file_paths, artist=None, album_artist=None, title=None, album=None):
    updated_count = 0
    for path in file_paths:
        if not os.path.exists(path): continue
        try:
            audio = None
            if path.lower().endswith('.mp3'):
                audio = EasyID3(path)
            elif path.lower().endswith('.flac'):
                audio = FLAC(path)
            
            if audio is not None:
                if artist: audio['artist'] = artist
                if album_artist: audio['albumartist'] = album_artist
                if title: audio['title'] = title
                if album: audio['album'] = album
                audio.save()
                updated_count += 1
                
                for f in state.files:
                    if f['path'] == path:
                        if artist: f['artist'] = artist
                        if album_artist: f['album_artist'] = album_artist
                        if title: f['title'] = title
                        if album: f['album'] = album
                        break
        except Exception as e:
            print(f"Update tag error {path}: {e}")
    return updated_count

def batch_rename_files(file_paths, pattern="{artist} - {title}"):
    renamed_count = 0
    for path in file_paths:
        if not os.path.exists(path): continue
        meta = next((f for f in state.files if f['path'] == path), None)
        if not meta: meta = get_metadata(path)

        def format_for_filename(text):
            return text.replace(" / ", " & ").replace("/", " & ")

        def sanitize(text):
            return text.replace("\\", "_").replace("/", "_") \
                       .replace(":", "-").replace("*", "") \
                       .replace("?", "").replace("\"", "'") \
                       .replace("<", "(").replace(">", ")") \
                       .replace("|", "_")

        raw_artist = format_for_filename(meta['artist'])
        raw_album_artist = format_for_filename(meta['album_artist'])
        
        safe_artist = sanitize(raw_artist) or "Unknown"
        safe_album_artist = sanitize(raw_album_artist) or "Unknown"
        safe_title = sanitize(meta['title']) or sanitize(meta['filename'])
        safe_album = sanitize(meta['album']) or "Unknown"

        ext = os.path.splitext(path)[1]
        
        new_name = pattern.replace("{artist}", safe_artist)\
                          .replace("{album_artist}", safe_album_artist)\
                          .replace("{title}", safe_title)\
                          .replace("{album}", safe_album) + ext
        
        dir_name = os.path.dirname(path)
        new_path = os.path.join(dir_name, new_name)

        if path != new_path:
            try:
                os.rename(path, new_path)
                renamed_count += 1
                if meta:
                    meta['path'] = new_path
                    meta['filename'] = new_name
            except OSError as e:
                print(f"Rename failed: {e}")
    return renamed_count

def fix_single_metadata_ai(path):
    if not state.api_key: return {"error": "API Key Missing"}
    if not os.path.exists(path): return {"error": "File not found"}
    
    state.apply_proxy()
    genai.configure(api_key=state.api_key)
    model = genai.GenerativeModel(state.model_name)
    
    meta = get_metadata(path)
    
    prompt = f"""
    I have a music file: "{meta['filename']}".
    Current Tags -> Artist: "{meta['artist']}", Album Artist: "{meta['album_artist']}", Title: "{meta['title']}", Album: "{meta['album']}".
    
    Role: Expert Music Librarian.
    Task: Infer and correct metadata based on filename and common knowledge.
    
    Guidelines:
    1. "Artist": The specific performer(s). If multiple, separate with " / ".
    2. "Album Artist": The main artist of the album.
    3. Fix typos.
    
    Return JSON ONLY:
    {{
        "artist": "string",
        "album_artist": "string",
        "title": "string",
        "album": "string"
    }}
    """
    
    try:
        resp = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
        ai_data = json.loads(resp.text)
        
        batch_update_metadata(
            [path], 
            ai_data.get('artist'), 
            ai_data.get('album_artist'), 
            ai_data.get('title'), 
            ai_data.get('album')
        )
        
        return {"success": True, "data": ai_data}
    except Exception as e:
        return {"error": str(e)}

# ✅ 修改：增加 target_path 支持按文件夹扫描
def task_scan_and_group(target_path=None):
    state.status = "scanning"
    # 注意：这里不清空 state.files，而是进行 更新/追加
    # 如果是全量扫描 (target_path=None)，则清空
    if target_path is None:
        state.files = []
        scan_dir = state.music_dir
    else:
        # 如果是子文件夹，先移除该文件夹下的旧数据（防止重复），再追加
        state.files = [f for f in state.files if not f['path'].startswith(target_path)]
        scan_dir = target_path

    state.candidates = []
    state.results = [] 
    
    file_list = []
    # 遍历
    for root, _, filenames in os.walk(scan_dir):
        for filename in filenames:
            if filename.lower().endswith(('.mp3', '.flac', '.m4a', '.wma')):
                file_list.append(os.path.join(root, filename))
    
    state.total = len(file_list)
    state.message = f"在 {os.path.basename(scan_dir) or '根目录'} 发现 {state.total} 个文件，提取元数据..."
    
    temp_files = []
    for idx, f_path in enumerate(file_list):
        if idx % 50 == 0: state.progress = idx + 1
        temp_files.append(get_metadata(f_path))
    
    # 合并到主列表
    state.files.extend(temp_files)
    
    state.message = "正在进行模糊聚类 (全局)..."
    
    # 模糊聚类 (对所有已加载的文件进行)
    sorted_files = sorted(state.files, key=lambda x: x['search_text'])
    candidates = []
    if not sorted_files:
        state.status = "idle"
        return
    current_group = [sorted_files[0]]
    for i in range(1, len(sorted_files)):
        prev = current_group[0] 
        curr = sorted_files[i]
        state.progress = i
        similarity = fuzz.token_set_ratio(prev['search_text'], curr['search_text'])
        if similarity > 80:
            current_group.append(curr)
        else:
            if len(current_group) > 1: candidates.append(current_group)
            current_group = [curr]
    if len(current_group) > 1: candidates.append(current_group)
    
    state.candidates = candidates
    state.status = "idle"
    state.message = f"扫描完成。共加载 {len(state.files)} 个文件，发现 {len(state.candidates)} 组疑似重复。"

def task_analyze_with_gemini():
    # ... (保持不变) ...
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
        
        total_groups = len(state.candidates)
        batch_size = 5 
        
        for i in range(0, total_groups, batch_size):
            batch = state.candidates[i:i+batch_size]
            state.progress = i
            state.total = total_groups
            state.message = f"正在请求 AI ({state.model_name})... 进度 {i}/{total_groups}"
            
            prompt_data = []
            for idx, group in enumerate(batch):
                prompt_data.append({
                    "group_id": i + idx,
                    "files": [{k: v for k, v in f.items() if k not in ['path', 'search_text']} for f in group]
                })

            prompt = f"""
            Identify duplicates.
            Rules:
            1. Different extensions (mp3 vs flac) -> DUPLICATE.
            2. "Live", "Remix" vs Original -> DUPLICATE.
            3. Different songs -> NOT DUPLICATE.
            
            Input: {json.dumps(prompt_data)}
            Return JSON object with "results": [ {{ "group_id": int, "is_duplicate": bool, "reason": "brief explanation" }} ]
            """
            try:
                resp = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
                ai_res = json.loads(resp.text)
                for res in ai_res.get("results", []):
                    if res.get("is_duplicate"):
                        gid = res["group_id"]
                        if gid < len(state.candidates):
                             state.results.append({
                                 "files": state.candidates[gid],
                                 "reason": res.get("reason", "AI 判定重复")
                             })
                time.sleep(1) 
            except Exception as e:
                print(f"AI Batch Error: {e}")
                
        state.status = "done"
        state.message = f"分析完成。共确认 {len(state.results)} 组重复文件。"
        
    except Exception as e:
        state.status = "error"
        state.message = f"AI 初始化失败: {str(e)}"

def start_scan_thread(target_path=None):
    t = threading.Thread(target=task_scan_and_group, args=(target_path,))
    t.start()

def start_analyze_thread():
    t = threading.Thread(target=task_analyze_with_gemini)
    t.start()

def delete_file(path):
    try:
        if os.path.exists(path):
            os.remove(path)
            state.files = [f for f in state.files if f['path'] != path]
            return True
    except Exception as e:
        print(f"Delete error: {e}")
    return False
