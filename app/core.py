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
        self.files = []       
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
                audio.add_tags() # 添加空标签
            tags = audio
            duration = int(audio.info.length)
            bitrate = int(audio.info.bitrate / 1000)
        elif path.lower().endswith('.flac'):
            audio = FLAC(path)
            tags = audio
            duration = int(audio.info.length)
            bitrate = int(audio.info.bitrate / 1000)
    except Exception as e:
        print(f"Error reading {path}: {e}")
    
    artist = tags.get('artist', [''])[0]
    title = tags.get('title', [''])[0]
    album = tags.get('album', [''])[0] # ✅ 新增读取专辑
    
    if not artist and not title:
        base = os.path.splitext(filename)[0]
        if " - " in base:
            parts = base.split(" - ")
            artist = parts[0]
            title = parts[1] if len(parts) > 1 else base
        else:
            title = base

    search_text = f"{artist} {title} {filename}".lower()

    return {
        "id": hash(path),
        "path": path,
        "filename": filename,
        "artist": artist.strip(),
        "title": title.strip(),
        "album": album.strip(), # ✅ 新增
        "duration": duration,
        "size_mb": size_mb,
        "bitrate": bitrate,
        "search_text": search_text 
    }

# ✅ 新增：批量更新元数据
def batch_update_metadata(file_paths, artist=None, title=None, album=None):
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
                if title: audio['title'] = title
                if album: audio['album'] = album
                audio.save()
                updated_count += 1
                
                # 更新内存中的缓存
                for f in state.files:
                    if f['path'] == path:
                        if artist: f['artist'] = artist
                        if title: f['title'] = title
                        if album: f['album'] = album
                        break
        except Exception as e:
            print(f"Update tag error {path}: {e}")
    return updated_count

# ✅ 新增：批量重命名
def batch_rename_files(file_paths, pattern="{artist} - {title}"):
    renamed_count = 0
    for path in file_paths:
        if not os.path.exists(path): continue
        
        # 找到内存中的元数据
        meta = next((f for f in state.files if f['path'] == path), None)
        if not meta: 
            meta = get_metadata(path) # 如果缓存里没有，重新读一遍

        # 简单的安全检查
        safe_artist = meta['artist'].replace("/", "_").replace("\\", "_") or "Unknown"
        safe_title = meta['title'].replace("/", "_").replace("\\", "_") or meta['filename']
        safe_album = meta['album'].replace("/", "_").replace("\\", "_") or "Unknown"

        ext = os.path.splitext(path)[1]
        
        # 生成新文件名
        new_name = pattern.replace("{artist}", safe_artist)\
                          .replace("{title}", safe_title)\
                          .replace("{album}", safe_album) + ext
        
        dir_name = os.path.dirname(path)
        new_path = os.path.join(dir_name, new_name)

        if path != new_path:
            try:
                os.rename(path, new_path)
                renamed_count += 1
                # 更新内存引用
                if meta:
                    meta['path'] = new_path
                    meta['filename'] = new_name
            except OSError as e:
                print(f"Rename failed: {e}")
    
    return renamed_count

def task_scan_and_group():
    state.status = "scanning"
    state.files = []
    state.candidates = []
    state.results = [] 
    
    file_list = []
    for root, _, filenames in os.walk(state.music_dir):
        for filename in filenames:
            if filename.lower().endswith(('.mp3', '.flac', '.m4a', '.wma')):
                file_list.append(os.path.join(root, filename))
    
    state.total = len(file_list)
    state.message = f"发现 {state.total} 个文件，正在提取元数据..."
    
    temp_files = []
    for idx, f_path in enumerate(file_list):
        if idx % 50 == 0: state.progress = idx + 1
        temp_files.append(get_metadata(f_path))
    
    state.files = temp_files
    state.message = "正在进行模糊聚类..."
    
    # ... (保持原有的模糊聚类逻辑不变) ...
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
    state.message = f"扫描完成。发现 {len(state.candidates)} 组疑似重复。"

def task_analyze_with_gemini():
    # ... (保持原有的 task_analyze_with_gemini 逻辑完全不变) ...
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

def start_scan_thread():
    t = threading.Thread(target=task_scan_and_group)
    t.start()

def start_analyze_thread():
    t = threading.Thread(target=task_analyze_with_gemini)
    t.start()

def delete_file(path):
    try:
        if os.path.exists(path):
            os.remove(path)
            return True
    except Exception as e:
        print(f"Delete error: {e}")
    return False
