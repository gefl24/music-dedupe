import os
import json
import threading
import time
import google.generativeai as genai
from mutagen.easyid3 import EasyID3
from mutagen.mp3 import MP3
from mutagen.flac import FLAC

# 数据存储路径
DATA_DIR = "/data"
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")

class AppState:
    def __init__(self):
        self.api_key = ""
        self.model_name = "gemini-1.5-flash"  # 默认模型
        self.music_dir = "/music"
        self.status = "idle" 
        self.progress = 0
        self.total = 0
        self.message = "准备就绪"
        self.files = []       
        self.candidates = []  
        self.results = []     
        self.load_config()

    def load_config(self):
        """加载持久化配置"""
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r') as f:
                    config = json.load(f)
                    self.api_key = config.get("api_key", "")
                    self.model_name = config.get("model_name", "gemini-1.5-flash")
            except Exception as e:
                print(f"Error loading config: {e}")

    def save_config(self):
        """保存配置到文件"""
        try:
            if not os.path.exists(DATA_DIR):
                os.makedirs(DATA_DIR)
            with open(CONFIG_FILE, 'w') as f:
                json.dump({
                    "api_key": self.api_key,
                    "model_name": self.model_name
                }, f)
        except Exception as e:
            print(f"Error saving config: {e}")

state = AppState()

def get_metadata(path):
    # ... (保持原有的 get_metadata 逻辑不变，为了节省篇幅省略) ...
    filename = os.path.basename(path)
    size_mb = round(os.path.getsize(path) / (1024 * 1024), 2)
    tags = {}
    duration = 0
    bitrate = 0
    try:
        if path.lower().endswith('.mp3'):
            audio = MP3(path, ID3=EasyID3)
            tags = audio
            duration = int(audio.info.length)
            bitrate = int(audio.info.bitrate / 1000)
        elif path.lower().endswith('.flac'):
            audio = FLAC(path)
            tags = audio
            duration = int(audio.info.length)
            bitrate = int(audio.info.bitrate / 1000)
    except:
        pass
    artist = tags.get('artist', [''])[0]
    title = tags.get('title', [''])[0]
    if not artist or not title:
        base = os.path.splitext(filename)[0]
        if " - " in base:
            parts = base.split(" - ")
            artist = parts[0]
            title = parts[1] if len(parts) > 1 else base
        else:
            title = base
    return {
        "id": hash(path),
        "path": path,
        "filename": filename,
        "artist": artist.strip(),
        "title": title.strip(),
        "duration": duration,
        "size_mb": size_mb,
        "bitrate": bitrate
    }

def task_scan_and_group():
    state.status = "scanning"
    state.files = []
    state.candidates = []
    state.results = [] # 清空上次结果
    
    file_list = []
    for root, _, filenames in os.walk(state.music_dir):
        for filename in filenames:
            if filename.lower().endswith(('.mp3', '.flac', '.m4a')):
                file_list.append(os.path.join(root, filename))
    
    state.total = len(file_list)
    state.message = f"发现 {state.total} 个文件，正在提取元数据..."
    
    temp_files = []
    for idx, f_path in enumerate(file_list):
        state.progress = idx + 1
        temp_files.append(get_metadata(f_path))
    
    state.files = temp_files
    state.message = "正在进行本地模糊分组..."
    
    groups = {}
    for item in state.files:
        clean_name = "".join(filter(str.isalnum, (item['artist'] + item['title']).lower()))
        if len(clean_name) < 5: 
             clean_name = "".join(filter(str.isalnum, item['filename'].lower()))
        
        if clean_name not in groups:
            groups[clean_name] = []
        groups[clean_name].append(item)
    
    state.candidates = [v for k, v in groups.items() if len(v) > 1]
    
    state.status = "idle"
    state.message = f"扫描完成。本地发现 {len(state.candidates)} 组疑似重复。"

def task_analyze_with_gemini():
    if not state.api_key:
        state.status = "error"
        state.message = "API Key 未配置"
        return

    state.status = "analyzing"
    state.results = []
    
    try:
        genai.configure(api_key=state.api_key)
        # 使用配置的模型
        model = genai.GenerativeModel(state.model_name)
        
        total_groups = len(state.candidates)
        batch_size = 5 # 减小一点 batch 以防大模型 token 超出
        
        for i in range(0, total_groups, batch_size):
            batch = state.candidates[i:i+batch_size]
            state.progress = i
            state.total = total_groups
            state.message = f"正在请求 AI ({state.model_name})... 进度 {i}/{total_groups}"
            
            prompt_data = []
            for idx, group in enumerate(batch):
                prompt_data.append({
                    "group_id": i + idx,
                    "files": [{k: v for k, v in f.items() if k != 'path'} for f in group]
                })

            prompt = f"""
            Identify duplicates in these music groups. 
            Ignore purely format extensions if the song is the same.
            Input: {json.dumps(prompt_data)}
            Return JSON object with "results": [ {{ "group_id": int, "is_duplicate": bool, "reason": string }} ]
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
