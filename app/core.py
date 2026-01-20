import os
import json
import threading
import time
import google.generativeai as genai
from mutagen.easyid3 import EasyID3
from mutagen.mp3 import MP3
from mutagen.flac import FLAC
from thefuzz import fuzz # 引入模糊匹配库

# 数据存储路径
DATA_DIR = "/data"
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")

class AppState:
    def __init__(self):
        self.api_key = ""
        self.model_name = "gemini-1.5-flash"
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
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r') as f:
                    config = json.load(f)
                    self.api_key = config.get("api_key", "")
                    self.model_name = config.get("model_name", "gemini-1.5-flash")
            except Exception as e:
                print(f"Error loading config: {e}")

    def save_config(self):
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
    
    # 如果没有标签，尝试从文件名解析
    if not artist and not title:
        base = os.path.splitext(filename)[0]
        # 尝试常见格式 "Artist - Title"
        if " - " in base:
            parts = base.split(" - ")
            artist = parts[0]
            title = parts[1] if len(parts) > 1 else base
        else:
            title = base

    # 构造用于模糊搜索的文本 (包含歌手、标题、文件名)
    # 这样即使元数据不全，文件名相似也能匹配
    search_text = f"{artist} {title} {filename}".lower()

    return {
        "id": hash(path),
        "path": path,
        "filename": filename,
        "artist": artist.strip(),
        "title": title.strip(),
        "duration": duration,
        "size_mb": size_mb,
        "bitrate": bitrate,
        "search_text": search_text # 核心字段：用于排序和比对
    }

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
        # 每处理 50 个文件更新一次进度，减少开销
        if idx % 50 == 0:
            state.progress = idx + 1
        temp_files.append(get_metadata(f_path))
    
    state.files = temp_files
    state.message = "正在进行模糊聚类 (智能排序+相似度对比)..."
    
    # --- 核心改进：模糊匹配算法 ---
    
    # 1. 按照 search_text 排序
    # 这一步非常关键：它将相似的文件排在相邻位置
    # 例如：["adele hello", "adele hello live", "backstreet boys"]
    sorted_files = sorted(state.files, key=lambda x: x['search_text'])
    
    candidates = []
    if not sorted_files:
        state.status = "idle"
        return

    current_group = [sorted_files[0]]
    
    # 2. 遍历排序后的列表，比较相邻项
    for i in range(1, len(sorted_files)):
        prev = current_group[0] # 拿当前组的第一个作为基准
        curr = sorted_files[i]
        
        state.progress = i
        
        # 使用 token_set_ratio:
        # 它自动处理单词乱序和子集问题。
        # 例如: "周杰伦 晴天" vs "晴天 周杰伦" -> Score 100
        # "Hello" vs "Hello (Live)" -> Score 100 (因为 Hello 是集合的子集)
        similarity = fuzz.token_set_ratio(prev['search_text'], curr['search_text'])
        
        # 阈值设定为 80，稍微宽松一点，把筛选交给 AI
        if similarity > 80:
            current_group.append(curr)
        else:
            # 如果当前组超过1个文件，说明有疑似重复，保存下来
            if len(current_group) > 1:
                candidates.append(current_group)
            # 开启新的一组
            current_group = [curr]
            
    # 处理最后一组
    if len(current_group) > 1:
        candidates.append(current_group)
    
    state.candidates = candidates
    state.status = "idle"
    state.message = f"扫描完成。基于标题相似度，发现 {len(state.candidates)} 组疑似重复。"

def task_analyze_with_gemini():
    if not state.api_key:
        state.status = "error"
        state.message = "API Key 未配置"
        return

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
                    "files": [{k: v for k, v in f.items() if k != 'path' and k != 'search_text'} for f in group]
                })

            prompt = f"""
            I have grouped these music files because they have SIMILAR filenames or tags.
            Your task is to be the judge: Are they actually the same song (duplicates)?
            
            Rules:
            1. Different file extensions (mp3 vs flac) of the same song -> DUPLICATE.
            2. "Live", "Remix", "Instrumental" versions vs Original -> DUPLICATE (I want to see them grouped).
            3. Completely different songs (false positive fuzzy match) -> NOT DUPLICATE.
            
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
