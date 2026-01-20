import os
import json
import threading
import time
import google.generativeai as genai
from mutagen.easyid3 import EasyID3
from mutagen.mp3 import MP3
from mutagen.flac import FLAC
from thefuzz import fuzz

# 全局状态管理
class AppState:
    def __init__(self):
        self.api_key = ""
        self.music_dir = "/music"
        self.status = "idle" # idle, scanning, analyzing, done
        self.progress = 0
        self.total = 0
        self.message = ""
        self.files = []       # 所有文件元数据
        self.candidates = []  # 本地模糊匹配出的候选组
        self.results = []     # AI 分析后的结果

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
    
    # 文件名回退机制
    if not artist or not title:
        base = os.path.splitext(filename)[0]
        if " - " in base:
            parts = base.split(" - ")
            artist = parts[0]
            title = parts[1] if len(parts) > 1 else base
        else:
            title = base

    return {
        "id": hash(path), # 简单的 ID
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
    
    file_list = []
    for root, _, filenames in os.walk(state.music_dir):
        for filename in filenames:
            if filename.lower().endswith(('.mp3', '.flac', '.m4a')):
                file_list.append(os.path.join(root, filename))
    
    state.total = len(file_list)
    state.message = f"Found {state.total} files. Extracting metadata..."
    
    temp_files = []
    for idx, f_path in enumerate(file_list):
        state.progress = idx + 1
        temp_files.append(get_metadata(f_path))
    
    state.files = temp_files
    state.message = "Grouping files locally..."
    
    # 简单的模糊分组逻辑
    groups = {}
    for item in state.files:
        # 创建简化的 Key: "artist+title" (去除特殊字符)
        clean_name = "".join(filter(str.isalnum, (item['artist'] + item['title']).lower()))
        if len(clean_name) < 5: # 如果 Key 太短，使用文件名
             clean_name = "".join(filter(str.isalnum, item['filename'].lower()))
        
        if clean_name not in groups:
            groups[clean_name] = []
        groups[clean_name].append(item)
    
    # 只保留 > 1 的组
    state.candidates = [v for k, v in groups.items() if len(v) > 1]
    
    state.status = "idle"
    state.message = f"Scan complete. Found {len(state.candidates)} potential duplicate groups."

def task_analyze_with_gemini():
    if not state.api_key:
        state.status = "error"
        state.message = "API Key missing"
        return

    state.status = "analyzing"
    state.results = []
    genai.configure(api_key=state.api_key)
    model = genai.GenerativeModel('gemini-1.5-flash')
    
    total_groups = len(state.candidates)
    batch_size = 10
    
    # 分批处理
    for i in range(0, total_groups, batch_size):
        batch = state.candidates[i:i+batch_size]
        state.progress = i
        state.total = total_groups
        state.message = f"Asking Gemini (Batch {i//batch_size + 1})..."
        
        prompt_data = []
        for idx, group in enumerate(batch):
            prompt_data.append({
                "group_id": i + idx,
                "files": [{k: v for k, v in f.items() if k != 'path'} for f in group] # 不发路径给AI，省流量
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
                    # 找到原始文件数据
                    gid = res["group_id"]
                    # 这里的逻辑需要根据实际索引匹配，简化处理：
                    # 我们假设顺序一致，或者你需要更健壮的 ID 匹配
                    # 这里为了演示，我们直接在 state.candidates 里找
                    
                    # 修正：直接用索引匹配可能不准，但在单线程下凑合，
                    # 生产环境建议用 UUID 匹配 group
                    if gid < len(state.candidates):
                         state.results.append({
                             "files": state.candidates[gid],
                             "reason": res.get("reason", "Duplicate")
                         })
            time.sleep(1) # 限流
        except Exception as e:
            print(f"AI Error: {e}")
            
    state.status = "done"
    state.message = "Analysis complete."

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
