import os
import json
import re
import html
import google.generativeai as genai
from mutagen.easyid3 import EasyID3
from mutagen.mp3 import MP3
from mutagen.flac import FLAC
from pathlib import Path
from tqdm import tqdm
import time

# --- 配置 ---
API_KEY = os.getenv("GEMINI_API_KEY")
MUSIC_DIR = "/music"
DATA_DIR = "/data"
LIBRARY_FILE = os.path.join(DATA_DIR, "library_cache.json")
CANDIDATES_FILE = os.path.join(DATA_DIR, "candidates.json")
REPORT_FILE = os.path.join(DATA_DIR, "duplicates_report.html")

genai.configure(api_key=API_KEY)
model = genai.GenerativeModel('gemini-1.5-flash')

class MusicScanner:
    def __init__(self, directory):
        self.directory = directory
        self.files = []

    def scan(self):
        if os.path.exists(LIBRARY_FILE):
            print(f"[1/3] Loading cached library from {LIBRARY_FILE}...")
            with open(LIBRARY_FILE, 'r', encoding='utf-8') as f:
                self.files = json.load(f)
            print(f"Loaded {len(self.files)} files from cache.")
            return

        print(f"[1/3] Scanning directory: {self.directory} (This may take a while for 30k files)...")
        file_list = []
        for root, _, filenames in os.walk(self.directory):
            for filename in filenames:
                if filename.lower().endswith(('.mp3', '.flac', '.m4a', '.wma')):
                    file_list.append(os.path.join(root, filename))

        print(f"Found {len(file_list)} files. Extracting metadata...")
        
        for idx, file_path in enumerate(tqdm(file_list, unit="file")):
            try:
                meta = self._get_metadata(file_path)
                meta['id'] = idx
                self.files.append(meta)
            except Exception:
                continue # Skip corrupt files
        
        # Save cache
        with open(LIBRARY_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.files, f, ensure_ascii=False)
        print("Scan complete and cached.")

    def _get_metadata(self, path):
        tags = {}
        duration = 0
        size_mb = round(os.path.getsize(path) / (1024 * 1024), 2)
        filename = os.path.basename(path)
        
        try:
            if path.lower().endswith('.mp3'):
                audio = MP3(path, ID3=EasyID3)
                tags = audio
                duration = int(audio.info.length)
            elif path.lower().endswith('.flac'):
                audio = FLAC(path)
                tags = audio
                duration = int(audio.info.length)
            # Add more formats if needed
        except:
            pass
        
        artist = tags.get('artist', [''])[0]
        title = tags.get('title', [''])[0]
        
        # 如果没有标签，尝试从文件名解析 (Artist - Title.mp3)
        if not artist and not title:
            base = os.path.splitext(filename)[0]
            if " - " in base:
                parts = base.split(" - ")
                artist = parts[0]
                title = parts[1]
            else:
                title = base

        return {
            "path": path,
            "filename": filename,
            "artist": artist.strip(),
            "title": title.strip(),
            "duration": duration,
            "size_mb": size_mb,
            "bitrate": "Unknown" # Getting exact bitrate can be slow, skipping for speed
        }

class DedupeGrouper:
    def __init__(self, music_list):
        self.music_list = music_list
        self.candidates = []

    def _normalize(self, text):
        # 移除括号内容 (Live), [HQ] 等，移除标点，转小写
        text = re.sub(r'[\(\[].*?[\)\]]', '', text) 
        text = re.sub(r'[^\w\s]', '', text)
        return text.lower().replace(" ", "")

    def group(self):
        print(f"[2/3] Grouping {len(self.music_list)} files locally...")
        
        groups = {}
        
        for item in tqdm(self.music_list, unit="file"):
            # 构造指纹：归一化的 "artist+title" 和 "title+artist"
            # 解决 "周杰伦 - 稻香" 和 "稻香 - 周杰伦" 的问题
            parts = [self._normalize(item['artist']), self._normalize(item['title'])]
            parts.sort() # 排序，确保 A+B 和 B+A 是一样的
            key = "".join(parts)
            
            # 如果Key太短（比如只有文件名），可能误判，暂且加入
            if len(key) < 3: 
                key = self._normalize(item['filename'])

            if key not in groups:
                groups[key] = []
            groups[key].append(item)

        # 筛选出数量 > 1 的组
        self.candidates = [items for k, items in groups.items() if len(items) > 1]
        
        print(f"Found {len(self.candidates)} groups of potential duplicates.")
        
        # Save candidates
        with open(CANDIDATES_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.candidates, f, ensure_ascii=False)
        return self.candidates

class AIVerifier:
    def __init__(self, candidates):
        self.candidates = candidates
        self.confirmed_duplicates = []

    def verify(self):
        print(f"[3/3] Asking Gemini to verify {len(self.candidates)} groups...")
        print("Note: Processing in batches of 10 groups to save API calls.")
        
        batch_size = 10
        chunks = [self.candidates[i:i + batch_size] for i in range(0, len(self.candidates), batch_size)]
        
        for chunk_index, chunk in enumerate(tqdm(chunks, unit="batch")):
            # 构造 Prompt
            # 我们发给 AI 多个组，让它在组内进行判断
            prompt_data = []
            for idx, group in enumerate(chunk):
                group_data = {
                    "group_id": idx,
                    "files": [{
                        "id": f['id'],
                        "artist": f['artist'],
                        "title": f['title'],
                        "filename": f['filename'],
                        "size": f"{f['size_mb']}MB",
                        "duration": f"{f['duration']}s"
                    } for f in group]
                }
                prompt_data.append(group_data)

            prompt = f"""
            I have grouped some music files based on fuzzy name matching. 
            Please analyze the metadata and tell me if they are actual duplicates or different versions (e.g. Instrumental, Live, Remix).
            
            Input Data:
            {json.dumps(prompt_data, ensure_ascii=False)}
            
            Task:
            For each group, identify which files are duplicates.
            Return a JSON object:
            {{
                "results": [
                    {{
                        "group_id": 0,
                        "is_duplicate": true,
                        "best_file_id": 123,  // Recommendation: highest quality or standard version
                        "reason": "Same song, file A is higher quality",
                        "duplicate_ids": [123, 456] 
                    }}
                ]
            }}
            If a group contains completely different songs (false positive), set "is_duplicate": false.
            """

            try:
                response = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
                result = json.loads(response.text)
                
                # 处理返回结果
                for res in result.get("results", []):
                    if res.get("is_duplicate"):
                        # 找回原始文件信息
                        original_group = chunk[res["group_id"]]
                        self.confirmed_duplicates.append({
                            "reason": res.get("reason"),
                            "files": original_group,
                            "best_id": res.get("best_file_id")
                        })
                
                # 简单的限流保护
                time.sleep(1) 

            except Exception as e:
                print(f"Batch {chunk_index} failed: {e}")
                # 发生错误时，我们可以选择跳过或者记录，这里简单的跳过
                continue

        self._generate_html_report()

    def _generate_html_report(self):
        print("Generating HTML report...")
        html_content = """
        <html>
        <head>
            <title>Music Deduplication Report</title>
            <style>
                body { font-family: sans-serif; background: #f4f4f4; padding: 20px; }
                .group { background: white; padding: 15px; margin-bottom: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
                .file { display: flex; align-items: center; padding: 8px; border-bottom: 1px solid #eee; }
                .file:last-child { border-bottom: none; }
                .best { background-color: #e6fffa; }
                .badge { background: #eee; padding: 2px 6px; border-radius: 4px; font-size: 0.8em; margin-left: 10px; }
                .path { font-family: monospace; color: #666; font-size: 0.9em; margin-left: auto; }
            </style>
        </head>
        <body>
            <h1>Duplicate Report</h1>
            <p>Found {} groups of duplicates.</p>
        """.format(len(self.confirmed_duplicates))

        for entry in self.confirmed_duplicates:
            html_content += f"<div class='group'><p><strong>AI Note:</strong> {entry['reason']}</p>"
            for f in entry['files']:
                is_best = f['id'] == entry.get('best_id')
                style = "class='file best'" if is_best else "class='file'"
                badge = "<span class='badge'>KEEP (Recommended)</span>" if is_best else ""
                
                html_content += f"""
                <div {style}>
                    <div>
                        <strong>{html.escape(f['filename'])}</strong><br>
                        <small>{f['size_mb']}MB | {f['duration']}s | {html.escape(f['artist'])} - {html.escape(f['title'])}</small>
                        {badge}
                    </div>
                    <div class='path'>{f['path']}</div>
                </div>
                """
            html_content += "</div>"
        
        html_content += "</body></html>"
        
        with open(REPORT_FILE, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"Report generated at: {REPORT_FILE}")

def main():
    if not API_KEY:
        print("Error: GEMINI_API_KEY is not set.")
        return

    # 1. 扫描 (支持缓存)
    scanner = MusicScanner(MUSIC_DIR)
    scanner.scan()
    
    if not scanner.files:
        print("No files found.")
        return

    # 2. 本地粗筛
    grouper = DedupeGrouper(scanner.files)
    candidates = grouper.group()

    if not candidates:
        print("No fuzzy duplicates found locally.")
        return

    # 3. AI 确认
    verifier = AIVerifier(candidates)
    verifier.verify()

if __name__ == "__main__":
    main()
