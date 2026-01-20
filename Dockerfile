FROM python:3.11-slim

WORKDIR /app

# 安装 ffmpeg 依赖（如果 mutagen 需要处理特定格式）
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制整个 app 目录
COPY app ./app

# 暴露端口
EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
