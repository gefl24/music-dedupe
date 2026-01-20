# ✅ 多阶段构建，减少镜像大小
FROM python:3.11-slim as builder

WORKDIR /tmp

# 安装编译依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 生成 wheels
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /tmp/wheels -r requirements.txt

# ✅ 最终阶段
FROM python:3.11-slim

WORKDIR /app

# 安装运行依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 复制 wheels 并安装
COPY --from=builder /tmp/wheels /wheels
COPY requirements.txt .
RUN pip install --no-cache /wheels/* && rm -rf /wheels

# 创建必要目录
RUN mkdir -p /data /music && \
    chown -R 1000:1000 /app /data

# 复制应用代码
COPY app ./app

# 暴露端口
EXPOSE 8000

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1

# 非 root 用户运行
USER 1000

# 启动命令
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
