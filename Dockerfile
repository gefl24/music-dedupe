# 多阶段构建 - 优化镜像大小
FROM python:3.11-slim as builder

WORKDIR /tmp

# 安装编译依赖 (合并 RUN 减少层数)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# 复制依赖文件
COPY requirements.txt .

# 升级 pip 并生成 wheels (优化缓存)
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip wheel --no-cache-dir --no-deps --wheel-dir /tmp/wheels -r requirements.txt

# =========================
# 最终运行阶段
# =========================
FROM python:3.11-slim

# 设置环境变量
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# 安装运行时依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# 复制 wheels 并安装
COPY --from=builder /tmp/wheels /wheels
COPY requirements.txt .
RUN pip install --no-cache-dir /wheels/* && \
    rm -rf /wheels && \
    pip cache purge

# 创建必要目录并设置权限
RUN mkdir -p /data /music /app/templates && \
    useradd -u 1000 -m -s /bin/bash appuser && \
    chown -R appuser:appuser /app /data /music

# 复制应用代码
COPY --chown=appuser:appuser app ./app

# 暴露端口
EXPOSE 8000

# 健康检查 (优化参数)
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1

# 切换到非 root 用户
USER appuser

# 启动命令 (使用更合理的 worker 数量)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2", "--log-level", "info"]
