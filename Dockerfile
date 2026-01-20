FROM python:3.9-slim

WORKDIR /app

# 设置时区和编码，防止中文乱码
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

# 创建数据目录挂载点
RUN mkdir /data

CMD ["python", "app.py"]
