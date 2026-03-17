# Dockerfile - CTP 交易环境 (Linux x86_64)
#
# 国贸期货 CTP 穿透式 v6.7.0 需要 Linux 环境
# 使用 openctp-ctp 6.7.0 + 国贸原生 .so 文件
#
# 构建: docker build --platform linux/amd64 -t ctp-arb .
# 运行: docker run --platform linux/amd64 --env-file .env ctp-arb

FROM --platform=linux/amd64 python:3.10-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 安装 Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir openctp-ctp==6.7.0.9

# 复制国贸期货原生 CTP .so 文件 (穿透式版本)
COPY ctp_libs/ /app/ctp_libs/
ENV LD_LIBRARY_PATH="/app/ctp_libs:${LD_LIBRARY_PATH}"

# 复制项目代码
COPY . .

# 创建数据目录
RUN mkdir -p logs data

# 默认运行测试脚本
CMD ["python3", "test_ctp_connection.py"]
