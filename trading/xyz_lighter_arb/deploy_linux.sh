#!/bin/bash
# deploy_linux.sh - 在 Linux 服务器上部署 CTP 交易环境
#
# 使用方法:
#   1. 将整个项目目录上传到 Linux 服务器
#   2. chmod +x deploy_linux.sh
#   3. ./deploy_linux.sh
#
# 要求: Linux x86_64, Python 3.9+

set -e

echo "========================================"
echo "  国贸期货 CTP + Hyperliquid 部署脚本"
echo "========================================"

# 检查 Python
PYTHON=$(command -v python3 || true)
if [ -z "$PYTHON" ]; then
    echo "错误: 未找到 python3"
    exit 1
fi
echo "Python: $($PYTHON --version)"

# 检查架构
ARCH=$(uname -m)
if [ "$ARCH" != "x86_64" ]; then
    echo "错误: CTP 需要 x86_64 架构, 当前为 $ARCH"
    exit 1
fi
echo "架构: $ARCH ✓"

# 创建虚拟环境
echo ""
echo "创建虚拟环境..."
$PYTHON -m venv venv
source venv/bin/activate

# 安装依赖
echo "安装 Python 依赖..."
pip install --upgrade pip
pip install -r requirements.txt
pip install openctp-ctp==6.7.0.9

# 设置 CTP 库路径
CTP_LIB_DIR="$(pwd)/ctp_libs"
if [ -d "$CTP_LIB_DIR" ]; then
    echo "设置 CTP 库路径: $CTP_LIB_DIR"
    export LD_LIBRARY_PATH="$CTP_LIB_DIR:$LD_LIBRARY_PATH"
    echo "export LD_LIBRARY_PATH=\"$CTP_LIB_DIR:\$LD_LIBRARY_PATH\"" >> venv/bin/activate
else
    echo "警告: ctp_libs 目录不存在, 将使用 openctp-ctp 内置库"
fi

# 创建 .env 文件 (如果不存在)
if [ ! -f .env ]; then
    echo ""
    echo "创建 .env 配置文件..."
    cp .env.example .env
    echo "请编辑 .env 文件填入你的账号信息:"
    echo "  vim .env"
fi

# 创建必要目录
mkdir -p logs data

echo ""
echo "========================================"
echo "  部署完成!"
echo "========================================"
echo ""
echo "下一步:"
echo "  1. 编辑 .env 填入账号信息"
echo "  2. source venv/bin/activate"
echo "  3. python3 test_ctp_connection.py    # 测试 CTP 连接"
echo "  4. python3 arbitrage_bot.py          # 启动交易机器人"
echo ""
