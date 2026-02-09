#!/bin/bash
# Water Quality Scraper - 云服务器部署脚本
# 支持 Ubuntu/Debian 系统

set -e

echo "=========================================="
echo "  Water Quality Scraper 部署脚本"
echo "=========================================="

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 检查 Python 版本
echo "[1/5] 检查 Python 环境..."
if ! command -v python3 &> /dev/null; then
    echo "未找到 Python3，正在安装..."
    sudo apt update && sudo apt install -y python3 python3-pip python3-venv
fi

PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
echo "  Python 版本: $PYTHON_VERSION"

# 创建虚拟环境
echo "[2/5] 创建虚拟环境..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "  虚拟环境已创建"
else
    echo "  虚拟环境已存在，跳过"
fi

# 激活虚拟环境并安装依赖
echo "[3/5] 安装 Python 依赖..."
source venv/bin/activate
pip install --upgrade pip -q
pip install -r requirements.txt -q
echo "  依赖安装完成"

# 安装 Playwright 浏览器
echo "[4/5] 安装 Playwright Chromium..."
playwright install chromium
echo "  正在安装系统依赖 (需要 sudo 权限)..."
playwright install-deps chromium || echo "  [警告] 系统依赖安装失败，请手动运行: playwright install-deps"

# 创建必要目录
echo "[5/5] 初始化目录结构..."
mkdir -p data/snapshots
mkdir -p logs
echo "  目录结构就绪"

echo ""
echo "=========================================="
echo "  部署完成！"
echo "=========================================="
echo ""
echo "使用方法:"
echo "  1. 手动运行一次爬虫:"
echo "     source venv/bin/activate && python run_once.py"
echo ""
echo "  2. 添加定时任务 (每两小时执行):"
echo "     crontab -e"
echo "     添加: 30 */2 * * * cd $SCRIPT_DIR && ./run_cron.sh >> logs/cron.log 2>&1"
echo ""
