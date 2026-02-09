#!/bin/bash
# 定时任务运行脚本

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 设置 Playwright 浏览器路径（手动上传的浏览器）
export PLAYWRIGHT_BROWSERS_PATH="$SCRIPT_DIR/browsers"

source venv/bin/activate
python run_once.py
