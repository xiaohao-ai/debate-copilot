#!/bin/bash
# 双击启动「局域网作战台」：启动本地代理（HTTPS），并阻止 Mac 休眠；关闭本窗口即停止
cd "$(dirname "$0")"
clear
echo "============================================================"
echo " 反方作战台 · 局域网服务启动中…"
echo " 保持本窗口开着，比赛结束再关闭"
echo "============================================================"
if ! command -v node >/dev/null 2>&1; then
  echo "未检测到 node，请先安装 Node.js（https://nodejs.org）"; read -n1; exit 1
fi
LANIP=$(ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null)
echo ""
echo " 本机访问：   https://127.0.0.1:8787"
[ -n "$LANIP" ] && echo " 队友访问：   https://$LANIP:8787（连同一 WiFi）"
echo ""
# caffeinate -i 阻止系统空闲休眠（服务期间生效）
caffeinate -is node server.js
echo ""
echo "服务已停止，按任意键关闭窗口。"
read -n1
