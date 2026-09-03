#!/bin/bash
# 双击启动：本地代理 server.js + Serveo HTTPS 隧道（供 GitHub Pages 在线页使用豆包流式识别）
cd "$(dirname "$0")"
echo "============================================================"
echo " 反方作战台 · 启动本地代理 + 公网隧道"
echo " 关闭本窗口即停止全部服务"
echo "============================================================"

# 1) 启动本地代理（如已在运行则跳过）
if ! curl -s -o /dev/null --max-time 2 http://127.0.0.1:8787/config-status; then
  echo "[1/2] 启动本地代理 server.js …"
  node server.js &
  sleep 2
else
  echo "[1/2] 本地代理已在运行，跳过"
fi

# 2) 启动 Serveo 隧道（macOS 自带 ssh，无需注册；免费版每次地址会变）
echo "[2/2] 建立 HTTPS 隧道 …"
echo "（出现 Forwarding 那一行的 https 地址后，复制下面生成的在线链接）"
echo ""
ssh -o StrictHostKeyChecking=no -o HostKeyAlgorithms=+ssh-rsa -o ServerAliveInterval=30 -R 80:127.0.0.1:8787 serveo.net | while read -r line; do
  echo "$line"
  URL=$(echo "$line" | grep -o 'https://[^ ]*serveousercontent.com')
  if [ -n "$URL" ]; then
    OPEN="https://xiaohao-ai.github.io/debate-copilot/?proxy=${URL}"
    echo ""
    echo "============================================================"
    echo " 隧道已通！在 Chrome 打开下面这个链接（已自动配好代理与豆包识别）："
    echo "$OPEN"
    echo "$OPEN" | pbcopy
    echo "（链接已复制到剪贴板，可直接发给队友；队友打开即走你的代理）"
    echo "============================================================"
  fi
done
