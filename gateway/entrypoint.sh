#!/bin/sh
# 网关容器入口：
#   1. 安装通往采集端的初始主机路由（走主用下一跳）
#   2. 启动独立看门狗进程（到期自动回退，不依赖管理 API）
#   3. 启动管理 API（uvicorn）
# 看门狗与 API 互为独立进程：API 停止不影响看门狗回退；看门狗异常退出会被重新拉起。

STATE_DIR="${TSW_STATE_DIR:-/state}"
mkdir -p "$STATE_DIR"

echo "[entrypoint] install initial host route: ${TARGET_IP}/32 via ${PRIMARY_NEXTHOP}"
ip route replace "${TARGET_IP}/32" via "${PRIMARY_NEXTHOP}"

cd /app

python3 -m app.watchdog &
WATCHDOG_PID=$!

uvicorn app.main:app --host 0.0.0.0 --port 8000 &
API_PID=$!

trap 'kill -TERM "$API_PID" "$WATCHDOG_PID" 2>/dev/null; exit 0' TERM INT

while true; do
    sleep 2
    if ! kill -0 "$WATCHDOG_PID" 2>/dev/null; then
        echo "[entrypoint] watchdog exited, restarting it"
        python3 -m app.watchdog &
        WATCHDOG_PID=$!
    fi
    if ! kill -0 "$API_PID" 2>/dev/null; then
        echo "[entrypoint] management API is DOWN; watchdog keeps guarding active trials"
    fi
done
