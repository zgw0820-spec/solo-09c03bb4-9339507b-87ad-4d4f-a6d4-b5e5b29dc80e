"""共享核心：配置加载、状态存储（flock 互斥）、路由操作、回退逻辑。

管理 API（app.main）与看门狗（app.watchdog）是两个相互独立的进程，
都通过本模块操作同一份状态文件。这样即使管理 API 进程停止，
看门狗仍能在试切到期时自行回退路由。
"""
from __future__ import annotations

import contextlib
import fcntl
import json
import os
import subprocess
import time
from datetime import datetime, timezone

STATE_DIR = os.environ.get("TSW_STATE_DIR", "/state")
STATE_FILE = os.path.join(STATE_DIR, "state.json")
LOCK_FILE = os.path.join(STATE_DIR, "state.lock")
CONFIG_FILE = os.environ.get("TSW_CONFIG", "/app/config.yaml")

ACTIVE_STATUSES = ("trialing", "probing")      # 进行中的试切（占用试切槽位）
TERMINAL_STATUSES = ("kept", "rolled_back")    # 终态
HISTORY_LIMIT = 50

REASON_PROBE_FAILED = "probe_failed"       # 确认后探测失败 -> 立即回退
REASON_DEADLINE = "deadline_exceeded"      # 到期仍未确认成功 -> 自动回退


def now_iso(ts: float | None = None) -> str:
    return datetime.fromtimestamp(
        ts if ts is not None else time.time(), tz=timezone.utc
    ).isoformat()


def load_config() -> dict:
    import yaml

    with open(CONFIG_FILE, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    # 环境变量覆盖（本地联调 / 容器注入用）
    env = os.environ
    if env.get("TSW_TARGET"):
        raw["target"] = env["TSW_TARGET"]
    for env_key, cfg_key, cast in (
        ("TSW_PROBE_PORT", "probe_port", int),
        ("TSW_PROBE_ATTEMPTS", "probe_attempts", int),
        ("TSW_PROBE_TIMEOUT_SEC", "probe_timeout_sec", float),
        ("TSW_PROBE_INTERVAL_SEC", "probe_interval_sec", float),
    ):
        if env.get(env_key):
            raw[cfg_key] = cast(env[env_key])
    return raw


class Config:
    def __init__(self, raw: dict):
        self.target = raw["target"]                       # 采集端 IPv4
        self.probe_port = int(raw.get("probe_port", 9000))
        self.probe_attempts = int(raw.get("probe_attempts", 3))
        self.probe_timeout = float(raw.get("probe_timeout_sec", 1.5))
        self.probe_interval = float(raw.get("probe_interval_sec", 0.3))
        self.nexthops = dict(raw.get("nexthops", {}))     # 预置下一跳：名称 -> IPv4
        # ip     = 真实执行 iproute2（容器内）
        # dryrun = 只打印命令并维护模拟路由表（本地无 NET_ADMIN 联调用）
        self.route_backend = os.environ.get("TSW_ROUTE_BACKEND", "ip")


class StateStore:
    """JSON 状态文件 + flock 互斥，供 API 与看门狗两个进程安全读写。"""

    def __init__(self, path: str = STATE_FILE):
        self.path = path
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        if not os.path.exists(self.path):
            self._write({"current": None, "history": []})

    @contextlib.contextmanager
    def locked(self):
        """read-modify-write 临界区。正常退出时落盘；抛异常则丢弃改动。"""
        os.makedirs(os.path.dirname(LOCK_FILE), exist_ok=True)
        with open(LOCK_FILE, "w", encoding="utf-8") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                state = self._read()
                yield state
                self._write(state)
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def _read(self) -> dict:
        try:
            with open(self.path, encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"current": None, "history": []}

    def _write(self, state: dict) -> None:
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)  # 原子替换，避免看门狗读到半写文件


class RouteManager:
    """对「通往采集端的 IPv4 主机路由」的实际操作。"""

    def __init__(self, target: str, backend: str = "ip"):
        self.target = target
        self.backend = backend
        self.sim_file = os.path.join(STATE_DIR, "dryrun_route.json")

    def get(self) -> dict:
        """当前去往采集端的路由（nexthop 为 None 表示直连/无主机路由）。"""
        if self.backend == "dryrun":
            sim = self._sim_read()
            return {"target": self.target, "nexthop": sim.get("nexthop"),
                    "dev": "sim0", "backend": "dryrun"}
        out = subprocess.run(
            ["ip", "-j", "-4", "route", "get", self.target],
            capture_output=True, text=True, check=True,
        )
        data = json.loads(out.stdout)[0]
        return {"target": self.target, "nexthop": data.get("gateway"),
                "dev": data.get("dev"), "raw": data, "backend": "ip"}

    def install_host_route(self, nexthop: str) -> None:
        """试切换路：替换通往采集端的 /32 主机路由。"""
        if self.backend == "dryrun":
            print(f"[dryrun-route] ip -4 route replace {self.target}/32 via {nexthop}",
                  flush=True)
            self._sim_write({"target": self.target, "nexthop": nexthop})
            return
        subprocess.run(
            ["ip", "-4", "route", "replace", f"{self.target}/32", "via", nexthop],
            capture_output=True, text=True, check=True,
        )

    def restore(self, original_nexthop: str | None) -> None:
        """回退：恢复试切前的下一跳；原来没有主机路由则删除试切路由。"""
        if original_nexthop:
            if self.backend == "dryrun":
                print(f"[dryrun-route] ip -4 route replace {self.target}/32 "
                      f"via {original_nexthop}", flush=True)
                self._sim_write({"target": self.target, "nexthop": original_nexthop})
                return
            subprocess.run(
                ["ip", "-4", "route", "replace", f"{self.target}/32",
                 "via", original_nexthop],
                capture_output=True, text=True, check=True,
            )
        else:
            if self.backend == "dryrun":
                print(f"[dryrun-route] ip -4 route del {self.target}/32", flush=True)
                self._sim_write({"target": self.target, "nexthop": None})
                return
            subprocess.run(
                ["ip", "-4", "route", "del", f"{self.target}/32"],
                capture_output=True, text=True,  # 路由已不存在时忽略错误
            )

    def _sim_read(self) -> dict:
        try:
            with open(self.sim_file, encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"target": self.target, "nexthop": None}

    def _sim_write(self, sim: dict) -> None:
        os.makedirs(os.path.dirname(self.sim_file), exist_ok=True)
        with open(self.sim_file, "w", encoding="utf-8") as f:
            json.dump(sim, f)


# ---- 以下函数必须在 StateStore.locked() 临界区内调用 ----

def rollback_locked(state: dict, routes: RouteManager, reason: str) -> dict | None:
    """恢复原下一跳，把当前试切以 rolled_back 终态移入历史。幂等。"""
    cur = state.get("current")
    if not cur or cur["status"] not in ACTIVE_STATUSES:
        return None
    err = None
    try:
        routes.restore(cur.get("original_nexthop"))
    except Exception as exc:  # noqa: BLE001 - 回退路由失败也要落终态并记录
        err = f"{type(exc).__name__}: {exc}"
    cur["status"] = "rolled_back"
    cur["rollback_reason"] = reason
    cur["finished_at"] = now_iso()
    cur["finished_ts"] = time.time()
    if err:
        cur["route_restore_error"] = err
    state["history"].insert(0, cur)
    del state["history"][HISTORY_LIMIT:]
    state["current"] = None
    return cur


def keep_locked(state: dict) -> dict | None:
    """确认+探测均在期限内成功：保留新路由，试切以 kept 终态移入历史。"""
    cur = state.get("current")
    if not cur or cur["status"] not in ACTIVE_STATUSES:
        return None
    cur["status"] = "kept"
    cur["finished_at"] = now_iso()
    cur["finished_ts"] = time.time()
    state["history"].insert(0, cur)
    del state["history"][HISTORY_LIMIT:]
    state["current"] = None
    return cur


def expire_if_needed(state: dict, routes: RouteManager) -> dict | None:
    """到期仍未确认成功 -> 自动回退（无论探测是否仍在进行中）。"""
    cur = state.get("current")
    if cur and cur["status"] in ACTIVE_STATUSES and time.time() > cur["deadline_ts"]:
        return rollback_locked(state, routes, REASON_DEADLINE)
    return None
