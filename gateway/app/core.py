"""共享核心：配置加载、状态存储（flock 互斥）、路由操作、回退逻辑。

管理 API（app.main）与看门狗（app.watchdog）是两个相互独立的进程，
都通过本模块操作同一份状态文件。这样即使管理 API 进程停止，
看门狗仍能在试切到期时自行回退路由。

试切发起的交接协议（修复「新试切记错回退目标」与「换路后、存档前退出」）：

1. 所有「判定到期 -> 读换路前实际下一跳 -> 写换路意图标记 -> 实际换路
   -> 写试切记录」都在同一把 flock 临界区内完成。看门狗的到期回退也持
   同一把锁，因此不可能插在「读取原下一跳」与「换路」之间把路由恢复掉，
   每笔试切记录的 original_nexthop 必然是它自己换路前一刻实际在用的下一跳。
2. 换路*之前*先把 pending_switch（撤回凭据：新/旧下一跳、试切号）原子落盘。
   若 API 在换路生效后、试切记录保存前退出，flock 随进程死亡释放，看门狗
   拿到锁后会看到无主的 pending_switch，随即按其中的 original_nexthop
   独立撤回这次换路，并留一条 api_interrupted 历史记录。
3. 试用计时的起点 switched_ts 取在换路成功返回之后，慢换路占用的时间不计入试用期。
"""
from __future__ import annotations

import contextlib
import fcntl
import json
import os
import signal
import subprocess
import time
from datetime import datetime, timezone

STATE_DIR = os.environ.get("TSW_STATE_DIR", "/state")
STATE_FILE = os.path.join(STATE_DIR, "state.json")
LOCK_FILE = os.path.join(STATE_DIR, "state.lock")
CONFIG_FILE = os.environ.get("TSW_CONFIG", "/app/config.yaml")

ACTIVE_STATUSES = ("trialing", "probing")      # 进行中的试切（占用试切槽位）
TERMINAL_STATUSES = ("kept", "rolled_back")    # 终态
# 换路事务「半提交」的中间态：若 API 在这些状态下死亡，看门狗应立即撤回路由。
# 正常本实现使用独立的 pending_switch 标记、不会把 current 停在这些状态；
# 保留这层识别是为了兼容其它写法/外部种入的状态文件。
INTERMEDIATE_STATUSES = (
    "switching", "switch_pending", "pending_switch", "pending", "initiating",
)
HISTORY_LIMIT = 50

REASON_PROBE_FAILED = "probe_failed"       # 确认后探测失败 -> 立即回退
REASON_DEADLINE = "deadline_exceeded"      # 到期仍未确认成功 -> 自动回退
REASON_MANUAL = "manual_rollback"          # 值班员要求提前回退
REASON_API_INTERRUPTED = "api_interrupted"  # API 换路后存档前退出 -> 看门狗撤回


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
            self._write({"current": None, "pending_switch": None, "history": []})

    @contextlib.contextmanager
    def locked(self):
        """read-modify-write 临界区。正常退出时落盘；抛异常则丢弃未落盘改动。

        注意：临界区内若需要在执行危险副作用（换路）*之前*先持久化凭据，
        应显式调用 :meth:`commit`；仅靠退出时落盘覆盖不到「进程被杀死」。
        """
        os.makedirs(os.path.dirname(LOCK_FILE), exist_ok=True)
        with open(LOCK_FILE, "w", encoding="utf-8") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                state = self._read()
                yield state
                self._write(state)
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def commit(self, state: dict) -> None:
        """在当前 flock 临界区内立即原子落盘（写换路意图标记等）。"""
        self._write(state)

    def _read(self) -> dict:
        try:
            with open(self.path, encoding="utf-8") as f:
                state = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            state = {"current": None, "history": []}
        # 兼容旧状态文件 / 写入一半的结构
        state.setdefault("current", None)
        state.setdefault("pending_switch", None)
        state.setdefault("history", [])
        return state

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
        # 故障注入 / 演练钩子（默认全部不触发；详见 README「故障演练」）。
        # 文件型钩子可用 `docker compose exec gateway touch /state/xxx`
        # 在不重启 API 的情况下动态触发。
        self.fail_file = os.environ.get(
            "TSW_FAIL_SWITCH_FILE", os.path.join(STATE_DIR, "fail_switch"))
        self.crash_file = os.environ.get(
            "TSW_CRASH_SWITCH_FILE", os.path.join(STATE_DIR, "crash_after_switch"))
        self.gate_file = os.environ.get(
            "TSW_GATE_SWITCH_FILE", os.path.join(STATE_DIR, "gate_after_switch"))
        self.slow_file = os.environ.get(
            "TSW_SLOW_SWITCH_FILE", os.path.join(STATE_DIR, "slow_switch"))

    # ---- 查询 ---------------------------------------------------------

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

    def actual_nexthop(self) -> str | None:
        """get() 的容错版本：查询失败时抛出让调用方决定，不吞异常。"""
        return self.get().get("nexthop")

    # ---- 换路（带演练钩子） -------------------------------------------

    def install_host_route(self, nexthop: str) -> None:
        """试切换路：替换通往采集端的 /32 主机路由。

        钩子顺序：失败注入 -> 慢换路延迟 -> 实际换路 -> 换路后自杀/闸门。
        「换路后自杀」精确复现「换路已生效、试切记录尚未保存」的崩溃窗口。
        """
        if os.path.exists(self.fail_file):
            raise RuntimeError(
                f"[fault-injection] {self.fail_file} 存在，模拟换路失败")
        self._maybe_slow_down()
        if self.backend == "dryrun":
            print(f"[dryrun-route] ip -4 route replace {self.target}/32 via {nexthop}",
                  flush=True)
            self._sim_write({"target": self.target, "nexthop": nexthop})
        else:
            subprocess.run(
                ["ip", "-4", "route", "replace", f"{self.target}/32", "via", nexthop],
                capture_output=True, text=True, check=True,
            )
        self._maybe_crash_after_switch()
        self._wait_after_switch_gate()

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

    # ---- 演练钩子实现 -------------------------------------------------

    def _slow_delay(self) -> float:
        raw = os.environ.get("TSW_ROUTE_DELAY_SEC")
        try:
            if os.path.exists(self.slow_file):
                with open(self.slow_file, encoding="utf-8") as f:
                    raw = f.read().strip() or "2.0"
            return max(0.0, float(raw)) if raw else 0.0
        except (ValueError, OSError):
            return 0.0

    def _maybe_slow_down(self) -> None:
        delay = self._slow_delay()
        if delay:
            print(f"[fault-injection] slow route switch: sleeping {delay:.2f}s",
                  flush=True)
            time.sleep(delay)

    def _maybe_crash_after_switch(self) -> None:
        if not os.path.exists(self.crash_file):
            return
        # 一次性触发：先删触发文件，避免 API 被 entrypoint/手动重启后再次自杀
        with contextlib.suppress(OSError):
            os.remove(self.crash_file)
        print("[fault-injection] route switched; killing API BEFORE trial record "
              "is saved", flush=True)
        os.kill(os.getpid(), signal.SIGKILL)
        os._exit(137)  # 兜底

    def _wait_after_switch_gate(self) -> None:
        """换路后挂起在「记录尚未保存」的窗口里，等待外部 kill 或放行文件。"""
        if not os.path.exists(self.gate_file):
            return
        switched_flag = self.gate_file + ".switched"
        continue_file = self.gate_file + ".continue"
        try:
            with open(switched_flag, "w", encoding="utf-8") as f:
                f.write(now_iso())
        except OSError:
            pass
        timeout = float(os.environ.get("TSW_GATE_TIMEOUT_SEC", "120"))
        deadline = time.time() + timeout
        print(f"[fault-injection] gate parked after switch: {switched_flag}; "
              f"touch {continue_file} to continue or kill the API now", flush=True)
        while time.time() < deadline:
            if os.path.exists(continue_file):
                with contextlib.suppress(OSError):
                    os.remove(switched_flag)
                return
            time.sleep(0.05)
        # 超时后放行，避免请求永久挂死
        with contextlib.suppress(OSError):
            os.remove(switched_flag)

    # ---- dryrun 模拟路由表 -------------------------------------------

    def _sim_read(self) -> dict:
        try:
            with open(self.sim_file, encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"target": self.target, "nexthop": None}

    def _sim_write(self, sim: dict) -> None:
        os.makedirs(os.path.dirname(self.sim_file), exist_ok=True)
        tmp = self.sim_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(sim, f)
        os.replace(tmp, self.sim_file)  # 原子替换，GET /api/route 不会读到半截


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


def _marker_original(marker: dict):
    """兼容不同命名风格的「原下一跳」字段。"""
    for key in ("original_nexthop", "rollback_target",
                "original_next_hop", "original"):
        if key in marker:
            return marker.get(key)
    return None


def _append_interrupted_history(state: dict, rec: dict) -> None:
    rec.setdefault("status", "rolled_back")
    rec.setdefault("rollback_reason", REASON_API_INTERRUPTED)
    rec.setdefault("finished_at", now_iso())
    rec.setdefault("finished_ts", time.time())
    rec.setdefault("probe", {"state": "none"})
    state["history"].insert(0, rec)
    del state["history"][HISTORY_LIMIT:]


def reconcile_pending_switch_locked(
    state: dict, routes: RouteManager
) -> dict | None:
    """撤回「已落换路意图、但试切记录未提交」的无主换路。

    调用方必须持有 flock。能在持锁状态下看到 pending_switch，只有两种可能：
      1. 上一个执行换路的 API 进程在 commit 标记后、提交试切记录前死亡，
         flock 已由内核释放（本函数正是被独立看门狗/下一个请求调用）；
      2. 上一个请求处理异常退出了临界区但没清理标记。
    两种情况下按标记中的 original_nexthop 恢复都是幂等安全的：
    换路没真正生效时，replace 成原值也无副作用。

    另外兼容把半提交事务写成 ``current.status='switching'`` 的状态结构。
    """
    marker = state.get("pending_switch")
    recovered: dict | None = None
    if marker:
        original = _marker_original(marker)
        observed = None
        observe_err = None
        try:
            # 只用 routes.get() 这一基础鸭子接口，兼容自定义/测试路由后端
            observed = routes.get().get("nexthop")
        except Exception as exc:  # noqa: BLE001
            observe_err = f"{type(exc).__name__}: {exc}"
        restore_err = None
        try:
            routes.restore(original)
        except Exception as exc:  # noqa: BLE001 - 仍要清标记并留记录
            restore_err = f"{type(exc).__name__}: {exc}"
        rec = {
            "trial_id": marker.get("trial_id"),
            "target": marker.get("target"),
            "nexthop_name": marker.get("nexthop_name"),
            "nexthop": marker.get("nexthop"),
            "original_nexthop": original,
            "rollback_target": original,
            "started_at": marker.get("created_at"),
            "started_ts": marker.get("created_ts"),
            "observed_nexthop_on_recovery": observed,
            "route_observe_error": observe_err,
            "route_restore_error": restore_err,
            "note": (
                "管理 API 在换路意图落盘后、试切记录保存前退出/中断；"
                "看门狗或下一请求依据 pending_switch 独立撤回了本次换路"
            ),
        }
        _append_interrupted_history(state, rec)
        state["pending_switch"] = None
        recovered = rec

    # 兼容 current 停在换路中间态的写法
    cur = state.get("current")
    if cur and cur.get("status") in INTERMEDIATE_STATUSES:
        original = _marker_original(cur)
        restore_err = None
        try:
            routes.restore(original)
        except Exception as exc:  # noqa: BLE001
            restore_err = f"{type(exc).__name__}: {exc}"
        cur["original_nexthop"] = original
        cur["rollback_target"] = original
        cur["route_restore_error"] = restore_err
        cur["note"] = ("试切记录停在换路中间态 "
                       f"{cur.get('status')}，判定管理 API 已中断；看门狗独立撤回换路")
        _append_interrupted_history(state, cur)
        state["current"] = None
        recovered = cur
    return recovered
