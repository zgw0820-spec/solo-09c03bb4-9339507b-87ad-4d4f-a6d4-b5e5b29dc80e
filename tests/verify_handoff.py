#!/usr/bin/env python3
"""试切交接修复的端到端实测脚本（无需 Docker / NET_ADMIN）。

用与容器内完全相同的进程拓扑跑真实代码：
  - 独立看门狗进程 `python -m app.watchdog`
  - 真实 uvicorn 管理 API 进程
  - TSW_ROUTE_BACKEND=dryrun（维护模拟路由表，状态机/flock/看门狗与容器一致）

覆盖任务要求的三类实测：
  1. 回退交接：连续/并发试切，断言每笔 original_nexthop 都是换路前实际下一跳；
  2. 慢换路：TSW_ROUTE_DELAY_SEC / slow_switch 文件，断言试用期从换路成功起算；
  3. API 中断：换路后存档前 SIGKILL/故障自杀，断言 API 已停时看门狗独立撤回；
  另含：换路失败原路由仍在可重试、提前回退（含探测在途）、到期回退、
        探测成功保留、409/404 等回归。

用法：
  python tests/verify_handoff.py
退出码 0 = 全部通过。
"""
from __future__ import annotations

import fcntl
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

GATEWAY_DIR = Path(__file__).resolve().parent.parent / "gateway"
PRIMARY = "10.99.0.11"
BACKUP_GOOD = "10.99.0.12"
BACKUP_TWO = "10.99.0.14"


# ----------------------------- 基础工具 ----------------------------------

class Report:
    def __init__(self) -> None:
        self.passed: list[str] = []
        self.failed: list[tuple[str, str]] = []

    def check(self, name: str, cond: bool, detail: str = "") -> None:
        if cond:
            self.passed.append(name)
            print(f"  [PASS] {name}" + (f" — {detail}" if detail else ""), flush=True)
        else:
            self.failed.append((name, detail))
            print(f"  [FAIL] {name} — {detail}", flush=True)

    def section(self, title: str) -> None:
        print(f"\n=== {title} ===", flush=True)


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def atomic_state_mutate(state_dir: Path, fn) -> None:
    """持与 API/看门狗同一把 flock 修改 state.json（模拟时间流逝等）。"""
    lock_path = state_dir / "state.lock"
    with open(lock_path, "w", encoding="utf-8") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        p = state_dir / "state.json"
        for _ in range(50):
            try:
                state = json.loads(p.read_text(encoding="utf-8"))
                break
            except (FileNotFoundError, json.JSONDecodeError):
                time.sleep(0.02)
        else:
            raise RuntimeError("cannot read state for mutation")
        fn(state)
        tmp = p.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, p)
        fcntl.flock(lf, fcntl.LOCK_UN)


# ----------------------------- 被测网关线束 -------------------------------

class Harness:
    def __init__(self, name: str, *, probe_port: int | None = None,
                 probe_attempts: int = 3, probe_timeout: float = 0.5,
                 watchdog_interval: float = 0.05,
                 route_delay: float | None = None) -> None:
        self.name = name
        self.root = Path(tempfile.mkdtemp(prefix=f"tsw_{name}_"))
        self.state_dir = self.root / "state"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.port = free_port()
        self.probe_port = probe_port or free_port()
        self.api_log = open(self.root / "api.log", "w", encoding="utf-8")
        self.wd_log = open(self.root / "wd.log", "w", encoding="utf-8")
        cfg = {
            "target": "127.0.0.1",
            "probe_port": self.probe_port,
            "probe_attempts": probe_attempts,
            "probe_timeout_sec": probe_timeout,
            "probe_interval_sec": 0.1,
            "nexthops": {
                "primary": PRIMARY,
                "backup-good": BACKUP_GOOD,
                "backup-two": BACKUP_TWO,
                "backup-broken": "10.99.0.13",
            },
        }
        # 用 YAML 写配置（PyYAML 在被测解释器里应有）
        import yaml
        (self.root / "config.yaml").write_text(
            yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
        (self.state_dir / "dryrun_route.json").write_text(
            json.dumps({"target": cfg["target"], "nexthop": PRIMARY}),
            encoding="utf-8")
        self.env = dict(os.environ)
        self.env.update({
            "TSW_CONFIG": str(self.root / "config.yaml"),
            "TSW_STATE_DIR": str(self.state_dir),
            "TSW_ROUTE_BACKEND": "dryrun",
            "TSW_WATCHDOG_INTERVAL_SEC": str(watchdog_interval),
            "PYTHONUNBUFFERED": "1",
        })
        if route_delay is not None:
            self.env["TSW_ROUTE_DELAY_SEC"] = str(route_delay)
        self.api_proc: subprocess.Popen | None = None
        self.wd_proc: subprocess.Popen | None = None
        self.base = f"http://127.0.0.1:{self.port}"

    # -- 进程生命周期 --
    def _spawn(self, args, log):
        return subprocess.Popen(
            [sys.executable, *args], cwd=str(GATEWAY_DIR), env=self.env,
            stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
            start_new_session=True,
        )

    def start_watchdog(self) -> None:
        self.wd_proc = self._spawn(["-m", "app.watchdog"], self.wd_log)

    def start_api(self, wait_health: bool = True) -> None:
        self.api_proc = self._spawn(
            ["-m", "uvicorn", "app.main:app", "--host", "127.0.0.1",
             "--port", str(self.port), "--log-level", "warning"],
            self.api_log)
        if wait_health:
            self.wait_health(timeout=15)

    def start_all(self) -> None:
        self.start_watchdog()
        self.start_api()

    def _kill_group(self, p: subprocess.Popen | None, sig: int) -> None:
        if p is None:
            return
        try:
            os.killpg(os.getpgid(p.pid), sig)
        except (ProcessLookupError, PermissionError):
            pass

    def stop(self) -> None:
        for p in (self.api_proc, self.wd_proc):
            if p and p.poll() is None:
                self._kill_group(p, signal.SIGTERM)
        deadline = time.time() + 5
        while time.time() < deadline:
            alive = [p for p in (self.api_proc, self.wd_proc)
                     if p and p.poll() is None]
            if not alive:
                break
            time.sleep(0.1)
        for p in (self.api_proc, self.wd_proc):
            if p and p.poll() is None:
                self._kill_group(p, signal.SIGKILL)
        for log in (self.api_log, self.wd_log):
            with contextlib_suppress():
                log.close()

    def kill_api9(self) -> int | None:
        assert self.api_proc is not None
        self._kill_group(self.api_proc, signal.SIGKILL)
        try:
            return self.api_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            return None

    # -- HTTP --
    def http(self, method: str, path: str, body=None, timeout: float = 10):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode()
                try:
                    return resp.status, (json.loads(raw) if raw else None)
                except json.JSONDecodeError:
                    return resp.status, raw
        except urllib.error.HTTPError as e:
            raw = e.read().decode()
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = raw
            return e.code, payload
        except (urllib.error.URLError, ConnectionError, OSError) as e:
            return 0, {"transport_error": f"{type(e).__name__}: {e}"}

    def wait_health(self, timeout: float = 15) -> None:
        end = time.time() + timeout
        last = None
        while time.time() < end:
            status, body = self.http("GET", "/api/health", timeout=2)
            if status == 200:
                return
            last = (status, body)
            time.sleep(0.15)
        raise RuntimeError(f"API did not become healthy: {last}")

    # -- 状态/路由观察 --
    def sim_nexthop(self):
        try:
            return json.loads(
                (self.state_dir / "dryrun_route.json").read_text()
            ).get("nexthop")
        except FileNotFoundError:
            return None

    def read_state(self) -> dict:
        return json.loads((self.state_dir / "state.json").read_text())

    def wait_route(self, expected, timeout: float = 6) -> bool:
        end = time.time() + timeout
        while time.time() < end:
            if self.sim_nexthop() == expected:
                return True
            time.sleep(0.05)
        return self.sim_nexthop() == expected

    def wait_history_reason(self, reason: str, timeout: float = 6):
        end = time.time() + timeout
        while time.time() < end:
            try:
                for t in self.read_state().get("history", []):
                    if t.get("rollback_reason") == reason:
                        return t
            except (FileNotFoundError, json.JSONDecodeError):
                pass
            time.sleep(0.05)
        return None

    def touch(self, name: str, content: str | None = None) -> Path:
        p = self.state_dir / name
        if content is not None:
            p.write_text(content, encoding="utf-8")
        else:
            p.touch()
        return p


def contextlib_suppress():
    import contextlib
    return contextlib.suppress(Exception)


# ----------------------------- 场景 --------------------------------------

def scenario_handoff(report: Report) -> None:
    report.section("1. 回退交接：连续 / 边界 / 并发试切，回退目标必须始终是主下一跳")
    h = Harness("handoff")
    try:
        h.start_all()

        # 1a. 连续 8 轮：上一笔标记到期 -> 立刻发起下一笔 -> 立即提前回退
        ids = []
        for i in range(8):
            s, t = h.http("POST", "/api/trials",
                          {"nexthop": "backup-good", "duration_sec": 5})
            report.check(f"1a.{i} 发起试切 201", s == 201, f"status={s} body={t}")
            if s != 201:
                continue
            ids.append(t["trial_id"])
            report.check(
                f"1a.{i} 回退目标=换路前实际下一跳 primary",
                t.get("original_nexthop") == PRIMARY
                and t.get("rollback_target") == PRIMARY,
                f"original={t.get('original_nexthop')}")
            report.check(f"1a.{i} 路由已切到 backup-good",
                         h.sim_nexthop() == BACKUP_GOOD, h.sim_nexthop())

            # 让这笔「到期」；随机让出时间，有时由看门狗先回退、有时由新 POST 交接
            tid = t["trial_id"]

            def expire_it(state, tid=tid):
                cur = state.get("current")
                if cur and cur["trial_id"] == tid:
                    cur["deadline_ts"] = time.time() - 0.01
                    cur["deadline_at"] = "expired-for-test"

            atomic_state_mutate(h.state_dir, expire_it)
            time.sleep((i % 3) * 0.03)

            s2, t2 = h.http("POST", "/api/trials",
                            {"nexthop": "backup-two", "duration_sec": 5})
            report.check(f"1a.{i} 到期边界发起下一笔成功(201)", s2 == 201,
                         f"status={s2} body={str(t2)[:160]}")
            if s2 == 201:
                report.check(
                    f"1a.{i} 新试切未把上一笔备用下一跳记成回退目标",
                    t2.get("original_nexthop") == PRIMARY,
                    f"original={t2.get('original_nexthop')} "
                    f"(错误值会是 {BACKUP_GOOD})")
                rb_s, rb_t = h.http("POST",
                                    f"/api/trials/{t2['trial_id']}/rollback")
                report.check(f"1a.{i} 提前回退 200 且恢复 primary",
                             rb_s == 200 and h.wait_route(PRIMARY, 3),
                             f"status={rb_s} route={h.sim_nexthop()}")

        # 1b. 临界并发：5 个 POST 同时抢交接，只能有 1 个 201，其余 409
        s, t = h.http("POST", "/api/trials",
                      {"nexthop": "backup-good", "duration_sec": 5})
        report.check("1b 准备试切", s == 201, str(s))
        tid = t["trial_id"]
        atomic_state_mutate(
            h.state_dir,
            lambda st: st["current"].update(deadline_ts=time.time() - 1)
            if st.get("current") and st["current"]["trial_id"] == tid else None)
        barrier = threading.Barrier(5)
        results = []

        def fire():
            barrier.wait()
            results.append(h.http("POST", "/api/trials",
                                  {"nexthop": "backup-two", "duration_sec": 5}))

        threads = [threading.Thread(target=fire) for _ in range(5)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        created = [r for r in results if r[0] == 201]
        conflicts = [r for r in results if r[0] == 409]
        report.check("1b 并发交接恰好 1 个 201", len(created) == 1,
                     f"201={len(created)} 409={len(conflicts)} "
                     f"other={[r[0] for r in results if r[0] not in (201,409)]}")
        report.check("1b 其余全部 409", len(conflicts) == 4,
                     f"got {len(conflicts)}")
        if created:
            ct = created[0][1]
            report.check("1b 赢家回退目标仍是 primary",
                         ct.get("original_nexthop") == PRIMARY,
                         str(ct.get("original_nexthop")))
            h.http("POST", f"/api/trials/{ct['trial_id']}/rollback")
            h.wait_route(PRIMARY, 3)

        # 1c. 全量历史不变量：每一笔的 original_nexthop 都必须是 primary
        s, listing = h.http("GET", "/api/trials")
        bad = [x["trial_id"] for x in listing.get("history", [])
               if x.get("original_nexthop") != PRIMARY]
        report.check("1c 全部历史试切 original_nexthop=primary", not bad,
                     f"bad={bad}")
        report.check("1c 无残留 pending_switch",
                     not listing.get("pending_switch"),
                     str(listing.get("pending_switch")))
        report.check("1c 当前无试切", listing.get("current") is None)
    finally:
        h.stop()


def scenario_slow_switch(report: Report) -> None:
    report.section("2. 慢换路：试用期必须从换路成功起算，慢延迟不被试用期吃掉")
    h = Harness("slow")
    try:
        h.start_all()
        h.touch("slow_switch", "1.2")  # RouteManager 在换路前 sleep 1.2s
        t0 = time.time()
        s, t = h.http("POST", "/api/trials",
                      {"nexthop": "backup-good", "duration_sec": 5},
                      timeout=20)
        dt = time.time() - t0
        report.check("2 慢换路 POST 仍 201", s == 201, f"status={s} {str(t)[:200]}")
        report.check("2 请求确实经历了慢换路(>=1.1s)", dt >= 1.1, f"{dt:.2f}s")
        if s == 201:
            gap = t["started_ts"] - t["switch_started_ts"]
            report.check("2 换路耗时被记录(~1.2s)", gap >= 1.1, f"{gap:.2f}s")
            report.check(
                "2 换路成功后起算：响应时剩余≈5s（旧实现会只剩≈3.8s）",
                t["remaining_sec"] >= 4.4,
                f"remaining={t['remaining_sec']}")
            report.check("2 deadline = switched_ts + duration",
                         abs((t["deadline_ts"] - t["started_ts"]) - 5) < 0.05)
            h.http("POST", f"/api/trials/{t['trial_id']}/rollback")
            report.check("2 回退恢复 primary", h.wait_route(PRIMARY, 3))
    finally:
        h.stop()


def scenario_api_crash_hook(report: Report) -> None:
    report.section("3a. API 中断：换路生效后、存档前故障自杀，看门狗在 API 停着时撤回")
    h = Harness("crash")
    try:
        h.start_all()
        h.touch("crash_after_switch")  # 一次性：换路成功瞬间 SIGKILL 本进程
        s, t = h.http("POST", "/api/trials",
                      {"nexthop": "backup-good", "duration_sec": 30},
                      timeout=10)
        report.check("3a POST 连接随 API 死亡而失败(0)", s == 0,
                     f"status={s} body={str(t)[:120]}")
        rc = h.api_proc.wait(timeout=5) if h.api_proc else None
        report.check("3a uvicorn 进程确已死亡(SIGKILL/-9)",
                     rc is not None and rc != 0, f"returncode={rc}")
        report.check("3a 换路在 API 死前已生效",
                     h.sim_nexthop() == BACKUP_GOOD, str(h.sim_nexthop()))

        # 不重启 API，只等看门狗
        ok = h.wait_route(PRIMARY, timeout=5)
        report.check("3a API 停着时看门狗独立撤回换路 -> primary", ok,
                     f"route={h.sim_nexthop()}")
        rec = h.wait_history_reason("api_interrupted", timeout=3)
        report.check("3a 状态文件留下 api_interrupted 记录", rec is not None,
                     "" if rec else "未找到记录")
        if rec:
            report.check("3a 记录的撤回目标是 primary",
                         rec.get("original_nexthop") == PRIMARY,
                         str(rec.get("original_nexthop")))

        # 重启 API 后结果可查
        h.start_api()
        s, listing = h.http("GET", "/api/trials")
        reasons = [x.get("rollback_reason") for x in listing.get("history", [])]
        report.check("3a API 重启后可查到中断撤回记录",
                     s == 200 and "api_interrupted" in reasons, str(reasons))
        s, cur = h.http("GET", "/api/trials/current")
        report.check("3a 无残留当前试切", cur.get("current") is None, str(cur))
    finally:
        h.stop()


def scenario_gate_sigkill(report: Report) -> None:
    report.section("3b. 真实 SIGKILL：闸门停在『已换路未存档』窗口，外部 kill -9 API")
    h = Harness("gate")
    try:
        h.start_all()
        gate = h.touch("gate_after_switch")
        switched_flag = Path(str(gate) + ".switched")

        box: dict = {}

        def parked_post():
            box["result"] = h.http(
                "POST", "/api/trials",
                {"nexthop": "backup-good", "duration_sec": 30}, timeout=20)

        th = threading.Thread(target=parked_post)
        th.start()
        end = time.time() + 10
        while time.time() < end and not switched_flag.exists():
            time.sleep(0.05)
        report.check("3b API 已停在换路后闸门(.switched 出现)",
                     switched_flag.exists())
        report.check("3b 此刻路由确实已切到 backup-good（记录尚未保存）",
                     h.sim_nexthop() == BACKUP_GOOD, str(h.sim_nexthop()))
        state = h.read_state()
        report.check("3b state 中只有 pending_switch、没有 current 试切",
                     bool(state.get("pending_switch"))
                     and state.get("current") is None,
                     f"pending={bool(state.get('pending_switch'))}")

        rc = h.kill_api9()
        report.check("3b 已 kill -9 uvicorn", rc == -9, f"rc={rc}")
        th.join(timeout=5)

        ok = h.wait_route(PRIMARY, timeout=5)
        report.check("3b API 已死，看门狗仍独立恢复 primary", ok,
                     f"route={h.sim_nexthop()}")
        rec = h.wait_history_reason("api_interrupted", timeout=3)
        report.check("3b 留下 api_interrupted 历史", rec is not None)
        gate.unlink(missing_ok=True)
        Path(str(gate) + ".continue").unlink(missing_ok=True)
    finally:
        h.stop()


def scenario_fail_retry(report: Report) -> None:
    report.section("4. 换路失败且原路由还在：清理槽位，允许立即重新发起")
    h = Harness("failretry")
    try:
        h.start_all()
        h.touch("fail_switch")
        s, t = h.http("POST", "/api/trials",
                      {"nexthop": "backup-good", "duration_sec": 5})
        report.check("4 故障注入时 POST 返回 502", s == 502, f"status={s}")
        report.check("4 错误信息说明可重新发起",
                     isinstance(t, dict) and "重新发起" in str(t.get("detail", "")),
                     str(t)[:180])
        report.check("4 原路由未被破坏，仍是 primary",
                     h.sim_nexthop() == PRIMARY, str(h.sim_nexthop()))
        s, cur = h.http("GET", "/api/trials/current")
        report.check("4 没有留下占用槽位的 current",
                     cur.get("current") is None, str(cur)[:160])

        (h.state_dir / "fail_switch").unlink()
        s, t = h.http("POST", "/api/trials",
                      {"nexthop": "backup-good", "duration_sec": 5})
        report.check("4 排除故障后重新发起成功 201", s == 201,
                     f"status={s} {str(t)[:160]}")
        if s == 201:
            report.check("4 重试试切回退目标=primary",
                         t.get("original_nexthop") == PRIMARY)
            s2, _ = h.http("POST", f"/api/trials/{t['trial_id']}/rollback")
            report.check("4 回退成功", s2 == 200 and h.wait_route(PRIMARY, 3))
    finally:
        h.stop()


def scenario_early_and_deadline(report: Report) -> None:
    report.section("5. 提前回退 / 到期回退 / 一次一个 / 旧试切号")
    h = Harness("earlydl")
    try:
        h.start_all()
        # 提前回退
        s, t = h.http("POST", "/api/trials",
                      {"nexthop": "backup-good", "duration_sec": 30})
        tid = t["trial_id"]
        s, rb = h.http("POST", f"/api/trials/{tid}/rollback")
        report.check("5a 提前回退 200 + manual_rollback",
                     s == 200 and rb.get("rollback_reason") == "manual_rollback",
                     f"{s} {rb.get('rollback_reason') if isinstance(rb,dict) else rb}")
        report.check("5a 路由立即恢复 primary", h.wait_route(PRIMARY, 2))
        # 重复回退幂等
        s2, rb2 = h.http("POST", f"/api/trials/{tid}/rollback")
        report.check("5a 重复回退幂等 200", s2 == 200
                     and rb2.get("rollback_reason") == "manual_rollback")
        # current/rollback 在无试切时 409
        s3, _ = h.http("POST", "/api/trials/current/rollback")
        report.check("5a 无当前试切时 current/rollback=409", s3 == 409, str(s3))

        # 到期回退（schema 允许的最小 duration=5，不确认）
        s, t = h.http("POST", "/api/trials",
                      {"nexthop": "backup-good", "duration_sec": 5})
        report.check("5b 发起 5s 试切 201", s == 201, f"{s} {str(t)[:120]}")
        tid2 = t["trial_id"]
        rec = h.wait_history_reason("deadline_exceeded", timeout=8)
        report.check("5b 看门狗到期自动回退", rec is not None
                     and rec["trial_id"] == tid2)
        report.check("5b 到期后路由恢复 primary", h.wait_route(PRIMARY, 2))

        # 一次一个试切
        s, t = h.http("POST", "/api/trials",
                      {"nexthop": "backup-good", "duration_sec": 30})
        tid3 = t["trial_id"]
        s409, _ = h.http("POST", "/api/trials",
                         {"nexthop": "backup-two", "duration_sec": 30})
        report.check("5c 进行中再发起=409", s409 == 409, str(s409))
        # 旧试切号
        s404, _ = h.http("POST", f"/api/trials/{tid}/confirm")
        report.check("5d 旧试切号 confirm=404", s404 == 404, str(s404))
        # 当前试切不受影响
        s, cur = h.http("GET", "/api/trials/current")
        report.check("5d 当前试切仍是 tid3",
                     cur["current"] and cur["current"]["trial_id"] == tid3)
        h.http("POST", f"/api/trials/{tid3}/rollback")
        h.wait_route(PRIMARY, 2)
    finally:
        h.stop()


def scenario_manual_rollback_during_probe(report: Report) -> None:
    report.section("6. 探测在途时提前回退：回退生效，迟到探测结果不得改写终态")
    # 关闭的端口 + 很多次探测，让 probing 持续足够久
    h = Harness("probeabort", probe_attempts=30, probe_timeout=0.4)
    try:
        h.start_all()
        s, t = h.http("POST", "/api/trials",
                      {"nexthop": "backup-good", "duration_sec": 30})
        tid = t["trial_id"]
        s, c = h.http("POST", f"/api/trials/{tid}/confirm")
        report.check("6 confirm 202 进入探测", s == 202, str(s))
        # 等状态变 probing
        end = time.time() + 3
        probing = False
        while time.time() < end:
            _, tt = h.http("GET", f"/api/trials/{tid}")
            if isinstance(tt, dict) and tt.get("status") == "probing":
                probing = True
                break
            time.sleep(0.05)
        report.check("6 已进入 probing", probing)
        s, rb = h.http("POST", "/api/trials/current/rollback")
        report.check("6 探测中手动回退 200/manual",
                     s == 200 and rb.get("rollback_reason") == "manual_rollback")
        report.check("6 路由恢复 primary", h.wait_route(PRIMARY, 2))
        time.sleep(2.5)  # 等几轮迟到探测返回并尝试定夺
        _, rec = h.http("GET", f"/api/trials/{tid}")
        report.check("6 终态保持 rolled_back/manual_rollback，未被迟到探测改写",
                     isinstance(rec, dict)
                     and rec.get("status") == "rolled_back"
                     and rec.get("rollback_reason") == "manual_rollback",
                     f"{rec.get('status')}/{rec.get('rollback_reason') if isinstance(rec,dict) else rec}")
    finally:
        h.stop()


def scenario_probe_keep(report: Report) -> None:
    report.section("7. 正常回归：探测成功 -> kept，路由保留")
    httpd = None
    h = Harness("keep", probe_attempts=2, probe_timeout=0.5)
    try:
        # 起一个真实的本地 TCP/HTTP 采集端
        serve_dir = h.root / "collector"
        serve_dir.mkdir(exist_ok=True)
        httpd = subprocess.Popen(
            [sys.executable, "-m", "http.server", str(h.probe_port),
             "--bind", "127.0.0.1"],
            cwd=str(serve_dir), stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL)
        # 等端口起来
        end = time.time() + 5
        while time.time() < end:
            try:
                with socket.create_connection(("127.0.0.1", h.probe_port), 0.2):
                    break
            except OSError:
                time.sleep(0.1)
        h.start_all()
        s, t = h.http("POST", "/api/trials",
                      {"nexthop": "backup-good", "duration_sec": 30})
        tid = t["trial_id"]
        s, _ = h.http("POST", f"/api/trials/{tid}/confirm")
        rec = None
        end = time.time() + 8
        while time.time() < end:
            _, tt = h.http("GET", f"/api/trials/{tid}")
            if isinstance(tt, dict) and tt.get("status") in ("kept", "rolled_back"):
                rec = tt
                break
            time.sleep(0.15)
        report.check("7 探测成功后 status=kept",
                     rec and rec.get("status") == "kept", str(rec)[:200])
        report.check("7 kept 后路由保留在 backup-good",
                     h.sim_nexthop() == BACKUP_GOOD, str(h.sim_nexthop()))
    finally:
        h.stop()
        if httpd:
            httpd.terminate()
            with contextlib_suppress():
                httpd.wait(timeout=3)


def main() -> int:
    print(f"gateway dir: {GATEWAY_DIR}")
    print(f"python: {sys.executable}")
    if not (GATEWAY_DIR / "app" / "main.py").exists():
        print("FATAL: cannot locate gateway/app/main.py")
        return 2
    r = Report()
    scenario_handoff(r)
    scenario_slow_switch(r)
    scenario_api_crash_hook(r)
    scenario_gate_sigkill(r)
    scenario_fail_retry(r)
    scenario_early_and_deadline(r)
    scenario_manual_rollback_during_probe(r)
    scenario_probe_keep(r)

    print("\n" + "=" * 60)
    print(f"PASSED {len(r.passed)} checks, FAILED {len(r.failed)} checks")
    if r.failed:
        for name, detail in r.failed:
            print(f"  - {name}: {detail}")
        return 1
    print("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
