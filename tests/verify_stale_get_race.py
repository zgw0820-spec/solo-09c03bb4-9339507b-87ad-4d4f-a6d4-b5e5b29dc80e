#!/usr/bin/env python3
"""确定性复现/验证「新试切记错回退目标」的交接竞态。

手法：tests/fakebin/ip 让 `ip route get` 先读到当前（上一笔的备用）下一跳，
再 sleep 0.9s 后返回陈旧值。时间线：

  1. 种入一笔 0.5s 后到期的试切 A（original=primary，路由在 backup-A）；
  2. 立刻 POST 新试切 B：
     - 旧代码：expire_current() 看到「未到期」-> 锁外 route get 读到 backup-A
       并卡在假 ip 里；0.5s 到期，看门狗把路由恢复成 primary；假 ip 随后返回
       陈旧的 backup-A；B 把 backup-A 当成自己的 original_nexthop 落盘 —— BUG；
     - 新代码：读路由+到期交接都在 flock 内，未到期只会得到 409；重试后在锁内
       先恢复/交接再读，必然读到 primary，记录正确。

退出码：0=交接正确（未复现 bug）；3=复现记错回退目标；1=线束/断言异常。

用法：
  python tests/verify_stale_get_race.py [--gateway /path/to/gateway]
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_GATEWAY = HERE.parent / "gateway"
FAKEBIN = HERE / "fakebin"
PRIMARY = "172.30.0.11"
BACKUP_A = "172.30.0.12"
BACKUP_B = "172.30.0.14"
TARGET = "172.30.0.100"


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def http(base, method, path, body=None, timeout=15):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode()
            return r.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, None
    except Exception as e:  # transport
        return 0, {"transport_error": str(e)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gateway", default=str(DEFAULT_GATEWAY))
    args = ap.parse_args()
    gateway = Path(args.gateway).resolve()
    if not (gateway / "app" / "main.py").exists():
        print(f"FATAL: no app/main.py under {gateway}")
        return 1

    root = Path(tempfile.mkdtemp(prefix="tsw_race_"))
    state = root / "state"
    state.mkdir(parents=True)
    port = free_port()
    fake_state = root / "fake_ip_routes.json"
    fake_state.write_text(json.dumps({"nexthop": BACKUP_A}), encoding="utf-8")

    cfg = f"""
target: {TARGET}
probe_port: 9000
probe_attempts: 1
probe_timeout_sec: 0.2
nexthops:
  primary: {PRIMARY}
  backup-good: {BACKUP_A}
  backup-two: {BACKUP_B}
"""
    cfg_path = root / "config.yaml"
    cfg_path.write_text(cfg, encoding="utf-8")

    # 种入一笔活着但 0.5s 后到期的试切 A（与真实落盘结构兼容）
    now = time.time()
    trial_a = {
        "trial_id": "tOLDAAAAAA",
        "target": TARGET,
        "nexthop_name": "backup-good",
        "nexthop": BACKUP_A,
        "original_nexthop": PRIMARY,
        "started_at": now,
        "started_ts": now,
        "deadline_at": now + 0.5,
        "deadline_ts": now + 0.5,
        "duration_sec": 5,
        "status": "trialing",
        "probe": {"state": "none"},
        "rollback_reason": None,
    }
    (state / "state.json").write_text(
        json.dumps({"current": trial_a, "pending_switch": None,
                    "history": []}), encoding="utf-8")

    env = dict(os.environ)
    env.update({
        "PATH": f"{FAKEBIN}:{env.get('PATH', '')}",
        "FAKE_IP_STATE": str(fake_state),
        "FAKE_IP_GET_DELAY": "0.9",
        "TSW_CONFIG": str(cfg_path),
        "TSW_STATE_DIR": str(state),
        "TSW_ROUTE_BACKEND": "ip",
        "TSW_WATCHDOG_INTERVAL_SEC": "0.05",
        "PYTHONUNBUFFERED": "1",
    })
    py = sys.executable
    wdlog = open(root / "wd.log", "w")
    apilog = open(root / "api.log", "w")
    wd = subprocess.Popen([py, "-m", "app.watchdog"], cwd=str(gateway),
                          env=env, stdout=wdlog, stderr=subprocess.STDOUT,
                          start_new_session=True)
    api = subprocess.Popen(
        [py, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1",
         "--port", str(port), "--log-level", "warning"],
        cwd=str(gateway), env=env, stdout=apilog, stderr=subprocess.STDOUT,
        start_new_session=True)
    base = f"http://127.0.0.1:{port}"

    def cleanup():
        for p in (api, wd):
            try:
                os.killpg(os.getpgid(p.pid), signal.SIGKILL)
            except Exception:
                pass

    try:
        end = time.time() + 15
        while time.time() < end:
            s, _ = http(base, "GET", "/api/health", timeout=2)
            if s == 200:
                break
            time.sleep(0.15)
        else:
            print("FATAL: api not healthy\n" + (root / "api.log").read_text()[-2000:])
            return 1

        # 关键一击：A 还有 ~0.5s 才到期，立即发起 B
        t_fire = time.time()
        s1, b1 = http(base, "POST", "/api/trials",
                      {"nexthop": "backup-two", "duration_sec": 5}, timeout=10)
        fire_dt = time.time() - t_fire
        print(f"first POST -> {s1} in {fire_dt:.2f}s")

        created = None
        if s1 == 201:
            created = b1
        elif s1 == 409:
            # 新代码：未到期被拒；等看门狗到期回退后重试
            deadline = time.time() + 6
            while time.time() < deadline and created is None:
                s, b = http(base, "POST", "/api/trials",
                            {"nexthop": "backup-two", "duration_sec": 5},
                            timeout=10)
                if s == 201:
                    created = b
                    break
                time.sleep(0.1)
        else:
            print(f"HARNESS ERROR: unexpected first status {s1}: {b1}")
            return 1

        if created is None:
            print("HARNESS ERROR: never created trial B")
            print("wd log:\n", (root / "wd.log").read_text()[-1500:])
            return 1

        orig = created.get("original_nexthop")
        print(f"trial B={created['trial_id']} original_nexthop={orig} "
              f"(expect {PRIMARY}; buggy value {BACKUP_A})")

        # 收尾：回退 B（用真实 API；假 ip）
        http(base, "POST", f"/api/trials/{created['trial_id']}/rollback")

        if orig == BACKUP_A:
            print("VERDICT=BUG_REPRODUCED: 新试切把上一笔的备用下一跳记成了回退目标")
            return 3
        if orig == PRIMARY:
            print("VERDICT=OK: 每笔试切都以换路前实际下一跳为回退目标")
            return 0
        print(f"HARNESS ERROR: unexpected original_nexthop {orig}")
        return 1
    finally:
        cleanup()
        wdlog.close()
        apilog.close()


if __name__ == "__main__":
    sys.exit(main())
