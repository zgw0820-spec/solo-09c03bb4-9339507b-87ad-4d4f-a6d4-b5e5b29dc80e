#!/usr/bin/env python3
"""Swagger 演示流程的命令行等价脚本：连续试切 + 三种回退结局。

真实拉起 watchdog + uvicorn(dryrun)，按值班员在 http://localhost:8080/docs
上的点击顺序调用 API，并打印请求/响应摘要。Docker 环境下把同一组 HTTP 调用
打到 localhost:8080 即可，路径与报文体完全一致。
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from verify_handoff import Harness  # noqa: E402


def pp(label, status, body, fields=None):
    print(f"\n$ {label}\n-> HTTP {status}")
    if isinstance(body, dict):
        if fields:
            slim = {k: body.get(k) for k in fields if k in body}
            print(json.dumps(slim, ensure_ascii=False, indent=2))
        else:
            print(json.dumps(body, ensure_ascii=False, indent=2)[:1200])
    else:
        print(str(body)[:300])


def main() -> int:
    # 探测端口不放采集端 -> backup-broken 确认后探测快速失败
    h = Harness("swagger_demo", probe_attempts=2, probe_timeout=0.4)
    failures = 0
    try:
        h.start_all()

        s, spec = h.http("GET", "/openapi.json")
        examples = (spec.get("paths", {}).get("/api/trials", {})
                    .get("post", {}).get("requestBody", {})
                    .get("content", {}).get("application/json", {})
                    .get("examples", {}))
        print("Swagger /docs 与 OpenAPI 检查")
        print(f"  openapi paths: {len(spec.get('paths', {}))} 个；"
              f"POST /api/trials 样例: {sorted(examples)}")
        print(f"  /docs HTTP {h.http('GET', '/docs')[0]}")
        if "rollback_early" not in examples:
            print("  WARN: rollback_early 样例未出现在 OpenAPI")
            failures += 1

        print("\n########## 第 1 笔：backup-good，随后手动提前回退 ##########")
        s, t1 = h.http("POST", "/api/trials",
                       {"nexthop": "backup-good", "duration_sec": 60})
        pp("POST /api/trials", s, t1,
           ["trial_id", "nexthop", "original_nexthop", "rollback_target",
            "status", "remaining_sec", "rollback_url"])
        id1 = t1["trial_id"]
        pp(f"POST /api/trials/{id1}/rollback",
           *h.http("POST", f"/api/trials/{id1}/rollback"),
           ["trial_id", "status", "rollback_reason", "original_nexthop"])
        pp("GET /api/route", *h.http("GET", "/api/route"),
           ["target", "nexthop", "backend"])

        print("\n########## 第 2 笔：立刻再试切 backup-broken，确认后探测失败回退 ##########")
        s, t2 = h.http("POST", "/api/trials",
                       {"nexthop": "backup-broken", "duration_sec": 60})
        pp("POST /api/trials", s, t2,
           ["trial_id", "nexthop_name", "original_nexthop", "status"])
        id2 = t2["trial_id"]
        pp(f"POST /api/trials/{id2}/confirm",
           *h.http("POST", f"/api/trials/{id2}/confirm"),
           ["trial_id", "status"])
        # 等探测失败定夺
        rec = None
        end = time.time() + 6
        while time.time() < end:
            _, rec = h.http("GET", f"/api/trials/{id2}")
            if isinstance(rec, dict) and rec.get("status") == "rolled_back":
                break
            time.sleep(0.15)
        pp(f"GET /api/trials/{id2}（探测失败后）", 200, rec,
           ["trial_id", "status", "rollback_reason", "original_nexthop"])
        print("route after probe failure:", h.sim_nexthop())

        print("\n########## 第 3 笔：连续再切 backup-good，不确认，5s 到期看门狗回退 ##########")
        s, t3 = h.http("POST", "/api/trials",
                       {"nexthop": "backup-good", "duration_sec": 5})
        pp("POST /api/trials", s, t3,
           ["trial_id", "original_nexthop", "deadline_at", "remaining_sec"])
        id3 = t3["trial_id"]
        rec3 = h.wait_history_reason("deadline_exceeded", timeout=8)
        assert rec3 is not None, "看门狗未在预期时间内写入 deadline_exceeded"
        _, got3 = h.http("GET", f"/api/trials/{id3}")
        pp(f"GET /api/trials/{id3}（看门狗到期后）", 200, got3,
           ["trial_id", "status", "rollback_reason", "original_nexthop"])
        print("route after deadline:", h.sim_nexthop())

        print("\n########## 第 4 笔：用 /api/trials/current/rollback 一键提前回退 ##########")
        s, t4 = h.http("POST", "/api/trials",
                       {"nexthop": "backup-two", "duration_sec": 60})
        pp("POST /api/trials", s, t4,
           ["trial_id", "nexthop", "original_nexthop"])
        pp("POST /api/trials/current/rollback",
           *h.http("POST", "/api/trials/current/rollback"),
           ["trial_id", "status", "rollback_reason"])

        print("\n########## 汇总：GET /api/trials ##########")
        _, listing = h.http("GET", "/api/trials")
        print(f"current={listing['current']} pending={listing.get('pending_switch')}")
        print("history (newest first):")
        for x in listing["history"]:
            print(f"  {x['trial_id']}  {x.get('nexthop_name'):<14} "
                  f"orig={x.get('original_nexthop')}  "
                  f"{x['status']}/{x.get('rollback_reason')}")
        bad = [x["trial_id"] for x in listing["history"]
               if x.get("original_nexthop") != "10.99.0.11"]
        if bad:
            print("BAD original_nexthop:", bad)
            failures += 1
        else:
            print("\n结论：连续 4 笔试切，回退目标全部是换路前实际下一跳 "
                  "10.99.0.11（primary），无一笔误记为上一笔的备用下一跳。")
        return 1 if failures else 0
    finally:
        h.stop()


if __name__ == "__main__":
    sys.exit(main())
