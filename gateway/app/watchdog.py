"""独立看门狗进程：到期仍未确认成功的试切，自动恢复原下一跳。

与管理 API 分进程运行（同一容器内由 entrypoint 拉起）：
  - 管理 API 停止/崩溃时，回退依然按期限执行；
  - 覆盖「探测尚未返回但已到期」的场景：看门狗先回退，
    迟到的探测结果会被 API 侧按试切号+状态校验丢弃。
"""
from __future__ import annotations

import time

from .core import Config, RouteManager, StateStore, expire_if_needed, load_config

INTERVAL_SEC = 0.5


def main() -> None:
    cfg = Config(load_config())
    store = StateStore()
    routes = RouteManager(cfg.target, backend=cfg.route_backend)
    print(f"[watchdog] started: target={cfg.target} backend={cfg.route_backend}",
          flush=True)
    while True:
        try:
            with store.locked() as state:
                rolled = expire_if_needed(state, routes)
            if rolled:
                print(
                    f"[watchdog] trial {rolled['trial_id']} auto rolled back "
                    f"(reason={rolled['rollback_reason']}, restored via "
                    f"{rolled.get('original_nexthop') or 'direct'})",
                    flush=True,
                )
        except Exception as exc:  # noqa: BLE001 - 看门狗不允许因单次异常退出
            print(f"[watchdog] error: {type(exc).__name__}: {exc}", flush=True)
        time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    main()
