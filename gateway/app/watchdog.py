"""独立看门狗进程：到期试切自动回退；并撤回无主的「半提交换路」。

与管理 API 分进程运行（同一容器内由 entrypoint 拉起）：
  - 管理 API 停止/崩溃时，回退依然按期限执行；
  - 若 API 在换路生效后、保存试切记录前退出，状态里会留下换路前落盘的
    pending_switch 标记；看门狗拿到 flock 后立即按其中记录的原下一跳撤回，
    不需要 API 在场；
  - 覆盖「探测尚未返回但已到期」的场景：看门狗先回退，
    迟到的探测结果会被 API 侧按试切号+状态校验丢弃。
"""
from __future__ import annotations

import os
import time

from .core import (
    Config,
    RouteManager,
    StateStore,
    expire_if_needed,
    load_config,
    reconcile_pending_switch_locked,
)

INTERVAL_SEC = float(os.environ.get("TSW_WATCHDOG_INTERVAL_SEC", "0.5"))


def main() -> None:
    cfg = Config(load_config())
    store = StateStore()
    routes = RouteManager(cfg.target, backend=cfg.route_backend)
    print(f"[watchdog] started: target={cfg.target} backend={cfg.route_backend} "
          f"interval={INTERVAL_SEC}s", flush=True)
    while True:
        try:
            reverted = None
            rolled = None
            with store.locked() as state:
                # 先处理「API 换路后、存档前退出」留下的无主换路（可能立即撤回）
                reverted = reconcile_pending_switch_locked(state, routes)
                # 再处理到期试切
                rolled = expire_if_needed(state, routes)
            if reverted:
                print(
                    f"[watchdog] reverted orphan switch of trial "
                    f"{reverted['trial_id']} while management API was gone "
                    f"(reason={reverted['rollback_reason']}, restored via "
                    f"{reverted.get('original_nexthop') or 'direct'}, observed "
                    f"{reverted.get('observed_nexthop_on_recovery')})",
                    flush=True,
                )
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
