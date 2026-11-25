"""采集网关静态路由试切管理 API（FastAPI）。

流程：
  1. POST /api/trials            值班员选预置备用下一跳 + 试用秒数；
                                 在同一把状态锁内完成「到期交接 -> 读换路前
                                 实际下一跳 -> 落换路意图 -> 换路 -> 存试切记录」，
                                 试用时间从换路成功起算；
  2. POST /api/trials/{id}/confirm  凭试切号确认，网关经新下一跳 TCP 探测采集端；
     - 确认与探测都在期限内成功 -> 保留新路由（kept）
     - 探测失败               -> 立即恢复原下一跳（rolled_back/probe_failed）
  3. POST /api/trials/{id}/rollback 值班员提前回退（manual_rollback）；
  4. 到期仍未确认成功           -> 看门狗自动回退（rolled_back/deadline_exceeded），
                                 即使本 API 进程已停、或探测尚未返回；
                                 若 API 换路后、存档前退出，看门狗还会依据
                                 pending_switch 意图标记独立撤回那次换路。

一次只允许一个试切；迟到的探测结果与旧试切号不影响当前试切。
"""
from __future__ import annotations

import asyncio
import contextlib
import time
import uuid

from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from .core import (
    ACTIVE_STATUSES,
    REASON_DEADLINE,
    REASON_MANUAL,
    REASON_PROBE_FAILED,
    Config,
    RouteManager,
    StateStore,
    expire_if_needed,
    keep_locked,
    load_config,
    now_iso,
    reconcile_pending_switch_locked,
    rollback_locked,
)

cfg = Config(load_config())
store = StateStore()
routes = RouteManager(cfg.target, backend=cfg.route_backend)

app = FastAPI(
    title="采集网关静态路由试切 API",
    version="1.1.0",
    description=(
        "现场采集网关的静态路由试切：替换通往采集端的 Linux IPv4 主机路由，"
        "每笔试切以换路前实际使用的下一跳为回退目标，试用时间从换路成功起算；"
        "可凭试切号确认探测、可提前回退，到期由独立看门狗自动恢复原下一跳"
        "（即使本 API 已停；换路后存档前崩溃也会被看门狗独立撤回）。"
        "一次只允许一个试切；迟到的探测结果与旧试切号不影响当前试切。"
    ),
)


class TrialIn(BaseModel):
    nexthop: str = Field(..., description="预置备用下一跳名称（见 GET /api/config）")
    duration_sec: int = Field(
        30, ge=5, le=900,
        description="试用秒数：自换路成功时刻起算，到期未确认成功即自动回退",
    )


TRIAL_EXAMPLES = {
    "keep": {
        "summary": "保留：期限内确认 + 探测成功",
        "description": "切到可正常转发的 backup-good，60 秒内发起确认，探测成功后新路由被保留。",
        "value": {"nexthop": "backup-good", "duration_sec": 60},
    },
    "rollback_probe_failed": {
        "summary": "回退：确认后探测失败，立即恢复原下一跳",
        "description": "backup-broken 不转发报文，确认后 TCP 探测超时失败，网关立即回退。",
        "value": {"nexthop": "backup-broken", "duration_sec": 60},
    },
    "rollback_early": {
        "summary": "提前回退：换路后随时手动恢复",
        "description": "发起后调用 POST /api/trials/{trial_id}/rollback，立即恢复换路前下一跳。",
        "value": {"nexthop": "backup-good", "duration_sec": 60},
    },
    "rollback_deadline": {
        "summary": "回退：到期未确认，看门狗自动回退",
        "description": "发起后不确认，15 秒到期由看门狗自动回退（即使停掉管理 API 也会回退）。",
        "value": {"nexthop": "backup-good", "duration_sec": 15},
    },
}


def enrich(trial: dict) -> dict:
    """附加剩余秒数与显式回退目标，便于值班员查看。"""
    t = dict(trial)
    if t["status"] in ACTIVE_STATUSES:
        t["remaining_sec"] = round(max(0.0, t["deadline_ts"] - time.time()), 1)
    else:
        t["remaining_sec"] = 0.0
    # 显式别名：这笔试切到期/提前回退时将恢复到的下一跳
    t["rollback_target"] = t.get("original_nexthop")
    return t


def housekeeping() -> None:
    """懒维护：任何请求进来时先处理无主换路并把已到期试切回退掉。

    与看门狗使用同一把 flock、同一套幂等逻辑，谁先抢到锁谁执行。
    """
    with store.locked() as state:
        reconcile_pending_switch_locked(state, routes)
        expire_if_needed(state, routes)


# 旧名字保留为薄封装，避免外部脚本/测试按这个名字导入时失效
def expire_current() -> None:
    housekeeping()


async def tcp_probe() -> tuple[bool, str, int]:
    """经当前（新）下一跳对采集端做 TCP 连接探测，带有限次重试。"""
    last = "no attempt"
    for i in range(1, cfg.probe_attempts + 1):
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(cfg.target, cfg.probe_port),
                timeout=cfg.probe_timeout,
            )
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # noqa: BLE001
                pass
            return True, f"第 {i} 次探测成功：TCP {cfg.target}:{cfg.probe_port} 连接建立", i
        except Exception as exc:  # noqa: BLE001 - 任何连接异常都视为本次探测失败
            last = f"第 {i} 次探测失败：{type(exc).__name__}: {exc}"
            if i < cfg.probe_attempts:
                await asyncio.sleep(cfg.probe_interval)
    return False, last, cfg.probe_attempts


async def probe_and_finalize(trial_id: str) -> None:
    """探测结束后按规则定夺；迟到的结果直接丢弃，不影响当前试切。"""
    try:
        via = routes.get().get("nexthop")
    except Exception:  # noqa: BLE001
        via = None
    ok, detail, attempts = await tcp_probe()
    finished = time.time()
    with store.locked() as state:
        cur = state.get("current")
        # 试切已结束（看门狗/手动/探测失败已回退）或已换成别的试切 -> 迟到结果，丢弃
        if not cur or cur["trial_id"] != trial_id or cur["status"] != "probing":
            return
        cur["probe"].update({
            "finished_at": now_iso(finished),
            "attempts": attempts,
            "detail": detail,
            "route_nexthop": via,
        })
        if finished > cur["deadline_ts"]:
            # 探测返回时已超过期限：不算确认成功，按到期回退
            cur["probe"]["state"] = "late_expired"
            rollback_locked(state, routes, REASON_DEADLINE)
        elif ok:
            cur["probe"]["state"] = "success"
            keep_locked(state)
        else:
            cur["probe"]["state"] = "failed"
            rollback_locked(state, routes, REASON_PROBE_FAILED)


@app.get("/", include_in_schema=False)
def index():
    return RedirectResponse(url="/docs")


@app.get("/api/health", summary="健康检查")
def health():
    return {"status": "ok", "time": now_iso()}


@app.get("/api/config", summary="查看预置下一跳与探测参数")
def get_config():
    return {
        "target": cfg.target,
        "probe_port": cfg.probe_port,
        "probe_attempts": cfg.probe_attempts,
        "probe_timeout_sec": cfg.probe_timeout,
        "nexthops": cfg.nexthops,
        "route_backend": cfg.route_backend,
    }


@app.get("/api/route", summary="查看当前通往采集端的路由")
def get_route():
    return routes.get()


@app.post("/api/trials", status_code=201,
          summary="发起试切：原子交接、换路并自换路成功起计时")
async def start_trial(body: TrialIn = Body(..., openapi_examples=TRIAL_EXAMPLES)):
    nh_ip = cfg.nexthops.get(body.nexthop)
    if nh_ip is None:
        raise HTTPException(
            422, f"未知下一跳 '{body.nexthop}'，预置可选：{sorted(cfg.nexthops)}")

    trial_id = "t" + uuid.uuid4().hex[:12]
    conflict: dict | None = None
    trial: dict | None = None
    switch_error: Exception | None = None
    failure_detail: str | None = None
    precheck_error: Exception | None = None

    # 关键交接区：到期回退 / 无主换路撤回 / 槽位检查 / 读实际原下一跳 /
    # 写意图标记 / 换路 / 存试切记录，全程同一把 flock。
    with store.locked() as state:
        reconcile_pending_switch_locked(state, routes)
        expire_if_needed(state, routes)

        cur = state.get("current")
        if cur and cur["status"] in ACTIVE_STATUSES:
            # 仍未到期的试切占用槽位；不做任何路由改动
            conflict = cur
        else:
            # 1) 换路前「实际」使用的下一跳 —— 必须在锁内、紧接换路前读取，
            #    这样看门狗不可能在读取与换路之间把上一笔回退掉。
            try:
                # 沿用 routes.get() 鸭子接口，便于测试替身；锁内紧接换路前读取
                original_nexthop = routes.get().get("nexthop")
            except Exception as exc:  # noqa: BLE001
                precheck_error = exc
                original_nexthop = None

            if precheck_error is None:
                # 2) 先持久化换路意图（撤回凭据），再碰路由。
                #    即使下一行换路后进程立刻被 SIGKILL，看门狗也能凭此撤回。
                switch_begin_ts = time.time()
                marker = {
                    "trial_id": trial_id,
                    "target": cfg.target,
                    "nexthop_name": body.nexthop,
                    "nexthop": nh_ip,
                    "original_nexthop": original_nexthop,
                    "created_at": now_iso(switch_begin_ts),
                    "created_ts": switch_begin_ts,
                }
                state["pending_switch"] = marker
                store.commit(state)

                # 3) 实际换路（可能很慢，也可能被故障钩子打断/杀死）
                try:
                    routes.install_host_route(nh_ip)
                except Exception as exc:  # noqa: BLE001 - 换路失败要清理标记并允许重试
                    switch_error = exc
                    actual_after_fail = None
                    with contextlib.suppress(Exception):
                        actual_after_fail = routes.get().get("nexthop")
                    repair_err = None
                    if actual_after_fail != original_nexthop:
                        # 路由已偏离换路前状态：尽力恢复，避免留下半切换路由
                        try:
                            routes.restore(original_nexthop)
                        except Exception as restore_exc:  # noqa: BLE001
                            repair_err = f"{type(restore_exc).__name__}: {restore_exc}"
                    state["pending_switch"] = None
                    # 显式落盘清理后的状态，再在临界区外抛错
                    store.commit(state)
                    if actual_after_fail == original_nexthop:
                        failure_detail = (
                            f"替换主机路由失败（原路由仍在 {original_nexthop!r}，"
                            f"已清理试切槽位，可直接重新发起）: "
                            f"{type(exc).__name__}: {exc}"
                        )
                    elif repair_err:
                        failure_detail = (
                            f"替换主机路由失败且路由状态不确定"
                            f"（actual={actual_after_fail!r}, expected="
                            f"{original_nexthop!r}，自动恢复失败: {repair_err}）；"
                            f"请检查 /api/route 后处理: {exc}"
                        )
                    else:
                        failure_detail = (
                            f"替换主机路由失败，但已恢复原下一跳 "
                            f"{original_nexthop!r}，可重新发起: {exc}"
                        )
                else:
                    # 4) 换路成功之后才起算试用时间
                    switched_ts = time.time()
                    trial = {
                        "trial_id": trial_id,
                        "target": cfg.target,
                        "nexthop_name": body.nexthop,
                        "nexthop": nh_ip,
                        "original_nexthop": original_nexthop,
                        "rollback_target": original_nexthop,
                        "switch_started_at": now_iso(switch_begin_ts),
                        "switch_started_ts": switch_begin_ts,
                        "started_at": now_iso(switched_ts),
                        "started_ts": switched_ts,
                        "switched_at": now_iso(switched_ts),
                        "switched_ts": switched_ts,
                        "deadline_at": now_iso(switched_ts + body.duration_sec),
                        "deadline_ts": switched_ts + body.duration_sec,
                        "duration_sec": body.duration_sec,
                        "status": "trialing",
                        "probe": {"state": "none"},
                        "rollback_reason": None,
                        "confirm_url": f"/api/trials/{trial_id}/confirm",
                        "rollback_url": f"/api/trials/{trial_id}/rollback",
                    }
                    state["current"] = trial
                    state["pending_switch"] = None
    # 临界区正常退出时会再原子写一次 state（trial 在此正式成为试切记录）

    if conflict is not None:
        raise HTTPException(
            409, f"已有进行中的试切 {conflict['trial_id']}，一次只允许一个试切；"
                 f"可先调用 rollback 提前回退或等待到期")
    if precheck_error is not None:
        raise HTTPException(
            502, "读取当前实际下一跳失败，未发起试切: "
                 f"{type(precheck_error).__name__}: {precheck_error}") from precheck_error
    if switch_error is not None:
        raise HTTPException(502, failure_detail) from switch_error
    return enrich(trial)


@app.post("/api/trials/current/rollback", status_code=200,
          summary="提前回退当前试切：立即恢复换路前下一跳")
def rollback_current_trial():
    return _rollback_current()


@app.post("/api/rollback", status_code=200, include_in_schema=False)
def rollback_top_level_alias():
    return _rollback_current()


@app.post("/api/trials/rollback", status_code=200, include_in_schema=False)
def rollback_collection_alias():
    return _rollback_current()


@app.delete("/api/trials/{trial_id}", status_code=200, include_in_schema=False)
def delete_trial_alias(trial_id: str):
    """DELETE 语义别名：对进行中试切等同提前回退；已回退记录幂等返回。"""
    return rollback_trial(trial_id)


def _rollback_current() -> dict:
    with store.locked() as state:
        reconcile_pending_switch_locked(state, routes)
        expire_if_needed(state, routes)
        cur = state.get("current")
        if not cur:
            raise HTTPException(409, "当前没有进行中的试切，无需回退")
        done = rollback_locked(state, routes, REASON_MANUAL)
    return enrich(done)


@app.post("/api/trials/{trial_id}/rollback", status_code=200,
          summary="提前回退指定试切：立即恢复其换路前实际下一跳")
@app.post("/api/trials/{trial_id}/cancel", status_code=200,
          include_in_schema=False)
@app.post("/api/trials/{trial_id}/abort", status_code=200,
          include_in_schema=False)
def rollback_trial(trial_id: str):
    # 兼容把字面量 current 当作 id 传入
    if trial_id == "current":
        return _rollback_current()
    with store.locked() as state:
        reconcile_pending_switch_locked(state, routes)
        expire_if_needed(state, routes)
        cur = state.get("current")
        if cur and cur["trial_id"] == trial_id:
            done = rollback_locked(state, routes, REASON_MANUAL)
            return enrich(done)
        if cur:
            raise HTTPException(
                409, f"当前试切是 {cur['trial_id']}，与请求的 {trial_id} 不符；"
                     f"一次只允许一个试切")
        for t in state["history"]:
            if t["trial_id"] == trial_id:
                if t.get("status") == "rolled_back":
                    # 幂等：看门狗/探测失败/重复点击已经回退过
                    return enrich(t)
                raise HTTPException(
                    409, f"试切 {trial_id} 已 {t.get('status')}，不能再回退")
    raise HTTPException(404, f"试切 {trial_id} 不存在")


@app.post("/api/trials/{trial_id}/confirm", status_code=202,
          summary="凭试切号确认：网关经新下一跳探测采集端")
async def confirm_trial(trial_id: str):
    housekeeping()
    expired = False
    snapshot = None
    with store.locked() as state:
        cur = state.get("current")
        if cur is None or cur["trial_id"] != trial_id:
            # 旧试切号 / 不存在的试切号：报错且不影响当前试切
            raise HTTPException(404, "试切号不存在或已结束；旧试切号不影响当前试切")
        if cur["status"] == "probing":
            raise HTTPException(409, "该试切的探测已在进行中")
        if cur["status"] != "trialing":
            raise HTTPException(409, f"试切已结束（{cur['status']}）")
        if time.time() > cur["deadline_ts"]:
            rollback_locked(state, routes, REASON_DEADLINE)
            expired = True
        else:
            cur["status"] = "probing"
            cur["probe"] = {"state": "running", "started_at": now_iso(), "attempts": 0}
            snapshot = enrich(cur)
    if expired:
        raise HTTPException(410, "已超过确认期限，试切已自动回退")
    asyncio.create_task(probe_and_finalize(trial_id))
    return snapshot


@app.get("/api/trials/current", summary="查看当前进行中的试切")
def current_trial():
    housekeeping()
    with store.locked() as state:
        cur = state.get("current")
        return {"current": enrich(cur) if cur else None}


@app.get("/api/trials", summary="试切列表：当前试切 + 历史结果")
def list_trials():
    housekeeping()
    with store.locked() as state:
        cur = state.get("current")
        return {
            "current": enrich(cur) if cur else None,
            "pending_switch": state.get("pending_switch"),
            "history": [enrich(t) for t in state["history"]],
        }


@app.get("/api/trials/{trial_id}", summary="查询试切结果与回退原因")
def get_trial(trial_id: str):
    housekeeping()
    with store.locked() as state:
        cur = state.get("current")
        if cur and cur["trial_id"] == trial_id:
            return enrich(cur)
        for t in state["history"]:
            if t["trial_id"] == trial_id:
                return enrich(t)
        marker = state.get("pending_switch")
        if marker and marker.get("trial_id") == trial_id:
            return {"trial_id": trial_id, "status": "interrupted_pending_recovery",
                    "pending_switch": marker}
    raise HTTPException(404, f"试切 {trial_id} 不存在")
