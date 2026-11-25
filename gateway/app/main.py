"""采集网关静态路由试切管理 API（FastAPI）。

流程：
  1. POST /api/trials            值班员选预置备用下一跳 + 试用秒数，
                                 网关替换通往采集端的 IPv4 主机路由并开始计时；
  2. POST /api/trials/{id}/confirm  凭试切号确认，网关经新下一跳 TCP 探测采集端；
     - 确认与探测都在期限内成功 -> 保留新路由（kept）
     - 探测失败               -> 立即恢复原下一跳（rolled_back/probe_failed）
  3. 到期仍未确认成功           -> 看门狗自动回退（rolled_back/deadline_exceeded），
                                 即使本 API 进程已停、或探测尚未返回。
"""
from __future__ import annotations

import asyncio
import time
import uuid

from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from .core import (
    ACTIVE_STATUSES,
    REASON_DEADLINE,
    REASON_PROBE_FAILED,
    Config,
    RouteManager,
    StateStore,
    expire_if_needed,
    keep_locked,
    load_config,
    now_iso,
    rollback_locked,
)

cfg = Config(load_config())
store = StateStore()
routes = RouteManager(cfg.target, backend=cfg.route_backend)

app = FastAPI(
    title="采集网关静态路由试切 API",
    version="1.0.0",
    description=(
        "现场采集网关的静态路由试切：替换通往采集端的 Linux IPv4 主机路由并计时；"
        "凭试切号确认后由网关经新下一跳探测采集端；确认与探测均在期限内成功才保留新路由，"
        "探测失败立即回退，到期未确认成功由独立看门狗自动回退（即使本 API 已停）。"
        "一次只允许一个试切；迟到的探测结果与旧试切号不影响当前试切。"
    ),
)


class TrialIn(BaseModel):
    nexthop: str = Field(..., description="预置备用下一跳名称（见 GET /api/config）")
    duration_sec: int = Field(
        30, ge=5, le=900,
        description="试用秒数：换路成功后开始计时，到期未确认成功即自动回退",
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
    "rollback_deadline": {
        "summary": "回退：到期未确认，看门狗自动回退",
        "description": "发起后不确认，15 秒到期由看门狗自动回退（即使停掉管理 API 也会回退）。",
        "value": {"nexthop": "backup-good", "duration_sec": 15},
    },
}


def enrich(trial: dict) -> dict:
    """附加剩余秒数，便于值班员查看。"""
    t = dict(trial)
    if t["status"] in ACTIVE_STATUSES:
        t["remaining_sec"] = round(max(0.0, t["deadline_ts"] - time.time()), 1)
    else:
        t["remaining_sec"] = 0.0
    return t


def expire_current() -> None:
    """懒回退：任何请求进来时先把已到期的试切回退掉（与看门狗同一逻辑）。"""
    with store.locked() as state:
        expire_if_needed(state, routes)


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
        # 试切已结束（看门狗/探测失败已回退）或已换成别的试切 -> 迟到结果，丢弃
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
          summary="发起试切：替换通往采集端的主机路由并开始计时")
async def start_trial(body: TrialIn = Body(..., openapi_examples=TRIAL_EXAMPLES)):
    nh_ip = cfg.nexthops.get(body.nexthop)
    if nh_ip is None:
        raise HTTPException(
            422, f"未知下一跳 '{body.nexthop}'，预置可选：{sorted(cfg.nexthops)}")
    expire_current()
    original = routes.get().get("nexthop")  # 试切前的下一跳（回退目标）
    started = time.time()
    trial_id = "t" + uuid.uuid4().hex[:12]
    with store.locked() as state:
        cur = state.get("current")
        if cur and cur["status"] in ACTIVE_STATUSES:
            raise HTTPException(
                409, f"已有进行中的试切 {cur['trial_id']}，一次只允许一个试切")
        try:
            routes.install_host_route(nh_ip)  # 实际换路，换路后开始计时
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(502, f"替换主机路由失败: {exc}") from exc
        trial = {
            "trial_id": trial_id,
            "target": cfg.target,
            "nexthop_name": body.nexthop,
            "nexthop": nh_ip,
            "original_nexthop": original,
            "started_at": now_iso(started),
            "started_ts": started,
            "deadline_at": now_iso(started + body.duration_sec),
            "deadline_ts": started + body.duration_sec,
            "duration_sec": body.duration_sec,
            "status": "trialing",
            "probe": {"state": "none"},
            "rollback_reason": None,
            "confirm_url": f"/api/trials/{trial_id}/confirm",
        }
        state["current"] = trial
        return enrich(trial)


@app.post("/api/trials/{trial_id}/confirm", status_code=202,
          summary="凭试切号确认：网关经新下一跳探测采集端")
async def confirm_trial(trial_id: str):
    expire_current()
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
    expire_current()
    with store.locked() as state:
        cur = state.get("current")
        return {"current": enrich(cur) if cur else None}


@app.get("/api/trials", summary="试切列表：当前试切 + 历史结果")
def list_trials():
    expire_current()
    with store.locked() as state:
        cur = state.get("current")
        return {
            "current": enrich(cur) if cur else None,
            "history": [enrich(t) for t in state["history"]],
        }


@app.get("/api/trials/{trial_id}", summary="查询试切结果与回退原因")
def get_trial(trial_id: str):
    expire_current()
    with store.locked() as state:
        cur = state.get("current")
        if cur and cur["trial_id"] == trial_id:
            return enrich(cur)
        for t in state["history"]:
            if t["trial_id"] == trial_id:
                return enrich(t)
    raise HTTPException(404, f"试切 {trial_id} 不存在")
