# 采集网关静态路由试切 API

现场采集网关的静态路由**试切**（trial switch）能力：值班员选择**预置备用下一跳**和**试用秒数**，
网关实际替换通往采集端的一条 Linux IPv4 主机路由（`/32` host route），**从换路成功起算试用时间**；
凭本次试切号发起确认后，网关经新下一跳对采集端做 TCP 探测。**确认与探测都在期限内成功才保留新路由**；
探测失败立即恢复原下一跳；值班员也可**随时提前回退**；到期仍未确认成功由**独立看门狗进程**自动回退——
即使管理 API 已停止、或探测尚未返回。若 API 在**换路已生效、试切记录尚未保存**的窗口内退出，
看门狗会依据换路前落盘的意图标记**独立撤回这次换路**。一次只允许一个试切；迟到的探测结果与旧试切号不影响当前试切。

> **本次修复的交接问题**：上一笔试切到期、看门狗正在回退时发起新试切，旧实现在状态锁之外读取
> `ip route get`，可能读到上一笔的备用下一跳；若看门狗恰好在读之后、写新记录之前完成恢复，
> 新试切就会把**上一笔的备用下一跳误记为自己的回退目标**。现在「到期交接 → 读换路前实际下一跳 →
> 写换路意图 → 换路 → 存试切记录」全部在同一把 `flock` 临界区内完成，问题消除。
> 详见下文「试切发起交接协议」与「实测：旧代码确定性复现 / 新代码通过」。

## 架构

```
┌──────────────────────────── gateway 容器（NET_ADMIN）────────────────────────┐
│  entrypoint.sh                                                               │
│   ├─ 启动时装初始主机路由: <采集端>/32 via <主用下一跳>                       │
│   ├─ python3 -m app.watchdog   ← 独立看门狗：每 0.5s 扫状态文件               │
│   │                              到期回退 + 撤回无主 pending_switch           │
│   └─ uvicorn app.main:app      ← 管理 API（FastAPI，:8000）                  │
│  两进程通过 /state/state.json（flock 互斥 + 原子写）共享试切状态             │
└──────────────────────────────────────────────────────────────────────────────┘
        │ 172.30.0.10            fieldnet 172.30.0.0/24（Docker bridge，隔离环境）
  ┌─────┼──────────────────┬───────────────────┬───────────────────┐
  ▼     ▼                  ▼                   ▼                   ▼
collector            nh-primary         nh-backup-good       nh-backup-broken
172.30.0.100:9000    172.30.0.11        172.30.0.12          172.30.0.13
（采集端，探测目标）  ip_forward=1        ip_forward=1         ip_forward=0（不转发）
```

关键设计：

- **试切发起交接协议（本次修复核心）**：`POST /api/trials` 在**同一把 flock 临界区**内依次完成
  ①撤回无主换路 ②到期试切回退 ③槽位检查 ④`ip route get` 读换路前**实际**下一跳
  ⑤落盘 `pending_switch` 换路意图 ⑥实际换路 ⑦写试切记录（计时起点取在换路成功之后）。
  看门狗的回退也持同一把锁，因此不可能插进④～⑦之间制造陈旧读取。每笔试切的
  `original_nexthop` / `rollback_target` 必然是**它自己换路前一刻实际在用的下一跳**。
- **换路意图先落盘（崩溃窗口保护）**：碰路由之前先把
  `pending_switch={trial_id, nexthop, original_nexthop, ...}` 原子写入 state.json。
  若 API 在换路生效后、试切记录保存前被 `SIGKILL`/崩溃，内核释放 flock；看门狗拿到锁后看到
  无主 `pending_switch`，立即 `ip route replace` 回原下一跳，并写一条
  `rolled_back / api_interrupted` 历史。换路实际没生效时该恢复也是幂等无副作用的。
- **试用时间从换路成功起算**：`started_ts = switched_ts = install_host_route()` 成功返回后的时刻，
  `deadline_ts = switched_ts + duration_sec`。慢换路（含注入延迟）消耗的是换路时间，不消耗试用期。
- **回退不依赖管理 API**：看门狗是独立进程（entrypoint 拉起、退出会自动重启）。
  API 进程被杀掉后，到期回退与无主换路撤回仍由看门狗执行；状态落盘，API 重启后可查结果。
- **允许提前回退**：`POST /api/trials/{id}/rollback`（另有 `…/current/rollback`）立即恢复原下一跳，
  原因记为 `manual_rollback`；探测在途也可回退，迟到探测结果按试切号+状态校验丢弃。
  重复回退同一笔已回退试切返回 200（幂等）。**最迟**在 `deadline_ts` 由看门狗恢复。
- **换路失败可重新发起**：换路命令抛错时清理 `pending_switch` 与试切槽位；若实测原路由仍在，
  返回 502 且明示「可直接重新发起」；若路由已偏离则尽力恢复原下一跳。不会留下永久 409 的死槽位。
- **一次一个试切**：`trialing`/`probing` 状态占用试切槽位，再发起返回 409；到期边界的请求会在
  同一临界区内先完成交接再创建新试切。并发发起恰好一个 201，其余 409。
- **迟到结果无效**：探测协程完成时校验「试切号一致 + 状态仍为 probing + 未超期」，
  三者任一不满足即丢弃；旧试切号确认返回 404，不影响当前试切。
- **跨进程互斥**：API 与看门狗的所有状态变更都在 `flock` 临界区内完成，
  到期回退在两个进程中是同一幂等逻辑（`expire_if_needed`），谁先抢到锁谁执行。

## 试切状态机

```
POST /api/trials ──► trialing ──confirm──► probing ──探测成功且在期限内──► kept（保留新路由）
   │                  │  │                    │
   │                  │  │                    ├─探测失败────────► rolled_back / probe_failed
   │                  │  ├─POST .../rollback──┼─（任何时刻）────► rolled_back / manual_rollback（提前）
   │                  │  └─confirm 已超期────► rolled_back / deadline_exceeded
   │                  └─到期未确认（看门狗）─► rolled_back / deadline_exceeded（探测在途也回退）
   └─API 换路后存档前死亡：pending_switch ───► rolled_back / api_interrupted（看门狗独立撤回）
```

## API 一览（Swagger: `http://localhost:8080/docs`）

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/trials` | 发起试切：`{"nexthop": "backup-good", "duration_sec": 60}`；进行中再发起返回 409 |
| POST | `/api/trials/{trial_id}/confirm` | 凭试切号确认，网关经新下一跳探测采集端（202 受理，异步探测） |
| POST | `/api/trials/{trial_id}/rollback` | **提前回退**：立即恢复该笔试切换路前的实际下一跳（`manual_rollback`，幂等） |
| POST | `/api/trials/current/rollback` | **提前回退当前试切**（Swagger 演示最顺手，不用粘 id） |
| GET | `/api/trials/current` | 当前进行中的试切（含剩余秒数、`rollback_target`） |
| GET | `/api/trials` | 当前试切 + `pending_switch` + 历史结果 |
| GET | `/api/trials/{trial_id}` | 试切结果与回退原因（`rollback_reason`: `manual_rollback` / `probe_failed` / `deadline_exceeded` / `api_interrupted`） |
| GET | `/api/route` | 当前通往采集端的路由（实测换路/回退的证据） |
| GET | `/api/config` | 预置下一跳与探测参数 |

每笔试切记录的关键字段：

```json
{
  "trial_id": "t1a2b3c4d5e6",
  "nexthop": "172.30.0.12",
  "original_nexthop": "172.30.0.11",
  "rollback_target": "172.30.0.11",
  "switch_started_ts": 1795...,
  "started_ts": 1795...,
  "switched_ts": 1795...,
  "deadline_ts": 1795...,
  "remaining_sec": 59.8,
  "status": "trialing",
  "rollback_url": "/api/trials/t1a2b3c4d5e6/rollback"
}
```

`POST /api/trials` 在 Swagger 内置四个命名样例（keep / rollback_early /
rollback_probe_failed / rollback_deadline），可直接试出各种结局。

## 用 Docker Compose 启动 + Swagger 演示连续试切和回退

```bash
docker compose up -d --build
# Swagger UI: http://localhost:8080/docs
docker compose exec gateway ip route get 172.30.0.100   # 初始：via 172.30.0.11
```

在 Swagger 上演示「连续试切 + 回退交接」（每一步都可同时 `GET /api/route` 看真实路由）：

1. `POST /api/trials` → 选 **rollback_early** 样例（`backup-good`, 60s）→ Execute，
   记下响应里的 `trial_id` 与 `original_nexthop=172.30.0.11`；
2. `POST /api/trials/{trial_id}/rollback` → 200，`rollback_reason=manual_rollback`；
   `GET /api/route` 仍是 via `172.30.0.11`；
3. **不要停，立刻**再 `POST /api/trials`（可换 `backup-two` 或仍用 `backup-good`）→
   新记录的 `original_nexthop` 仍然是 `172.30.0.11`，**不会**变成上一笔的 `172.30.0.12`；
   再用 `POST /api/trials/current/rollback` 一键回退；
4. 连续重复第 3 步若干次；也可试另外两种结局：
   - **keep**：`backup-good` + 60s → 期限内 confirm → 探测成功 → `kept`，路由保留；
   - **probe_failed**：`backup-broken` + 60s → confirm → 探测超时失败 → 立即回退；
   - **deadline**：`backup-good` + 15s → 不确认 → 看门狗到期自动回退。
5. `GET /api/trials` 查看全部历史：每一笔的 `original_nexthop` 都应是 `172.30.0.11`。

等价的命令行/Swagger 请求序列脚本（无需浏览器）：

```bash
python3 tests/demo_swagger_flow.py     # 自动拉起 dryrun 线束并打印完整对话
```

## 故障演练（慢换路 / API 中断 / 换路失败）

代码内置了默认不触发的文件型故障钩子，容器里用 `docker compose exec ... touch/写文件`
即可动态触发，不需要重启 API；本地 dryrun 同样可用（钩子路径默认在 `$TSW_STATE_DIR`）。

| 钩子文件 | 作用 |
|---|---|
| `$TSW_STATE_DIR/slow_switch` | 内容为秒数；换路前 sleep 该秒数，模拟慢换路（也可用环境变量 `TSW_ROUTE_DELAY_SEC`） |
| `$TSW_STATE_DIR/crash_after_switch` | 换路一生效立即 `SIGKILL` 本 API 进程（一次性，触发前自删文件），精确复现「换路后、存档前退出」 |
| `$TSW_STATE_DIR/gate_after_switch` | 换路后挂在存档前窗口，并写 `gate_after_switch.switched` 标记；`touch …/gate_after_switch.continue` 放行，或直接 `kill -9` API |
| `$TSW_STATE_DIR/fail_switch` | 文件存在时换路直接失败（模拟 netlink 拒绝），原路由保持不变；删文件后可立即重试 |

### 演练 1：管理 API 已停，看门狗到期回退（原有场景）

```bash
# 发起一个 15s 试切后不确认，然后停掉 API 进程（看门狗仍在）
curl -s -X POST localhost:8080/api/trials -H 'Content-Type: application/json' \
  -d '{"nexthop":"backup-good","duration_sec":15}'
docker compose exec gateway pkill -f 'uvicorn app.main'
sleep 16
docker compose exec gateway ip route get 172.30.0.100   # 已回退 via 172.30.0.11
docker compose logs gateway | grep watchdog             # watchdog ... auto rolled back
docker compose restart gateway                          # 恢复管理 API
```

### 演练 2：换路生效后、保存试切记录前 API 退出 —— 看门狗独立撤回（本次新增保护）

最简单的一次性故障自杀钩子：

```bash
docker compose exec gateway touch /state/crash_after_switch
curl -s -X POST localhost:8080/api/trials -H 'Content-Type: application/json' \
  -d '{"nexthop":"backup-good","duration_sec":60}'
# curl: (52) Empty reply；API 进程已 SIGKILL，但看门狗还活着
docker compose exec gateway ip route get 172.30.0.100   # 瞬间仍是 via .12
sleep 1
docker compose exec gateway ip route get 172.30.0.100   # 看门狗已撤回：via .11
docker compose exec gateway cat /state/state.json       # history: api_interrupted
docker compose logs gateway | grep 'orphan switch'      # [watchdog] reverted orphan switch ...
docker compose restart gateway                          # API 恢复后 GET /api/trials 可查
```

用闸门 + 外部 `kill -9` 做更真实的演练（可精确停在崩溃窗口里核对）：

```bash
docker compose exec gateway touch /state/gate_after_switch
# 在宿主机后台发起会挂住的请求：
curl -s -X POST localhost:8080/api/trials -H 'Content-Type: application/json' \
  -d '{"nexthop":"backup-good","duration_sec":60}' &
docker compose exec gateway sh -c \
  'while [ ! -f /state/gate_after_switch.switched ]; do sleep 0.1; done'
docker compose exec gateway ip route get 172.30.0.100   # 已 via .12
docker compose exec gateway cat /state/state.json \
  | grep -A3 pending_switch                             # 只有 pending_switch，没有 current
docker compose exec gateway pkill -9 -f 'uvicorn app.main'
sleep 1
docker compose exec gateway ip route get 172.30.0.100   # 看门狗撤回 via .11
docker compose exec gateway rm -f /state/gate_after_switch \
  /state/gate_after_switch.switched                     # 清理钩子
```

### 演练 3：慢换路不占用试用期

```bash
docker compose exec gateway sh -c 'echo 3 > /state/slow_switch'
time curl -s -X POST localhost:8080/api/trials -H 'Content-Type: application/json' \
  -d '{"nexthop":"backup-good","duration_sec":30}' | python3 -m json.tool
# 请求总耗时 ≈3s，但响应里 remaining_sec ≈30.0（旧实现会只剩 ≈27s）
docker compose exec gateway rm -f /state/slow_switch
```

### 演练 4：换路失败、原路由还在、允许重新发起

```bash
docker compose exec gateway touch /state/fail_switch
curl -s -X POST localhost:8080/api/trials -H 'Content-Type: application/json' \
  -d '{"nexthop":"backup-good","duration_sec":30}'
# 502：…原路由仍在 '172.30.0.11'，已清理试切槽位，可直接重新发起
docker compose exec gateway ip route get 172.30.0.100   # 仍是 via .11
docker compose exec gateway rm /state/fail_switch
curl -s -X POST localhost:8080/api/trials -H 'Content-Type: application/json' \
  -d '{"nexthop":"backup-good","duration_sec":30}'      # 201，重试成功
```

## 本地实测结果（本沙箱无 Docker daemon，用真实双进程 + dryrun/假 ip 后端等效实测）

本交付所在沙箱内没有 `docker` CLI/daemon、没有 `CAP_SYS_ADMIN/NET_ADMIN`、user namespace
被 seccomp 禁止，**无法在此真正执行 `docker compose up`**（见文末「未完成项」）。
作为替代，以下测试都启动**真实的独立看门狗进程 + 真实 uvicorn 进程**，跑真实的 FastAPI 路由、
`flock` 临界区、状态文件、异步探测与 SIGKILL 交接；`dryrun` 后端维护模拟路由表，
另用一个实现了 `ip -j route get/replace/del` 的假 `ip` 可执行文件确定性放大竞态。
**状态机、锁交接、看门狗、崩溃恢复逻辑与容器内逐行相同**，仅差真实内核路由与跨容器转发。

复现：

```bash
python3 -m pip install -r gateway/requirements.txt   # fastapi / uvicorn / PyYAML
# 或 python3 -m venv .venv && . .venv/bin/activate && pip install -r gateway/requirements.txt

# A. 99 项端到端断言：交接/慢换路/崩溃自杀/闸门+SIGKILL/失败重试/提前+到期回退/探测
python3 tests/verify_handoff.py

# B. 确定性陈旧读竞态：对修复后代码应 VERDICT=OK(exit 0)
python3 tests/verify_stale_get_race.py --gateway gateway
#    对 git HEAD 的旧代码归档应 VERDICT=BUG_REPRODUCED(exit 3)
git archive HEAD gateway | tar -x -C /tmp/oldgw --strip-components=1
python3 tests/verify_stale_get_race.py --gateway /tmp/oldgw

# C. Swagger 连续试切演示的等价 HTTP 对话
python3 tests/demo_swagger_flow.py
```

本次交付前的真实运行结果摘要：

| 实测项 | 方法 | 结果 |
|---|---|---|
| 回退交接（连续 8 轮到期边界 + 5 线程并发抢交接 + 全历史不变量） | `verify_handoff.py` §1 | 全部 PASS；每笔 `original_nexthop=10.99.0.11`；并发恰好 1×201 / 4×409 |
| **旧代码确定性复现记错目标** | 假 `ip`：get 先读上一笔 `.12` 再 sleep 0.9s，看门狗期间恢复 `.11` | 旧 HEAD：`first POST -> 201 in 0.95s`，`original_nexthop=172.30.0.12`（上一笔备用），`VERDICT=BUG_REPRODUCED` exit 3 |
| **同一竞态对新代码** | 同上 | 新代码首次 POST 立即 409（锁内见未到期），重试后 `original_nexthop=172.30.0.11`，`VERDICT=OK` exit 0 |
| 真实 `iproute2` 后端 | 解压 Debian `iproute2_6.1.0` 二进制（免 root），`TSW_ROUTE_BACKEND=ip` | 真实 `ip -j route get 8.8.8.8` JSON 解析出 `gateway=172.17.0.1`；无 `CAP_NET_ADMIN` 时真实 `ip route replace` 被拒，API 返回 502 且判定「原路由仍在」、槽位清空，再次 POST 仍为 502（可重试）而非 409 |
| 并发浸泡 | 6 线程 ×15s POST/rollback + 读压力 | **420 笔试切**，0 个错误状态、0 个错误 `original_nexthop`、无残留 `pending_switch`/`current` |
| 慢换路 | `slow_switch=1.2` | POST 耗时 1.20s；`started_ts-switch_started_ts=1.20s`；响应 `remaining_sec=5.0`（旧计时会是 ≈3.8） |
| API 中断（故障自杀） | `crash_after_switch` | uvicorn `returncode=-9`；死时路由已是 `.12`；**API 未重启**，看门狗（测试轮询 0.05s）立即恢复 `.11`；state 有 `api_interrupted`；重启 API 后可查 |
| API 中断（真实 SIGKILL + 闸门） | `gate_after_switch` 等到 `.switched` 后 `kill -9` | 闸门时 `pending_switch` 存在、`current=None`、路由 `.12`；kill 后看门狗独立恢复 `.11` 并留痕 |
| 换路失败重试 | `fail_switch` | 502 且提示可重新发起；路由仍 `.11`；`current=null`；删钩子后 POST 201 并可回退 |
| 提前回退 / 幂等 / 409 | §5 | `manual_rollback` 立即恢复；重复回退 200；无试切时 current/rollback=409；进行中再 POST=409 |
| 探测在途时提前回退 | 30 次探测的慢目标 | 回退立即生效；2.5s 后终态仍是 `rolled_back/manual_rollback`，迟到探测未改写 |
| 到期回退 | duration=5 不确认 | 看门狗写 `deadline_exceeded` 并恢复路由 |
| 探测成功保留（回归） | 本地真实 `http.server` 采集端 | `kept`，路由保留在 backup-good |
| Swagger/OpenAPI | `demo_swagger_flow.py` | `/docs` 200；9 个 paths；4 个 POST 样例（含 `rollback_early`）；连续 4 笔历史回退目标全为 primary |
| 合计 | `verify_handoff.py` | **99 项断言全部 PASS** |

## 未完成项 / 限制（如实说明）

1. **未在本沙箱真正执行 `docker compose up`**：环境无 docker CLI/daemon、无任何 capability、
   user namespace 被 seccomp 拒绝，无法运行容器或 dockerd。因此**真实 `ip route` 在内核中的生效、
   NET_ADMIN 权限、`backup-good`/`backup-broken` 的跨容器转发差异**没有由我本人实跑验证；
   `docker-compose.yml` 仅做了 YAML/服务/网络一致性核对（该文件本次未改）。
   已用 dryrun 双进程验证全部并发/崩溃逻辑、用假 `ip` 确定性复现并锁死交接竞态，
   还用真实 `iproute2 6.1.0` 二进制验证了 `-j route get` 的 JSON 解析与权限不足时的
   换路失败/重试路径；唯独**成功的 `ip route replace` 内核生效与跨容器转发**无法在本沙箱验证。
   **在有 Docker 的主机上交付前仍应按上文步骤实跑一次**，尤其是「演练 2」。
2. **容器重启不保留试切现场**：容器重启会重建网络命名空间，entrypoint 重新装初始路由；
   进行中的试切状态文件虽在，但路由已复位（看门狗到期仍会尝试回退，幂等无害）。
3. **无认证鉴权**：按内网管理面假设设计，未接认证；生产暴露前应加认证/鉴权。
4. **探测为 TCP 连接探测**：判活采集端 `probe_port`；如需 ICMP 或应用层探活可替换
   `tcp_probe` 实现。
5. ~~无手动取消接口~~ **已提供** `POST /api/trials/{id}/rollback` 与
   `POST /api/trials/current/rollback`（提前回退，`manual_rollback`）。
6. **故障钩子是测试/演练设施**：`slow_switch`/`crash_after_switch`/`gate_after_switch`/
   `fail_switch` 只应出现在隔离演练环境；生产镜像若担心被滥用，可通过不挂载可写 `/state`
   给非特权用户或裁剪这些钩子来收口（当前 `/state` 仅容器内 root/entrypoint 可触达）。
