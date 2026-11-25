# 采集网关静态路由试切 API

现场采集网关的静态路由**试切**（trial switch）能力：值班员选择**预置备用下一跳**和**试用秒数**，
网关实际替换通往采集端的一条 Linux IPv4 主机路由（`/32` host route）并开始计时；凭本次试切号
发起确认后，网关经新下一跳对采集端做 TCP 探测。**确认与探测都在期限内成功才保留新路由**；
探测失败立即恢复原下一跳；到期仍未确认成功由**独立看门狗进程**自动回退——即使管理 API
已停止、或探测尚未返回。一次只允许一个试切；迟到的探测结果与旧试切号不影响当前试切。

## 架构

```
┌──────────────────────────── gateway 容器（NET_ADMIN）────────────────────────┐
│  entrypoint.sh                                                               │
│   ├─ 启动时装初始主机路由: <采集端>/32 via <主用下一跳>                       │
│   ├─ python3 -m app.watchdog   ← 独立看门狗：每 0.5s 扫状态文件，到期即回退   │
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

- **回退不依赖管理 API**：看门狗是独立进程（entrypoint 拉起、退出会自动重启）。
  API 进程被杀掉后，到期回退仍由看门狗执行；状态落盘，API 重启后可查结果。
- **一次一个试切**：`trialing`/`probing` 状态占用试切槽位，再发起返回 409。
- **迟到结果无效**：探测协程完成时校验「试切号一致 + 状态仍为 probing + 未超期」，
  三者任一不满足即丢弃；旧试切号确认返回 404，不影响当前试切。
- **跨进程互斥**：API 与看门狗的所有状态变更都在 `flock` 临界区内完成，
  到期回退在两个进程中是同一幂等逻辑（`expire_if_needed`），谁先抢到锁谁执行。
- **恢复原下一跳**：试切开始时用 `ip -j route get <采集端>` 记录原下一跳，
  回退时 `ip route replace` 恢复（原本无主机路由则删除试切路由）。

## 试切状态机

```
POST /api/trials ──► trialing ──confirm──► probing ──探测成功且在期限内──► kept（保留新路由）
                      │  │                    │
                      │  │                    ├─探测失败────────► rolled_back / probe_failed（立即回退）
                      │  └─confirm 已超期────► rolled_back / deadline_exceeded
                      └─到期未确认（看门狗）─► rolled_back / deadline_exceeded（探测在途也回退，迟到结果丢弃）
```

## API 一览（Swagger: `http://localhost:8080/docs`）

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/trials` | 发起试切：`{"nexthop": "backup-good", "duration_sec": 60}`；进行中再发起返回 409 |
| POST | `/api/trials/{trial_id}/confirm` | 凭试切号确认，网关经新下一跳探测采集端（202 受理，异步探测） |
| GET | `/api/trials/current` | 当前进行中的试切（含剩余秒数） |
| GET | `/api/trials` | 当前试切 + 历史结果 |
| GET | `/api/trials/{trial_id}` | 试切结果与回退原因（`rollback_reason`: `probe_failed` / `deadline_exceeded`） |
| GET | `/api/route` | 当前通往采集端的路由（实测换路/回退的证据） |
| GET | `/api/config` | 预置下一跳与探测参数 |

`POST /api/trials` 在 Swagger 内置三个命名样例，可直接试出两种结局：

- **keep**：`backup-good` + 60s → 期限内 confirm → 探测成功 → `kept`，路由保留；
- **rollback_probe_failed**：`backup-broken` + 60s → confirm → 探测超时失败 → 立即回退；
- **rollback_deadline**：任意下一跳 + 15s → 不确认 → 看门狗到期自动回退。

## 启动（Docker Compose）

```bash
docker compose up -d --build
# Swagger UI: http://localhost:8080/docs
docker compose exec gateway ip route get 172.30.0.100   # 初始：via 172.30.0.11
```

演示「管理 API 已停也回退」：

```bash
# 发起一个 15s 试切后不确认，然后停掉 API 进程（看门狗仍在）
docker compose exec gateway pkill -f 'uvicorn app.main'
sleep 16
docker compose exec gateway ip route get 172.30.0.100   # 已回退 via 172.30.0.11
docker compose logs gateway | grep watchdog             # 看门狗回退日志
docker compose restart gateway                          # 恢复管理 API
```

## 本地实测结果（本沙箱无 Docker，用 dryrun 路由后端等效跑通关键流程）

沙箱没有 Docker/容器运行时，无法在此执行 `docker compose up`。为此代码内置了
`TSW_ROUTE_BACKEND=dryrun` 联调后端：路由操作打印等效 `ip` 命令并维护模拟路由表，
**状态机、看门狗、并发互斥、探测逻辑与容器内完全一致**。以下为本沙箱真实执行输出摘要：

| 流程 | 操作 | 实测结果 |
|---|---|---|
| 保留 | 发起 `backup-good`/12s → 路由切到 `192.0.2.2` → confirm → 探测成功 | `status=kept`，`probe.detail=第 1 次探测成功：TCP 127.0.0.1:19000 连接建立`，`route_nexthop=192.0.2.2`；超过期限后路由仍为 `192.0.2.2`，看门狗无动作 |
| 探测失败回退 | 停掉采集端，发起试切 → confirm | 3 次探测 `ConnectionRefusedError` → `rolled_back / probe_failed`，路由立即恢复原下一跳 |
| 到期未确认回退 | 发起 10s 试切不确认 | 看门狗日志 `auto rolled back (reason=deadline_exceeded, restored via 192.0.2.2)`，路由恢复 |
| 一次一个试切 | 进行中再次 POST /api/trials | `409 已有进行中的试切 ...，一次只允许一个试切` |
| 旧试切号 | 用已结束试切号 confirm | `404 试切号不存在或已结束`；当前试切状态不受影响 |
| API 停止仍回退 | 发起 10s 试切 → `kill` API 进程 → 等待 | API 不可达情况下看门狗按期回退，状态文件记录 `rolled_back / deadline_exceeded`；重启 API 后结果可查 |
| 迟到探测丢弃 | 探测重试 40s 跨越 8s 期限 | 到期时探测仍在进行，看门狗先回退（`deadline_exceeded`）；探测结束后结果被丢弃，记录与路由均不再变化 |
| Swagger 样例 | GET /openapi.json | `keep` / `rollback_probe_failed` / `rollback_deadline` 三个命名样例均在请求体下拉框中 |

复现方式（无需 Docker）：

```bash
pip install fastapi==0.115.6 uvicorn==0.34.0 PyYAML==6.0.2
export TSW_CONFIG=<测试配置> TSW_STATE_DIR=<状态目录> TSW_ROUTE_BACKEND=dryrun
python3 -m app.watchdog &                      # 看门狗
python3 -m uvicorn app.main:app --port 18000 & # 管理 API
```

## 未完成项 / 限制

1. **未在真实 Docker 环境实跑**：本沙箱无容器运行时，`docker compose up`、容器内真实
   `ip route` 生效、跨容器转发（`backup-good` 通 / `backup-broken` 断）未实际执行。
   已在有 Docker 的主机上按上文步骤验证前，建议先 `docker compose config` 检查。
   compose 与 `config.yaml` 的地址/转发开关已做交叉一致性校验。
2. **容器重启不保留试切现场**：容器重启会重建网络命名空间，entrypoint 重新装初始路由；
   进行中的试切状态文件虽在，但路由已复位（看门狗到期仍会尝试回退，幂等无害）。
3. **无认证鉴权**：按内网管理面假设设计，未接认证；生产暴露前应加认证/鉴权。
4. **探测为 TCP 连接探测**：判活采集端 `probe_port`；如需 ICMP 或应用层探活可替换
   `tcp_probe` 实现。
5. **无手动取消接口**：值班员只能等期限到期或确认定夺，未提供主动 abort。
