# Agent 常驻与本地索引设计

日期：2026-06-21

本文定义 Turbk Agent 的 7x24 常驻模式、本地 SQLite catalog、服务端异步命令和 chunk generation 同步机制。该设计基于当前已有的 Agent HTTP API、Client ID / Secret 认证、服务端 canonicalize manifest、append-only segment 仓库和维护任务。

## 1. 背景

当前 Agent 是一次性执行模型：启动后发送心跳，创建 run，扫描本地目录，逐 chunk 查询服务端是否存在，缺失则上传，最后提交 manifest。这个模型可以通过 cron 或 systemd timer 定时运行，但有几个问题：

- 每次运行都需要重新扫描和读取大量本地元数据。
- 对本地已知存在于服务端的 chunk 仍可能产生大量查询请求。
- 手动运行 Agent job 时，服务端不能直接唤醒公网后的 Agent，只能依赖客户端主动连接。
- 公网环境不适合长期稳定维持 WebSocket 或其他长连接。
- 多次触发可能导致同一 Agent 并发运行，需要明确互斥和丢弃策略。

目标是让 Agent 支持常驻值守，由 Agent 最终决定何时开始备份，同时仍能通过服务端统一管理策略、手动触发和状态展示。

## 2. 目标

- Agent 支持 7x24 daemon 模式，并保留一次性运行模式。
- Agent 使用本地 SQLite catalog 保存文件元数据、文件 chunk 列表和服务端 chunk 确认状态。
- 对本地 catalog 已确认存在于服务端的 chunk，不再逐个请求服务端。
- 服务端提供 generation 同步机制，保证本地 catalog 失效时可以发现并修复。
- 服务端手动运行采用异步命令队列，由 Agent 轮询领取，不依赖公网长连接。
- Agent 同一时间只能执行一个备份任务；多余任务直接丢弃并上报原因，不在本地排队。
- manifest 引用缺失 chunk 时，Agent 自动补传并重试提交。
- 客户端接入页面展示 daemon 部署方式、持久状态目录和同步状态。

## 3. 非目标

- 不要求 Agent 和服务端维持长连接。
- 不把 Agent 本地 catalog 当作权威数据源。
- 不在 Agent 本地保存 chunk 内容；本地只保存索引和元数据。
- 不实现多任务并发备份。
- 不实现 Agent 端恢复回写。
- 不实现服务端不可见的客户端零信任加密。

## 4. 核心原则

- 服务端仍是最终真相：snapshot、manifest、chunk index 和 segment record 以服务端为准。
- Agent 本地 catalog 只用于减少扫描、读取和网络请求。
- Agent 提交 manifest 时可以只依赖 chunk hash；服务端继续 canonicalize 为当前仓库内权威 `ChunkRef`。
- Generation 只用于判断本地正向缓存是否需要校验，不能替代服务端 manifest 校验。
- Agent 不维护长任务队列。同一时间只运行一个任务，忙碌时收到的其他触发直接标记为 `dropped`。
- 常规备份路径仍只追加写 segment；compact 和清理继续由服务端维护窗口负责。

## 5. Agent 运行模式

### 5.1 Once 模式

一次性模式保留，用于手动调试、cron/systemd timer 和向后兼容：

```text
turbk-agent -server ... -client-id ... -client-secret ... -root ... -once
```

行为和当前模型基本一致，但可以复用本地 catalog。

### 5.2 Daemon 模式

新增 daemon 模式：

```text
turbk-agent -daemon
```

或通过环境变量启用：

```text
TURBK_AGENT_DAEMON=true
TURBK_AGENT_STATE_DIR=/var/lib/turbk-agent
```

Daemon 主循环：

1. 启动后打开本地 SQLite catalog。
2. 发送 heartbeat/poll 请求。
3. 获取服务端策略、generation、维护状态和待处理命令。
4. 根据本地状态和服务端策略判断是否启动一次备份。
5. 如果正在备份，继续上报进度，其他触发直接丢弃。
6. 根据服务端返回的 `poll_interval`、`retry_after` 和本地退避策略等待下一轮。

默认轮询间隔建议为 10 分钟，并加入随机抖动，避免大量 Agent 同时请求服务端。备份不需要强实时性，手动运行命令接受分钟级延迟；如果某个部署需要更快响应，可以在服务端按 Agent 下发更短的 `poll_interval`。

首版 daemon 除了服务端 command，也提供本地 `backup_schedule` 作为兜底定期触发，默认 `0 0 * * *`。后续如果服务端下发更完整的 schedule policy，Agent 仍保留最终是否执行的决定权，并继续遵守单任务互斥和丢弃策略。

## 6. 单任务互斥与丢弃策略

Agent 必须保证同一时间最多一个备份任务运行。

互斥分两层：

- 进程内互斥：daemon 内部用 mutex/atomic state 防止重复启动。
- 状态目录互斥：使用本地 SQLite lease 或 lock file 防止多个 Agent 进程共享同一 `state_dir` 时并发运行。

触发来源包括：

- 服务端手动运行命令。
- 服务端下发的 schedule 策略。
- 本地 fallback schedule。
- daemon 启动后的首次立即运行策略。

如果 Agent 已有任务运行：

- 新的手动命令标记为 `dropped`，原因 `agent_busy`。
- schedule 触发不进入队列，只记录一次 `skipped_busy` 状态。
- 离线期间积压的多个 due 时间点不补跑多次，最多合并为一次最新备份机会。

该策略的目标是避免公网延迟、重复点击或调度抖动导致本地磁盘和网络被并发扫描压垮。

## 7. 本地 SQLite Catalog

Agent 本地状态目录默认为：

```text
/var/lib/turbk-agent
```

Docker 部署必须把该目录挂载为持久卷。否则容器删除后 catalog 丢失，Agent 会退化为一次性全量扫描和服务端查询模型。

建议数据库文件：

```text
/var/lib/turbk-agent/catalog.db
```

### 7.1 表结构草案

`agent_meta`：

```text
key TEXT PRIMARY KEY
value TEXT NOT NULL
updated_at DATETIME NOT NULL
```

保存 schema version、agent id、最近服务端信息等。

`server_state`：

```text
server_url TEXT NOT NULL
client_id TEXT NOT NULL
repository_id TEXT NOT NULL
chunk_generation INTEGER NOT NULL
last_invalidation_generation INTEGER NOT NULL
config_generation INTEGER NOT NULL
command_generation INTEGER NOT NULL
last_heartbeat_at DATETIME
PRIMARY KEY (server_url, client_id)
```

`last_invalidation_generation` 表示本地已经完整应用 invalidation journal 的最高 generation；它用于区分“已经精确同步过失效 hash”和“只知道服务端 generation 变了但还未同步失效明细”。

`files`：

```text
root_id TEXT NOT NULL
path TEXT NOT NULL
type TEXT NOT NULL
size INTEGER NOT NULL
mode INTEGER NOT NULL
uid INTEGER NOT NULL
gid INTEGER NOT NULL
mtime_ns INTEGER NOT NULL
dev INTEGER
inode INTEGER
link_target TEXT
chunk_fingerprint TEXT
updated_at DATETIME NOT NULL
PRIMARY KEY (root_id, path)
```

`file_chunks`：

```text
root_id TEXT NOT NULL
path TEXT NOT NULL
ordinal INTEGER NOT NULL
hash TEXT NOT NULL
original_size INTEGER NOT NULL
PRIMARY KEY (root_id, path, ordinal)
```

`server_chunks`：

```text
hash TEXT PRIMARY KEY
original_size INTEGER NOT NULL
status TEXT NOT NULL          -- confirmed | missing | unknown
confirmed_generation INTEGER
last_checked_at DATETIME
last_uploaded_at DATETIME
updated_at DATETIME NOT NULL
```

`agent_runs`：

```text
local_run_id TEXT PRIMARY KEY
server_run_id INTEGER
command_id INTEGER
trigger TEXT NOT NULL         -- schedule | manual | startup | once
status TEXT NOT NULL          -- running | completed | failed | dropped
started_at DATETIME NOT NULL
finished_at DATETIME
message TEXT
```

### 7.2 文件级复用

扫描文件时，Agent 先读取文件元数据。若 path、type、size、mode、uid、gid、mtime、dev、inode 与本地 `files` 表一致，则认为文件未变化：

- 不重新读取文件内容。
- 直接复用 `file_chunks` 中的 chunk hash 列表。
- 对这些 chunk 只在 generation 需要时做批量校验。

如果元数据变化，则重新读取文件并分块。分块后更新 `files` 和 `file_chunks`。

首版默认采用快速元数据判断。后续可以增加 paranoid 模式，对关键路径周期性读取内容 hash。

## 8. Generation 模型

Generation 用于让 Agent 知道“本地已确认存在的 chunk 是否仍可直接相信”。它不是安全边界，最终正确性仍由服务端 manifest 校验保证。

建议服务端维护四类版本：

```text
repository_id       -- 仓库 UUID，state/repo 重新初始化时变化
chunk_generation    -- chunk 正向缓存失效代数
config_generation   -- host/job/agent 策略变化代数
command_generation  -- agent 异步命令变化代数
```

### 8.1 repository_id

`repository_id` 是服务端仓库身份。服务端首次初始化时生成并持久化。

Agent 行为：

- 如果 heartbeat 返回的 `repository_id` 和本地一致，可以继续使用本地 catalog。
- 如果不一致，说明服务端仓库可能被重建或切换；Agent 必须把所有 `server_chunks` 标记为 `unknown`。
- 文件级 chunk 列表可以保留，因为它来自本地源文件；但所有服务端存在性都要重新确认或重新上传。

### 8.2 chunk_generation

`chunk_generation` 是 Agent 正向 chunk 缓存的失效代数。

它在以下情况递增：

- `cleanup-errors` 删除 orphan chunk index。
- `compact` 删除不再被 active snapshot 引用的旧 segment 或 chunk index。
- 仓库 verify/repair/reindex 改变 chunk index 成员。
- 管理员执行仓库恢复、索引重建或其他可能让 chunk 存在性收缩的维护动作。

它不需要在普通 chunk 上传时递增。普通上传只会让服务端 chunk 集合增加，不会让 Agent 已确认存在的 chunk 变成不存在。

`chunk_generation` 可以按保守策略递增。例如 compact 即使只改写物理 segment 位置、没有减少 hash 集合，也可以递增 generation。因为 Agent 提交 manifest 时只需要 hash，服务端会 canonicalize 物理 `ChunkRef`，所以这种保守递增最多导致下一次备份多做一次批量 check，不会影响正确性。

### 8.3 Chunk Invalidation Journal

`chunk_generation` 只表示“本地 chunk 正向缓存可能过期”，不直接告诉 Agent 哪些 chunk 失效。为了避免 generation 变化后对大量 chunk 做批量检查，服务端维护一个可丢弃的 chunk invalidation journal。

Journal 记录在某个 generation 中失效的 chunk hash：

```text
generation
hash
reason              -- cleanup-errors | compact | reindex | repair | repository-reset
created_at
```

Agent 发现 `chunk_generation` 增加后，先请求 invalidation journal：

```http
GET /agent/v1/chunks/invalidations?since=12
```

如果服务端仍保留完整日志，返回：

```json
{
  "repository_id": "repo-a",
  "from_generation": 12,
  "to_generation": 13,
  "complete": true,
  "invalidated_hashes": ["hash-a", "hash-b"]
}
```

Agent 只需要把本地命中的 hash 标记为 `missing` 或 `unknown`，其他 confirmed chunk 可以继续信任，并把本地已见 generation 推进到 `to_generation`。

Heartbeat 响应可以返回 `invalidation_available_from`，表示服务端仍能完整提供从哪个 generation 之后的 invalidation journal。Agent 如果发现自己的本地水位早于该值，可以直接跳过 journal 请求并进入批量 check 兜底路径。

如果日志已经过期、失效 hash 数量超过响应限制，或服务端无法精确表达变化，返回：

```json
{
  "repository_id": "repo-a",
  "from_generation": 12,
  "to_generation": 13,
  "complete": false,
  "reason": "journal_compacted"
}
```

`complete=false` 时，Agent 回退到 `POST /agent/v1/chunks/check`，但仍只检查本次备份 manifest 会引用的 stale chunk，不全量检查本地 catalog。

如果 compact 只改写物理位置、没有让任何 chunk hash 失效，可以递增 `chunk_generation` 但写入空 invalidation 集合。Agent 拿到 `complete=true` 且 `invalidated_hashes=[]` 后，可以直接推进本地 generation，不需要批量 check。

### 8.4 config_generation

`config_generation` 在 Agent host、Agent job、schedule、root、排除规则、限速、并发策略等配置变化时递增。

Agent heartbeat 发现该值变化后，应刷新本地缓存的 job/policy。

### 8.5 command_generation

`command_generation` 在服务端新增或更新 Agent 命令时递增。

Agent 不依赖长连接。每次 heartbeat/poll 带上本地已见 `command_generation`，服务端返回新命令或待确认命令。

## 9. Generation 工作流程

### 9.1 正常路径

1. Agent heartbeat：

```json
{
  "hostname": "nas",
  "version": "0.1.0",
  "repository_id": "repo-a",
  "chunk_generation": 12,
  "config_generation": 8,
  "command_generation": 31,
  "running_run_id": 120
}
```

2. Server 返回当前状态：

```json
{
  "status": "accepted",
  "server_time": "2026-06-21T10:00:00Z",
  "repository": {
    "id": "repo-a",
    "chunk_generation": 12,
    "invalidation_available_from": 1
  },
  "agent": {
    "config_generation": 8,
    "command_generation": 31,
    "poll_interval_seconds": 600
  },
  "maintenance": {
    "write_available": true
  }
}
```

3. 如果 `repository.id` 和 generation 均未变化，Agent 可以信任 `server_chunks.status = confirmed` 的本地记录。
4. 未变化文件直接复用本地 `file_chunks`。
5. 已确认 chunk 不请求服务端；未知 chunk 才上传或批量检查。
6. Manifest 提交后，服务端 canonicalize 所有 chunk hash，并发布 snapshot。
7. Agent 将本次 manifest 中所有 chunk 标记为 `confirmed_generation = 12`。

### 9.2 服务端维护后

1. 服务端执行 compact 或 cleanup，删除了 orphan chunk。
2. 服务端把 `chunk_generation` 从 12 增加到 13，并写入本次失效 hash 的 invalidation journal。
3. Agent 下一次 heartbeat 发现服务端 generation 为 13。
4. Agent 先请求 invalidation journal：

```http
GET /agent/v1/chunks/invalidations?since=12
```

5. 如果返回 `complete=true`，Agent 只把 `invalidated_hashes` 命中的本地 chunk 标记为 `missing` 或 `unknown`，然后把本地已见 `chunk_generation` 推进到 13。没有出现在 invalidation 列表里的 confirmed chunk 可以继续信任。
6. 如果返回 `complete=false`，Agent 不全量校验本地所有 chunk，只把 `confirmed_generation < 13` 视为 stale。
7. 下一次备份构建 manifest 前，Agent 收集本次会引用的 stale chunk，调用批量检查：

```http
POST /agent/v1/chunks/check
Content-Type: application/json

{
  "repository_id": "repo-a",
  "base_chunk_generation": 12,
  "hashes": ["..."]
}
```

8. Server 返回：

```json
{
  "repository_id": "repo-a",
  "chunk_generation": 13,
  "exists": ["hash-a", "hash-b"],
  "missing": ["hash-c"]
}
```

9. Agent 把存在的 chunk 更新为 `confirmed_generation = 13`。
10. 对缺失 chunk，Agent 根据 `file_chunks` 找到对应文件，重新读取源文件并补传。
11. 如果源文件元数据已经变化，则丢弃该文件旧 chunk 列表，重新扫描该文件。
12. 所有缺失 chunk 补齐后，Agent 再提交 manifest。

### 9.3 Manifest 提交时发现缺失

即使 Agent 已做预检查，服务端仍必须在 `POST /agent/v1/manifests` 中校验 chunk。

如果发现缺失，Server 不应发布 snapshot，而是返回结构化错误：

```json
{
  "status": "missing_chunks",
  "repository_id": "repo-a",
  "chunk_generation": 13,
  "missing_chunks": [
    {
      "hash": "hash-c",
      "paths": ["var/lib/app/data.db"]
    }
  ],
  "retryable": true
}
```

Agent 收到后：

1. 将缺失 chunk 标记为 `missing`。
2. 重新读取对应文件。
3. 补传缺失 chunk。
4. 重新提交 manifest。

补传重试需要有上限，建议默认 3 次。超过上限后本次 run 失败，避免源文件持续变化导致无限循环。

### 9.4 服务端仓库切换

如果 Server 返回新的 `repository_id`：

1. Agent 立即停止信任所有 `server_chunks`。
2. 本地文件 chunk 列表仍可作为重新上传的来源。
3. 下一次备份按 chunk hash 重新上传或批量检查。
4. 成功发布后更新本地 `repository_id` 和 generation。

## 10. 异步命令机制

服务端新增 Agent command 概念，用于手动运行和后续扩展。

命令类型首版包括：

```text
run-backup
refresh-config
cancel-run
```

### 10.1 命令状态

```text
pending
claimed
running
completed
failed
dropped
expired
```

命令必须有 `expires_at`。过期命令不再下发。默认命令 TTL 应显著长于轮询间隔，建议至少 30 分钟，避免 10 分钟轮询、jitter、短暂网络失败叠加时误过期。

### 10.2 领取流程

1. Web 用户点击 Agent job 的手动运行。
2. Server 创建 `agent_commands` 记录，状态 `pending`，payload 带上当前保存的 roots，递增 `command_generation`。
3. Agent heartbeat/poll 获取命令。
4. 如果 Agent 空闲，优先使用 command payload 的 roots 调用 create run 并携带 `command_id`，Server 原子地把命令标记为 `claimed/running`。
5. 如果 Agent 忙碌，Agent 调用 ack 接口把命令标记为 `dropped`，原因 `agent_busy`。
6. run 完成后，Agent 或 Server 把命令状态更新为 `completed/failed`。

### 10.3 API 草案

Heartbeat/poll：

```http
POST /agent/v1/heartbeat
```

响应中增加：

```json
{
  "commands": [
    {
      "id": 42,
      "type": "run-backup",
      "job_id": 7,
      "payload": {
        "job_id": 7,
        "roots": ["/backup/source"]
      },
      "created_at": "2026-06-21T10:00:00Z",
      "expires_at": "2026-06-21T10:30:00Z"
    }
  ]
}
```

命令确认：

```http
POST /agent/v1/commands/{id}/ack
Content-Type: application/json

{
  "status": "dropped",
  "reason": "agent_busy"
}
```

创建 run：

```http
POST /agent/v1/runs
Content-Type: application/json

{
  "command_id": 42,
  "trigger": "manual",
  "roots": ["/backup/source"],
  "repository_id": "repo-a",
  "base_chunk_generation": 13
}
```

## 11. 备份流程

### 11.1 启动 run

Agent 在真正扫描前创建服务端 run。这样服务端维护任务能看到 active run，并避免 compact 与备份交叉。

流程：

1. Agent 判断可以开始备份。
2. 调用 `POST /agent/v1/runs`。
3. Server 创建或返回该 Agent job 的 active run。
4. Agent 获得 run id 后开始扫描。

### 11.2 扫描与上传

每个文件：

1. `lstat` 读取元数据。
2. 检查伪文件系统和排除规则。
3. 如果本地 catalog 判断未变化，复用 `file_chunks`。
4. 如果变化，读取文件并使用内容定义分块。
5. 对每个 chunk：
   - 本地 `server_chunks` 在当前 `chunk_generation` 已 confirmed：直接引用。
   - 本地没有记录或 generation 过旧：加入批量 check。
   - check 缺失或未知：上传 chunk。
6. 更新进度。

### 11.3 提交 manifest

Agent 提交 manifest 时携带：

- `run_id`
- `repository_id`
- `base_chunk_generation`
- 文件树 manifest
- repair attempt 计数

服务端：

1. 校验 run 属于当前 Agent host。
2. 校验 manifest entries。
3. 按 hash 查询服务端 chunk index。
4. 改写为权威 `ChunkRef`。
5. 所有 chunk 存在时写 manifest 并发布 snapshot。
6. 缺失时返回 `missing_chunks`，不发布 snapshot。

### 11.4 自动补传

Agent 收到 `missing_chunks` 后：

1. 根据 hash 找到引用该 chunk 的文件路径。
2. 重新读取文件并重新分块。
3. 如果文件元数据和扫描时一致，只上传缺失 chunk。
4. 如果文件已经变化，重新扫描该文件并更新 manifest。
5. 重试提交 manifest。

默认最多重试 3 次。

## 12. 服务端数据模型补充

新增 `repository_meta`：

```text
key TEXT PRIMARY KEY
value TEXT NOT NULL
updated_at DATETIME NOT NULL
```

保存 `repository_id`、`chunk_generation` 等。

新增 `chunk_invalidations`：

```text
generation INTEGER NOT NULL
hash TEXT NOT NULL
reason TEXT NOT NULL
created_at DATETIME NOT NULL
PRIMARY KEY (generation, hash)
```

服务端维护该表的保留窗口，例如保留最近 30 天或最近 N 个 generation。超过保留窗口后，`GET /agent/v1/chunks/invalidations?since=...` 返回 `complete=false`，Agent 回退到批量 check。

新增 `agent_commands`：

```text
id INTEGER PRIMARY KEY
host_id INTEGER NOT NULL
job_id INTEGER
type TEXT NOT NULL
status TEXT NOT NULL
payload TEXT NOT NULL DEFAULT '{}'
reason TEXT
created_by TEXT
created_at DATETIME NOT NULL
updated_at DATETIME NOT NULL
expires_at DATETIME NOT NULL
claimed_at DATETIME
finished_at DATETIME
```

新增或扩展 `agent_heartbeats` 字段：

```text
mode TEXT                    -- once | daemon
state_dir TEXT
catalog_status TEXT
repository_id TEXT
chunk_generation INTEGER
config_generation INTEGER
command_generation INTEGER
running_run_id INTEGER
last_error TEXT
```

## 13. 客户端接入页面

Host 详情或 Agent 接入页需要适配 daemon 模式。

页面应展示：

- Client ID / Secret。
- Server URL。
- 推荐 Docker Compose 片段。
- 必须持久化的 Agent state 目录。
- 被备份目录的只读挂载示例。
- Daemon 状态：在线、离线、运行中、最近心跳、最近错误。
- Catalog 状态：repository id、chunk generation、最近同步时间、本地 DB 是否可用。
- 当前运行任务和最近一次 dropped 原因。

推荐部署片段应包含：

```yaml
services:
  turbk-agent:
    image: ghcr.io/tursom/turbk-agent:latest
    restart: unless-stopped
    user: "0:0"
    environment:
      TURBK_SERVER_URL: "https://backup.example.com"
      TURBK_AGENT_ID: "agt_..."
      TURBK_AGENT_SECRET: "ags_..."
      TURBK_AGENT_DAEMON: "true"
      TURBK_AGENT_STATE_DIR: "/var/lib/turbk-agent"
      TURBK_AGENT_ROOT: "/backup/source"
    volumes:
      - ./agent-state:/var/lib/turbk-agent
      - /path/to/source:/backup/source:ro
```

如果服务端统一管理 root/schedule，页面仍应提醒用户：Docker bind mount 的宿主机路径必须由客户端配置，容器内 root 才能由服务端策略引用。

## 14. 配置建议

Agent 配置：

```yaml
agent:
  mode: "daemon"
  state_dir: "/var/lib/turbk-agent"
  poll_interval: "10m"
  poll_jitter: "1m"
  backup_schedule: "0 0 * * *"
  max_concurrent_runs: 1
  busy_command_policy: "drop"
  repair_missing_chunks: true
  max_manifest_repair_attempts: 3
  metadata_reuse: true
  skip_pseudo_fs: true
```

服务端配置：

```yaml
agent:
  command_ttl: "30m"
  default_poll_interval: "10m"
  max_chunk_check_batch: 10000
  max_invalidation_response_hashes: 100000
  invalidation_retention_days: 30
```

## 15. 测试计划

单元测试：

- 本地 catalog schema migration。
- 文件元数据未变化时复用 `file_chunks`。
- `repository_id` 变化时 server chunk 状态失效。
- `chunk_generation` 变化且 invalidation journal 完整时，只标记失效 hash。
- invalidation journal 不完整时，只校验本次引用 chunk。
- 忙碌状态下手动命令标记为 dropped。

集成测试：

- Daemon heartbeat/poll 获取配置和命令。
- Web 手动运行创建 command，Agent 轮询后执行。
- Agent 忙碌时第二个 command 被 dropped。
- 服务端 cleanup 删除 orphan chunk 后写入 invalidation journal，Agent 精确标记失效 chunk 并补传。
- invalidation journal 过期后，Agent 回退到批量 check 并补传。
- Manifest 提交返回 missing chunks 后 Agent 自动补传并重试成功。
- Docker agent 使用持久 state 目录重启后复用 catalog。

端到端测试：

- 首次备份写入 catalog。
- 第二次无变化备份不重新上传已确认 chunk，并尽量不读取未变化文件。
- 修改少量文件后只读取变化文件。
- compact 后下一次备份能发现 generation 变化并完成校验。
- compact 只改写物理位置且 invalidation 为空时，Agent 不做不必要的批量 check。
- 离线一段时间后重新上线，不补跑多个积压调度，只执行一次最新备份机会。

## 16. 分阶段实现

### Phase 1：协议与服务端状态

- 增加 `repository_id`、`chunk_generation`、`config_generation`、`command_generation`。
- 扩展 heartbeat 响应。
- 增加 chunk invalidation journal API。
- 增加 chunk 批量 check API。
- manifest 缺失 chunk 返回结构化错误。

### Phase 2：Agent Daemon 和本地 Catalog

- 增加 daemon 主循环。
- 增加本地 SQLite catalog。
- 实现文件级元数据复用。
- 实现 chunk positive cache。
- 实现单任务互斥和 busy drop。

### Phase 3：异步命令

- 增加 `agent_commands`。
- Web 手动运行 Agent job 改为创建 command。
- Agent 领取、ack、执行、完成命令。

### Phase 4：自动补传和修复

- 实现 generation 变化后的批量校验。
- 实现 generation 变化后的 invalidation journal 优先同步。
- 实现 manifest missing chunks 自动补传。
- 实现 repository_id 变化后的 catalog 降级。

### Phase 5：前端和部署

- 更新 Host/Agent 接入页面。
- 更新 agent Docker Compose 示例。
- 展示 catalog、generation、命令和 dropped 状态。
