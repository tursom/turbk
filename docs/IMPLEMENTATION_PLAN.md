# Turbk 备份服务器实现计划

## 1. 目标

Turbk 是一个单机部署优先的备份服务器，面向 Linux 服务器和自托管环境。首版目标是提供可靠的增量备份、对主机管理 SMR 磁盘友好的顺序写入仓库、定时备份任务，以及可通过浏览器管理的前端界面。

系统需要同时支持两类备份连接模式：

- 服务端主动拉取被备份主机数据：SFTP、FTP/FTPS、WebDAV。
- 被备份主机安装 Agent，主动连接备份服务器：使用 HTTP/HTTPS 兼容协议，便于穿透代理、网关和各类反向代理服务。

首版不追求多节点、高可用、对象存储后端、客户端零信任加密和 Agent 回写恢复；这些能力保留为后续版本演进方向。

## 2. 技术栈与部署形态

### 2.1 技术栈

- 后端：Go。
- 前端：Vue 3 + TypeScript + Vite。
- 服务端入口：`cmd/turbk`。
- Agent 入口：`cmd/turbk-agent`。
- Web UI：`web/`，构建后嵌入服务端二进制或作为静态文件由服务端托管。

### 2.2 部署形态

首版按单机 Docker 部署设计。镜像运行时使用 Alpine，容器默认以 `root` 用户运行，便于挂载宿主机上的备份源目录、仓库盘和恢复目录。

运行时目录拆分为：

- `state_dir`：保存数据库、索引、任务状态、会话、临时文件等随机写数据。
- `repo_dir`：保存备份仓库数据，主要由 append-only segment 文件组成，适合挂载在 SMR 磁盘上。

推荐 Docker 挂载两个持久卷：

- `/var/lib/turbk/state`
- `/var/lib/turbk/repo`

## 3. 核心模块

### 3.1 Server

Server 负责：

- 用户登录和管理后台 API。
- 主机、凭据、备份任务、运行记录、快照、恢复任务管理。
- 定时调度和任务执行。
- Pull connector 执行服务端拉取备份。
- Agent HTTP API 接收主动上传。
- 仓库写入、快照提交、保留策略和维护任务。

### 3.2 Agent

Agent 首版 Linux 优先，负责：

- 根据服务端下发或本地配置的任务扫描文件系统。
- 计算文件元数据、chunk hash 和 manifest。
- 查询服务端已有 chunk，跳过已存在数据。
- 通过 HTTP/HTTPS 上传缺失 chunk。
- 提交快照 manifest。
- 上报心跳、进度和错误日志。

Agent 协议必须保持普通 HTTP 兼容，避免依赖 SSH，方便使用正向代理、反向代理、VPN、内网穿透和企业网关。

### 3.3 Web UI

首版页面：

- Dashboard：全局状态、最近任务、失败任务、仓库容量。
- Hosts：主机列表、Agent 注册状态、最近心跳。
- Credentials：SFTP、FTP、FTPS、WebDAV 等 Pull 凭据管理。
- Jobs：备份任务创建、编辑、启停、手动运行。
- Runs：任务运行记录、进度、日志、错误。
- Snapshots：快照浏览、文件下载、目录打包下载。
- Restore：恢复到服务端允许目录。
- Storage：仓库健康、segment 状态、维护任务。
- Settings：系统配置、管理员账号、保留策略默认值。

## 4. 数据仓库设计

### 4.1 设计原则

- 备份数据目录以顺序追加写为主，避免频繁随机改写。
- 元数据和索引允许放在 `state_dir`，不要求对 SMR 友好。
- 备份数据 chunk 以内容寻址，支持跨快照、跨任务去重。
- 快照提交必须原子化：未完成 run 不应产生可见快照。
- 数据段不可原地修改；删除快照只更新元数据，物理回收由维护任务完成。

### 4.2 Segment

备份数据写入 append-only segment 文件。

默认参数：

- segment 大小：512 MiB。
- chunk 平均大小：1 MiB。
- chunk hash：BLAKE3-256。
- 压缩：zstd。
- 加密：AES-256-GCM，服务端持有密钥。

segment record 包含：

- record magic/version。
- chunk hash。
- 原始大小。
- 压缩后大小。
- 加密 nonce。
- 加密 payload。
- record 校验信息。

segment 文件写满后切换到新文件；已关闭 segment 不再追加。

### 4.3 元数据与索引

建议使用：

- SQLite：保存稳定业务元数据，例如 host、credential、job、run、snapshot、file entry、restore task、app settings。
- Pebble：保存高频 chunk 索引，例如 `chunk_hash -> segment_id, offset, length, ref_state`。

SQLite 与 Pebble 均放在 `state_dir`，避免污染 SMR 友好的 `repo_dir`。

### 4.4 快照

每次成功备份生成一个 snapshot。

snapshot 记录：

- 所属 host/job。
- 创建时间。
- 备份源类型。
- 文件树 manifest。
- 每个文件的元数据、大小、mtime、mode、uid/gid、符号链接信息。
- 文件内容对应的 chunk 引用列表。

Linux 首版需要正确处理：

- 普通文件。
- 目录。
- 符号链接。
- 权限 mode。
- uid/gid。
- mtime。

硬链接可以首版记录 inode/dev 信息，但恢复时是否重建硬链接可作为后续增强。

## 5. 增量备份策略

### 5.1 文件级增量判断

扫描时优先使用文件元数据判断是否需要重新分块：

- path。
- size。
- mtime。
- mode。
- inode/dev，Agent 本地场景可用。

若文件元数据与上一快照一致，可直接复用上一快照的 chunk 引用。

### 5.2 Chunk 级去重

当文件需要重新读取时：

- 使用内容定义切分生成 chunk。
- 计算 chunk hash。
- 先查询服务端 chunk index。
- 已存在 chunk 只引用，不重复上传或写入。
- 不存在 chunk 才压缩、加密并追加写入 segment。

### 5.3 Run 原子性

一次备份 run 的状态包括：

- pending。
- running。
- failed。
- canceled。
- completed。

只有 completed run 才能发布 snapshot。failed/canceled run 上传过的 chunk 可以保留为孤儿 chunk，后续维护任务回收或被未来快照引用。

## 6. 连接模式

### 6.1 Pull Connector

服务端主动拉取模式通过统一接口适配不同协议。

统一能力：

- `Walk(root)`：遍历目录树。
- `Stat(path)`：读取元数据。
- `Open(path)`：打开文件流。
- `Read/Close`：读取和关闭。

首版 connector：

- SFTP：通过 SSH 凭据连接，不执行远端 shell。
- FTP/FTPS：支持明文 FTP、隐式 TLS、显式 TLS；自托管自签证书场景可通过凭据 payload 的 `skip_tls_verify` 显式开启跳过证书校验。
- WebDAV：支持基本认证和 Bearer token。

Pull 模式的凭据由服务端加密保存。

### 6.2 Agent Push

Agent 使用 HTTP/HTTPS 兼容协议主动连接 Server。

核心流程：

1. Agent 使用 Client ID / Secret 认证后创建 backup run。
2. Agent 扫描本地文件树。
3. Agent 查询服务端已有 chunk。
4. Agent 上传缺失 chunk。
5. Agent 提交 manifest。
6. Server 校验并发布 snapshot。

Agent API 需要满足：

- 请求可幂等重试。
- chunk 上传可重复提交。
- run id 可恢复。
- 支持代理和标准 HTTP 超时。
- 服务端能拒绝越权 host/job。

## 7. 安全模型

首版采用服务端加密模型。

- 传输层使用 HTTPS。
- 仓库 segment payload 使用 AES-256-GCM 加密。
- 服务端保存 master key 或 key-encryption-key。
- 凭据、Agent Client Secret、连接密钥在数据库中加密存储。
- Web 用户使用登录会话。
- Agent 使用独立 Client ID / Secret，不复用 Web 用户会话。

首版管理后台可浏览文件名和快照内容；不实现服务端不可见数据的零信任模型。

## 8. 定时任务与保留策略

### 8.1 定时任务

每个 job 支持：

- cron 表达式。
- 时区。
- 启停状态。
- 最大运行时长。
- 失败重试次数。
- 并发锁，同一 job 同一时间只允许一个 active run。
- 手动触发。

### 8.2 保留策略

默认保留策略：

- 最近 30 个成功快照。
- 每日保留 30 个。
- 每周保留 12 个。

快照过期只删除快照引用和元数据，不立即改写 segment。

### 8.3 仓库维护

维护任务包括：

- orphan chunk 扫描。
- 低利用率 segment 统计。
- segment rewrite/compact。
- 仓库校验。
- 容量报告。

compact 会产生顺序读和顺序写，必须作为维护窗口任务运行，不能混入常规备份主流程。

## 9. 恢复功能

首版恢复能力：

- Web 浏览 snapshot 文件树。
- 下载单个文件。
- 下载目录 tar.gz。
- 恢复到备份服务器本地允许目录。

服务端本地恢复必须限制在配置的 restore root 内，防止任意路径写入。

首版不实现 Agent 回写到原主机；该能力作为后续版本。

## 10. API 草案

### 10.1 管理 API

- `POST /api/v1/auth/login`
- `POST /api/v1/auth/logout`
- `GET /api/v1/auth/session`
- `GET /api/v1/hosts`
- `POST /api/v1/hosts`
- `GET /api/v1/credentials`
- `POST /api/v1/credentials`
- `GET /api/v1/jobs`
- `POST /api/v1/jobs`
- `PATCH /api/v1/jobs/:id`
- `POST /api/v1/jobs/:id/run`
- `GET /api/v1/runs`
- `GET /api/v1/runs/:id/logs`
- `GET /api/v1/snapshots`
- `DELETE /api/v1/snapshots/:id`
- `POST /api/v1/snapshots/delete`
- `GET /api/v1/snapshots/:id/tree`
- `GET /api/v1/snapshots/:id/files/*path`
- `POST /api/v1/restore`
- `GET /api/v1/restore/tasks`
- `GET /api/v1/storage/health`
- `GET /api/v1/storage/maintenance/runs`
- `POST /api/v1/storage/maintenance`

### 10.2 Agent API

- `POST /agent/v1/runs`
- `GET /agent/v1/chunks/:hash`
- `PUT /agent/v1/chunks/:hash`
- `POST /agent/v1/manifests`
- `POST /agent/v1/heartbeat`
- `POST /agent/v1/runs/:id/progress`
- `POST /agent/v1/runs/:id/finish`

## 11. 实现阶段

### Phase 1：项目骨架

- 初始化 Go module。
- 添加 `cmd/turbk` 和 `cmd/turbk-agent`。
- 添加基础配置加载。
- 添加 SQLite 初始化。
- 添加 Vue 管理后台骨架。
- Dockerfile 和 docker-compose 示例。

验收：

- 服务端可启动。
- Web UI 可访问。
- 健康检查 API 可返回状态。

### Phase 2：仓库内核

- 实现 chunker。
- 实现 segment writer/reader。
- 实现压缩、加密、校验。
- 实现 chunk index。
- 实现 snapshot manifest 写入和读取。

验收：

- 可以写入文件树并生成 snapshot。
- 可以从 snapshot 还原单文件。
- 重复内容不会重复写入 chunk。

### Phase 3：本地和 Pull 备份

- 实现本地文件源，便于测试。
- 实现 SFTP connector。
- 实现 FTP/FTPS connector。
- 实现 WebDAV connector。
- 实现 job/run 状态机。

验收：

- 可通过管理 API 创建 Pull job。
- 可手动触发备份。
- 第二次备份只写入变化数据。

### Phase 4：Agent Push

- 实现 Agent host 创建和服务端分配的 Client ID / Secret。
- 实现 Agent HTTP 协议。
- 实现 Agent 本地扫描和 chunk 上传。
- 实现断点重试和幂等提交。

当前首版实现边界：

- 创建 `agent` host 时服务端生成并绑定 `agent` credential；普通 Credentials API 不创建或展示 Agent credential。
- Agent API 使用 HTTP Basic Auth 携带 Client ID / Secret，服务端通过 `agent_credentials.client_id` 快速定位对应 host。
- Agent 创建 run 时按绑定 host 自动创建或复用唯一的 `agent` job；客户端不提交 job id/name，同一 job 已有 pending/running run 时返回原 run。
- chunk 查询和上传按 BLAKE3-256 内容 hash 幂等处理；重复上传不会再次追加写入 segment。
- manifest 提交由服务端校验 chunk 存在并改写为仓库内权威 `ChunkRef`，重复提交同一 run 不会产生重复 snapshot。
- Go HTTP 默认 transport 支持 `HTTP_PROXY`、`HTTPS_PROXY`、`NO_PROXY`，Agent 不需要专用代理协议。

验收：

- Agent 可通过 HTTP/HTTPS 连接服务端。
- Agent 可经过 HTTP 代理上传备份。
- 断开后重试不会产生重复快照或重复 chunk。

### Phase 5：调度、保留和维护

- 实现 cron 调度。
- 实现 job 并发锁。
- 实现保留策略。
- 实现仓库 health 和 maintenance。

当前首版实现边界：

- 服务端内置调度器每 30 秒扫描 enabled 且配置 schedule 的非 Agent job。
- schedule 支持 5 字段 cron、`@hourly`、`@daily`/`@midnight`、`@weekly`；调度按 job timezone 计算。
- 非 Agent job 支持 `max_runtime_seconds` 和 `retry_attempts`；两者为 0 时分别表示不限制运行时长、不自动重试。
- 失败重试限制在同一个 cron 命中窗口内，已完成或仍有 active run 时不会重复触发。
- 同一 job 的 active run 由状态层锁定；调度器额外受 `scheduler.max_concurrent_runs` 全局并发限制。
- 保留策略通过 `deleted_at` 软删除过期 snapshot，不改写 segment。
- `DELETE /api/v1/snapshots/:id` 和 `POST /api/v1/snapshots/delete` 支持手动软删除 snapshot；删除后恢复入口不再返回该 snapshot，物理空间由后续 compact 回收。
- `POST /api/v1/storage/maintenance` 支持 `retention`、`verify`、`cleanup-errors`、`compact` 和 `full-cleanup`；retention 返回 segment 利用率、active/deleted snapshot 数量和 orphan chunk 估算，verify 只读校验 active manifest 引用的 chunk index 和 segment record，cleanup-errors 清理 stale run、orphan manifest 和 orphan chunk index，compact/full-cleanup 在无 active run 时顺序重写 active chunk、更新 manifest/index 并删除不再引用的旧 segment。
- compact 与常规备份写入互斥：手动 run、定时 run、Agent run 创建和 Agent chunk 上传进入备份写入 gate；compact 进入维护 gate，发现已有备份写入或 active run 时跳过，避免维护窗口内混入新 segment 写入。
- 自动维护默认启用：每天执行 retention + cleanup-errors，每周执行 compact；自动 compact 会在预计回收收益低于阈值时跳过并记录维护历史。

验收：

- 定时任务按预期触发。
- 过期快照被标记删除。
- 维护任务可报告 segment 利用率。

### Phase 6：Web 管理界面

- 实现主机、凭据、任务、运行记录、快照、恢复、存储状态页面。
- 实现日志查看和进度展示。
- 实现快照树浏览、文件下载、目录打包下载。

当前首版实现边界：

- Snapshots 页面可打开 snapshot 树，按目录逐级浏览。
- 单文件下载直接流式读取 chunk；目录下载返回 `tar.gz`。
- Restore 页面可选择 snapshot/path/target 发起服务端本地恢复。
- `POST /api/v1/restore` 同步执行恢复任务并记录 restore task；target 必须落在配置的 restore root 内。
- 目录恢复会按 manifest 重建目录、普通文件和符号链接；不重建硬链接。
- Settings 页面可修改管理员用户名、管理员密码、会话 TTL 和默认保留策略；运行时配置写入 SQLite 并立即影响后续登录和维护任务。

验收：

- 常见备份、查看、恢复流程可全部在 Web UI 完成。

## 12. 测试计划

### 12.1 单元测试

- chunk 切分稳定性。
- chunk hash 与去重。
- segment append 和读取。
- 加密解密。
- record 校验失败。
- snapshot manifest 序列化和反序列化。
- retention policy 计算。

### 12.2 集成测试

- 本地文件源全量和增量备份。
- SFTP 测试服务备份。
- FTP/FTPS 测试服务备份。
- WebDAV 测试服务备份。
- Agent HTTP 上传。
- Agent 通过代理上传。
- 任务中断后恢复。
- compact 与备份写入互斥，维护窗口内不启动新 run。

当前测试覆盖：

- SFTP、FTP、显式 FTPS、WebDAV connector 均有进程内协议服务测试，覆盖真实网络握手、目录遍历和文件读取。
- WebDAV Pull job 有管理 API 端到端测试，覆盖凭据创建、任务创建、手动运行、snapshot 发布和文件下载。
- Agent HTTP 代理兼容有本地 HTTP proxy 测试覆盖。
- compact 与备份写入互斥有 HTTP/API 和调度路径测试覆盖。
- SMR 友好性有 repository 测试覆盖：常规写入后 `repo_dir` 只包含 segment 文件，Pebble/index/key/manifest 状态留在 `state_dir`，已关闭 segment 在后续常规写入中大小不变。

### 12.3 端到端测试

- 首次全量备份。
- 修改少量文件后的增量备份。
- 删除文件后的快照表现。
- 恢复文件和目录后做 hash 对比。
- 模拟 chunk 上传重复、run 重复提交、服务端重启。

### 12.4 SMR 友好性验证

- 常规备份期间，`repo_dir` 只追加写 segment。
- 已关闭 segment 不被原地改写。
- SQLite/Pebble 等随机写只发生在 `state_dir`。
- compact 仅在维护任务中执行，并以顺序读写为主。

## 13. 首版边界

首版包含：

- Go Server。
- Linux Agent。
- Vue 管理界面。
- SFTP、FTP/FTPS、WebDAV Pull。
- HTTP/HTTPS Agent Push。
- 增量备份。
- 顺序写 segment 仓库。
- 服务端加密。
- 定时任务。
- 快照浏览、下载、服务端本地恢复。

首版不包含：

- 多节点服务端。
- 对象存储仓库后端。
- Windows/macOS Agent 完整支持。
- Agent 回写恢复。
- 客户端零信任加密。
- rsync over SSH。
- 远端 shell 执行。
- 跨仓库复制和异地同步。

## 14. 默认配置建议

```yaml
server:
  listen: ":8080"
  public_url: "https://backup.example.com"
  web_dir: "/app/web/dist"

auth:
  username: "admin"
  password: "change-me"
  session_ttl_hours: 24

paths:
  state_dir: "/var/lib/turbk/state"
  repo_dir: "/var/lib/turbk/repo"
  restore_roots:
    - "/var/lib/turbk/restore"

repository:
  segment_size: "512MiB"
  chunk_avg_size: "1MiB"
  compression: "zstd"
  encryption: "aes-256-gcm"

scheduler:
  timezone: "Asia/Shanghai"
  max_concurrent_runs: 2

retention:
  keep_last: 30
  keep_daily: 30
  keep_weekly: 12
```
