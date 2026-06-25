# Turbk

Turbk 是一个单机部署优先的备份服务器。当前仓库已落地 Go Server、Linux Agent、SQLite 状态库、SMR 友好的 append-only 仓库、Pull connector 基础能力和 Vue 管理后台。

仓库地址：`https://github.com/tursom/turbk`
Go module：`github.com/tursom/turbk`

## 本地运行

安装前端依赖并构建 Web UI：

```bash
npm --prefix web install
npm --prefix web run build
```

启动服务端：

```bash
go run ./cmd/turbk -config configs/turbk.example.yaml
```

访问：

- Web UI: `http://localhost:8080/`
- Health API: `http://localhost:8080/api/v1/health`
- Storage API: `http://localhost:8080/api/v1/storage/health`

管理后台初始账号统一为 `admin` / `admin`。生产部署首次登录后应在 Settings 页面或通过 API 修改管理员账号和密码，不需要额外的一次性初始化口令。命令行访问管理 API 时先登录保存 Cookie：

```bash
curl -c /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  --data '{"username":"admin","password":"admin"}'
```

管理员账号、会话 TTL 和默认保留策略也可以在 Settings 页面修改，或通过 API 持久化到 SQLite：

```bash
curl -b /tmp/turbk.cookie -X PATCH http://localhost:8080/api/v1/settings \
  -H 'Content-Type: application/json' \
  --data '{"admin_username":"operator","current_password":"admin","admin_password":"new-admin-secret","session_ttl_hours":12,"retention":{"keep_last":30,"keep_daily":30,"keep_weekly":12}}'
```

下面示例里的 `credential_id`、`host_id` 需要替换为上一步 API 返回的实际 ID，数字只用于展示字段位置。

创建一个 Pull 凭据和主机记录。凭据只保存认证材料，主机保存上游地址并绑定凭据：

```bash
curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/credentials \
  -H 'Content-Type: application/json' \
  --data '{"name":"sftp-prod-login","type":"sftp","payload":{"username":"root","password":"secret"}}'

curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/hosts \
  -H 'Content-Type: application/json' \
  --data '{"name":"sftp-prod","source_type":"sftp","address":"10.0.0.10:22","credential_id":1}'
```

创建 Agent 主机。服务端会同时生成 Client ID 和 Secret；客户端凭据不出现在 Credentials 页面，只在对应 Host 详情中管理：

```bash
curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/hosts \
  -H 'Content-Type: application/json' \
  --data '{"name":"dev-host","source_type":"agent"}'
```

Agent 心跳 smoke test：

```bash
go run ./cmd/turbk-agent \
  -server http://localhost:8080 \
  -client-id "$TURBK_AGENT_ID" \
  -client-secret "$TURBK_AGENT_SECRET" \
  -once
```

Agent 心跳会更新对应 Host 的状态、最后心跳时间和 `address`，其中 `address` 使用 agent 上报的 hostname。

Agent Push 备份：

```bash
go run ./cmd/turbk-agent \
  -server http://localhost:8080 \
  -client-id "$TURBK_AGENT_ID" \
  -client-secret "$TURBK_AGENT_SECRET" \
  -root /data/source \
  -once
```

Agent daemon 常驻模式：

```bash
go run ./cmd/turbk-agent \
  -server http://localhost:8080 \
  -client-id "$TURBK_AGENT_ID" \
  -client-secret "$TURBK_AGENT_SECRET" \
  -root /data/source \
  -state-dir /var/lib/turbk-agent \
  -daemon
```

Agent 默认启用 `-skip-pseudo-fs=true`，会跳过 procfs、sysfs、cgroup 等 Linux 伪文件系统，避免读取 `/proc/*/attr/*` 这类伪文件导致备份失败。需要额外排除路径时可用 `-exclude` 或 `TURBK_AGENT_EXCLUDES`，规则相对备份根目录，多个规则用逗号或换行分隔。

Agent 读取普通文件时会校验读取过程中的大小和元数据是否稳定。若文件在读取期间发生变化，agent 会有限重试；多次重试后仍持续变化的文件会被跳过，以避免提交文件大小与 chunk 字节数不一致的 manifest。这个机制只保证备份集结构一致，不保证数据库等多文件/页级更新应用的一致性；数据库请优先使用数据库原生备份、逻辑导出，或配合 LVM/ZFS/Btrfs/云盘快照等一致性快照，并按需通过 `-exclude` 排除在线数据文件。

创建并运行一个本地备份任务。本地来源也先创建 Host，具体备份路径放在 Job 的 `source_config.root`：

```bash
curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/hosts \
  -H 'Content-Type: application/json' \
  --data '{"name":"server-local","source_type":"local"}'

curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  --data '{"name":"local demo","host_id":2,"source_config":{"root":"/data/source"},"enabled":true}'

curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/jobs/1/run
```

重复运行同一个任务时，服务端会先按上一快照的文件元数据复用未变化文件的 chunk 引用；需要重新读取的文件再通过内容寻址索引复用已存在 chunk，不会再次追加写入 segment。

创建定时任务时可以填写 5 字段 cron 表达式：

```bash
curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  --data '{"name":"nightly local","host_id":2,"source_config":{"root":"/data/source"},"enabled":true,"schedule":"0 2 * * *","timezone":"Asia/Shanghai","max_runtime_seconds":7200,"retry_attempts":2}'
```

服务端内置调度器每 30 秒检查一次 due job，同一 job 同一时间只允许一个 active run，失败后会在同一个 cron 命中窗口内按 `retry_attempts` 补跑。`max_runtime_seconds` 为 0 表示不限制运行时长，`retry_attempts` 为 0 表示失败不自动重试。Agent job 与 Agent host 一对一绑定，仍由 `turbk-agent` 主动发起。

更新任务调度或启停状态：

```bash
curl -b /tmp/turbk.cookie -X PATCH http://localhost:8080/api/v1/jobs/1 \
  -H 'Content-Type: application/json' \
  --data '{"enabled":false,"schedule":"","timezone":"Asia/Shanghai","max_runtime_seconds":0,"retry_attempts":0}'
```

创建一个 SFTP Pull 凭据、主机和任务：

```bash
curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/credentials \
  -H 'Content-Type: application/json' \
  --data '{"name":"sftp-prod-login","type":"sftp","payload":{"username":"root","password":"secret"}}'

curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/hosts \
  -H 'Content-Type: application/json' \
  --data '{"name":"sftp-prod","source_type":"sftp","address":"10.0.0.10:22","credential_id":1}'

curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  --data '{"name":"sftp pull","host_id":1,"source_config":{"root":"/srv/data"},"enabled":true}'
```

凭据 payload 会在 SQLite 中以 AES-256-GCM 加密保存。Pull 凭据列表只返回元数据；Agent 凭据由 Host 创建流程生成，并在对应 Host 详情中展示 `client_id` 和可重复查看的 `client_secret`。

FTP/FTPS Pull 凭据使用相同的凭据 API。FTPS 可按服务端类型选择显式 TLS，并在自签证书环境中临时跳过证书校验：

```bash
curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/credentials \
  -H 'Content-Type: application/json' \
  --data '{"name":"ftps-prod-login","type":"ftps","payload":{"username":"backup","password":"secret","tls":true,"explicit_tls":true,"skip_tls_verify":true}}'

curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/hosts \
  -H 'Content-Type: application/json' \
  --data '{"name":"ftps-prod","source_type":"ftps","address":"10.0.0.20:21","credential_id":3}'
```

执行仓库维护和保留策略：

```bash
curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/storage/maintenance \
  -H 'Content-Type: application/json' \
  --data '{"mode":"retention"}'

curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/storage/maintenance \
  -H 'Content-Type: application/json' \
  --data '{"mode":"verify"}'

curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/storage/maintenance \
  -H 'Content-Type: application/json' \
  --data '{"mode":"cleanup-errors"}'

curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/storage/maintenance \
  -H 'Content-Type: application/json' \
  --data '{"mode":"compact"}'

curl -b /tmp/turbk.cookie -X DELETE http://localhost:8080/api/v1/snapshots/1
```

`retention` 模式会按配置保留策略软删除过期 snapshot，并返回 segment 利用率、活跃/已删除 snapshot 数量和 orphan chunk 估算。`verify` 模式只读校验 active manifest 引用的 chunk index 和 segment record，不执行保留删除。`cleanup-errors` 会把服务重启前遗留且超过阈值的 stale run 标记失败，并清理不被 active snapshot 引用的旧 manifest 和 chunk index。`compact` 模式会先执行 retention 和 cleanup-errors，再把 active snapshot 仍引用的 chunk 顺序重写到新 segment，更新 manifest/index，并删除不再被 active manifest 引用的旧 segment；有 pending/running run 或备份写入 gate 正忙时会跳过 compact。`full-cleanup` 可用于手动执行完整清理流程。删除 snapshot 只写 `deleted_at`，磁盘空间由后续 compact 回收。

浏览、下载和恢复 snapshot：

```bash
curl -b /tmp/turbk.cookie http://localhost:8080/api/v1/snapshots/1/tree?path=.

curl -b /tmp/turbk.cookie -OJ 'http://localhost:8080/api/v1/snapshots/1/files?path=sub/file.txt'

curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/restore \
  -H 'Content-Type: application/json' \
  --data '{"snapshot_id":1,"path":"sub","target_path":"/var/lib/turbk/restore/sub"}'

curl -b /tmp/turbk.cookie http://localhost:8080/api/v1/restore/tasks
```

目录下载会返回 `tar.gz`。服务端本地恢复只允许写入配置的 `restore_roots` 内，避免通过管理 API 写入任意路径。

查看 run 日志：

```bash
curl -b /tmp/turbk.cookie http://localhost:8080/api/v1/runs/1/logs
```

`GET /api/v1/runs` 会返回每个 run 的 `progress` 字段，包含 phase、已处理文件/字节数以及上传/复用 chunk 计数。Agent 备份通过 `POST /agent/v1/runs/{id}/progress` 上报进度；本地和 Pull 备份由服务端执行器自动更新。

## Docker

这里部署的是 Turbk 服务端。服务端镜像只包含 `turbk`，不包含 `turbk-agent`。

从源码目录本地构建并部署服务端：

```bash
cp .env.example .env
cp deploy/server/config.example.yaml config.yaml
# 修改 config.yaml 中的 server.public_url 和仓库参数；首次登录后修改默认管理员密码
# 按需修改 .env 中的 TURBK_HTTP_PORT 与宿主机挂载目录
docker compose build
docker compose up -d
```

服务端 compose 默认镜像名：

- `ghcr.io/tursom/turbk:latest`

`docker compose build` 会把本地源码构建成这个镜像名。也可以通过 `TURBK_IMAGE` 指定其他 tag，例如 `turbk:local` 或 `ghcr.io/tursom/turbk:sha-<commit>`。
如果构建时下载 Go 依赖较慢，可以在 `.env` 中把 `GOPROXY` 改成当前网络可用的 Go module 代理。

如果只想使用 GitHub Container Registry 上已经推送的镜像：

```bash
cp .env.example .env
cp deploy/server/config.example.yaml config.yaml
# 修改 config.yaml 中的 server.public_url 和仓库参数；首次登录后修改默认管理员密码
# 按需修改 .env 中的 TURBK_HTTP_PORT 与宿主机挂载目录
docker compose pull
docker compose up -d --no-build
```

GitHub Actions 会在 push 和 tag 时构建并推送服务端镜像 `ghcr.io/tursom/turbk` 和 agent 镜像 `ghcr.io/tursom/turbk-agent`；PR 只构建不推送。

服务端应用配置默认来自根目录 `config.yaml`。先从 `deploy/server/config.example.yaml` 复制一份本地配置，Compose 会把它挂载到容器内 `/etc/turbk/config.yaml`，并以 `-config /etc/turbk/config.yaml` 启动。这个 YAML 覆盖完整应用配置：

- `configs/turbk.example.yaml` 用于本地源码运行，路径默认是相对仓库目录。
- `deploy/server/config.example.yaml` 用于 Docker 服务端部署，路径默认是容器内路径。

- `server.listen`、`server.public_url`、`server.web_dir`
- `auth.username`、`auth.password`、`auth.session_ttl_hours`
- `paths.state_dir`、`paths.repo_dir`、`paths.restore_roots`
- `repository.segment_size`、`repository.chunk_avg_size`、`repository.compression`、`repository.encryption`
- `scheduler.timezone`、`scheduler.max_concurrent_runs`
- `retention.keep_last`、`retention.keep_daily`、`retention.keep_weekly`
- `maintenance.enabled`、`maintenance.cleanup_schedule`、`maintenance.compact_enabled`、`maintenance.compact_schedule`
- `maintenance.error_grace_period`、`maintenance.stale_run_after`、`maintenance.keep_deleted_metadata_days`
- `maintenance.compact_min_reclaim_ratio`、`maintenance.compact_min_reclaim_bytes`

`.env` 只保留 Compose 层参数，例如镜像 tag、宿主机端口、构建参数和宿主机目录。默认绑定挂载：

- `${TURBK_CONFIG_FILE:-./config.yaml}` -> `/etc/turbk/config.yaml`
- `${TURBK_STATE_DIR:-./data/state}` -> `/var/lib/turbk/state`
- `${TURBK_REPO_DIR:-./data/repo}` -> `/var/lib/turbk/repo`
- `${TURBK_RESTORE_DIR:-./data/restore}` -> `/var/lib/turbk/restore`

注意区分两层路径：`.env` 中的 `TURBK_STATE_DIR`、`TURBK_REPO_DIR`、`TURBK_RESTORE_DIR` 是宿主机路径；`config.yaml` 中的 `paths.*` 是容器内路径，通常保持 `/var/lib/turbk/...`。

主机管理 SMR 磁盘建议把 `repository.segment_size` 设置为 zone size。若 `lsblk` 显示 `ZONE-SZ=256M`，配置为：

```yaml
repository:
  segment_size: "256MiB"
  chunk_avg_size: "1MiB"
```

如果要让容器内的 `local` 备份任务读取宿主机目录，需要在 `docker-compose.yml` 的 `volumes` 中额外挂载源目录，例如：

```yaml
      - "/srv/data:/mnt/source:ro"
```

容器默认以 `root` 运行，便于读写宿主机挂载进来的备份源、仓库盘和恢复目录。
镜像 runtime 使用 `alpine:3.22`，构建阶段使用 `golang:1.26-alpine` 和 `node:22-alpine`。
部署后请使用默认初始账号 `admin` / `admin` 登录，并立即在 Settings 页面或通过 `PATCH /api/v1/settings` 修改管理员密码。

## Agent Docker

Agent 要部署在被备份主机上，使用独立目录：

```bash
cd deploy/agent
cp .env.example .env
# 填写 TURBK_SERVER_URL、TURBK_AGENT_ID、TURBK_AGENT_SECRET 和 TURBK_AGENT_SOURCE_DIR
docker compose pull
docker compose up -d
```

备份 Docker 数据目录时，运行中的 `overlay2/*/merged` 可能包含容器内的 `/proc`、`/sys` 等挂载视图。agent 默认跳过 Linux 伪文件系统；如需路径级排除，可在 `deploy/agent/.env` 设置 `TURBK_AGENT_EXCLUDES=overlay2/*/merged/proc/**,overlay2/*/merged/sys/**`。

也可以在被备份主机上从源码构建 agent 镜像：

```bash
cd deploy/agent
docker compose build
docker compose up -d
```

agent compose 默认镜像名是 `ghcr.io/tursom/turbk-agent:latest`。它不暴露端口，只把被备份目录以只读方式挂载进容器，然后由 `turbk-agent` 主动连接服务端。当前 compose 默认以 daemon 模式常驻运行，并把本地 catalog 持久化到 `./agent-state`。默认 catalog backend 是 `hybrid`：SQLite 保存低频状态，Pebble 保存文件记录和服务端 chunk 确认状态以减少 SQLite WAL 写入。daemon 默认每 10 分钟轮询服务端 command，本地定期备份 cron 默认 `0 0 * * *`。

Web UI 的主机详情页会为 Agent 主机生成多种接入方式：Docker Compose、Docker run、Linux 二进制和 systemd service。推荐优先从页面复制对应配置，避免手工拼写 Client ID、Secret 和服务端地址。

## 开发验证

```bash
go test ./...
npm --prefix web run build
```

后续实现阶段仍以 `IMPLEMENTATION_PLAN.md` 为边界推进。
