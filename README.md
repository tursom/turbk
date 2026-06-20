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

管理后台默认账号为 `admin` / `admin`，生产部署应通过 `TURBK_ADMIN_USERNAME`、`TURBK_ADMIN_PASSWORD` 和 `TURBK_SESSION_TTL_HOURS` 覆盖。命令行访问管理 API 时先登录保存 Cookie：

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

创建手工管理的主机记录：

```bash
curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/hosts \
  -H 'Content-Type: application/json' \
  --data '{"name":"sftp-prod","source_type":"sftp","address":"10.0.0.10:22"}'
```

创建 Agent 客户端凭据。`client_secret` 会加密保存，之后也可以在 Credentials 页面或凭据列表 API 中查看：

```bash
curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/credentials \
  -H 'Content-Type: application/json' \
  --data '{"name":"agent-dev","type":"agent","payload":{"subject":"dev-host"}}'
```

Agent 心跳 smoke test：

```bash
go run ./cmd/turbk-agent \
  -server http://localhost:8080 \
  -client-id "$TURBK_AGENT_ID" \
  -client-secret "$TURBK_AGENT_SECRET" \
  -once
```

Agent 心跳会同步更新 Hosts 页中的 `agent` 主机状态，`name` 使用凭据 subject，`address` 使用 agent 上报的 hostname。

Agent Push 备份：

```bash
go run ./cmd/turbk-agent \
  -server http://localhost:8080 \
  -client-id "$TURBK_AGENT_ID" \
  -client-secret "$TURBK_AGENT_SECRET" \
  -root /data/source \
  -job-name "agent dev /data/source" \
  -once
```

创建并运行一个本地备份任务：

```bash
curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  --data '{"name":"local demo","source_type":"local","source_config":{"root":"/data/source"},"enabled":true}'

curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/jobs/1/run
```

重复运行同一个任务时，服务端会先按上一快照的文件元数据复用未变化文件的 chunk 引用；需要重新读取的文件再通过内容寻址索引复用已存在 chunk，不会再次追加写入 segment。

创建定时任务时可以填写 5 字段 cron 表达式：

```bash
curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  --data '{"name":"nightly local","source_type":"local","source_config":{"root":"/data/source"},"enabled":true,"schedule":"0 2 * * *","timezone":"Asia/Shanghai","max_runtime_seconds":7200,"retry_attempts":2}'
```

服务端内置调度器每 30 秒检查一次 due job，同一 job 同一时间只允许一个 active run，失败后会在同一个 cron 命中窗口内按 `retry_attempts` 补跑。`max_runtime_seconds` 为 0 表示不限制运行时长，`retry_attempts` 为 0 表示失败不自动重试。Agent job 仍由 `turbk-agent` 主动发起。

更新任务调度或启停状态：

```bash
curl -b /tmp/turbk.cookie -X PATCH http://localhost:8080/api/v1/jobs/1 \
  -H 'Content-Type: application/json' \
  --data '{"enabled":false,"schedule":"","timezone":"Asia/Shanghai","max_runtime_seconds":0,"retry_attempts":0}'
```

创建一个 SFTP Pull 凭据和任务：

```bash
curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/credentials \
  -H 'Content-Type: application/json' \
  --data '{"name":"sftp-prod","type":"sftp","payload":{"address":"10.0.0.10:22","username":"root","password":"secret"}}'

curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  --data '{"name":"sftp pull","source_type":"sftp","credential_id":1,"source_config":{"root":"/srv/data"},"enabled":true}'
```

凭据 payload 会在 SQLite 中以 AES-256-GCM 加密保存。Pull 凭据列表只返回元数据；Agent 凭据会额外返回 `client_id` 和可重复查看的 `client_secret`，用于客户端配置。

FTP/FTPS Pull 凭据使用相同的凭据 API。FTPS 可按服务端类型选择显式 TLS，并在自签证书环境中临时跳过证书校验：

```bash
curl -b /tmp/turbk.cookie -X POST http://localhost:8080/api/v1/credentials \
  -H 'Content-Type: application/json' \
  --data '{"name":"ftps-prod","type":"ftps","payload":{"address":"10.0.0.20:21","username":"backup","password":"secret","tls":true,"explicit_tls":true,"skip_tls_verify":true}}'
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
  --data '{"mode":"compact"}'
```

`retention` 模式会按配置保留策略软删除过期 snapshot，并返回 segment 利用率、活跃/已删除 snapshot 数量和 orphan chunk 估算。`verify` 模式只读校验 active manifest 引用的 chunk index 和 segment record，不执行保留删除。`compact` 模式会先执行 retention，再把 active snapshot 仍引用的 chunk 顺序重写到新 segment，更新 manifest/index，并删除不再被 active manifest 引用的旧 segment；有 pending/running run 或备份写入 gate 正忙时会跳过 compact。

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
# 修改 .env 中的 TURBK_ADMIN_PASSWORD 等部署参数
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
# 修改 .env 中的 TURBK_ADMIN_PASSWORD 等部署参数
docker compose pull
docker compose up -d --no-build
```

GitHub Actions 会在 push 和 tag 时构建并推送服务端镜像 `ghcr.io/tursom/turbk` 和 agent 镜像 `ghcr.io/tursom/turbk-agent`；PR 只构建不推送。

compose 默认绑定挂载：

- `${TURBK_STATE_DIR:-./data/state}` -> `/var/lib/turbk/state`
- `${TURBK_REPO_DIR:-./data/repo}` -> `/var/lib/turbk/repo`
- `${TURBK_RESTORE_DIR:-./data/restore}` -> `/var/lib/turbk/restore`

如果要让容器内的 `local` 备份任务读取宿主机目录，需要在 `docker-compose.yml` 的 `volumes` 中额外挂载源目录，例如：

```yaml
      - "/srv/data:/mnt/source:ro"
```

容器默认以 `root` 运行，便于读写宿主机挂载进来的备份源、仓库盘和恢复目录。
镜像 runtime 使用 `alpine:3.22`，构建阶段使用 `golang:1.26-alpine` 和 `node:22-alpine`。
部署时必须覆盖 `TURBK_ADMIN_PASSWORD`，不要沿用开发默认密码。

## Agent Docker

Agent 要部署在被备份主机上，使用独立目录：

```bash
cd deploy/agent
cp .env.example .env
# 填写 TURBK_SERVER_URL、TURBK_AGENT_ID、TURBK_AGENT_SECRET 和 TURBK_AGENT_SOURCE_DIR
docker compose pull
docker compose run --rm turbk-agent
```

也可以在被备份主机上从源码构建 agent 镜像：

```bash
cd deploy/agent
docker compose build
docker compose run --rm turbk-agent
```

agent compose 默认镜像名是 `ghcr.io/tursom/turbk-agent:latest`。它不暴露端口，只把被备份目录以只读方式挂载进容器，然后由 `turbk-agent` 主动连接服务端。当前 agent 是一次性执行模型；需要定时备份时，在被备份主机上用 cron 或 systemd timer 定时运行 `docker compose run --rm turbk-agent`。

## 开发验证

```bash
go test ./...
npm --prefix web run build
```

后续实现阶段仍以 `IMPLEMENTATION_PLAN.md` 为边界推进。
