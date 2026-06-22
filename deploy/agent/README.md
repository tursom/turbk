# Turbk Agent 部署

`turbk-agent` 部署在被备份主机上。它通过 HTTP(S) 主动连接 Turbk 服务端，服务端不需要能反向访问被备份主机。

## 配置

先在 Turbk Web UI 或 API 中创建 Agent 主机，然后把服务端生成的 Client ID 和 Client Secret 写入 `.env`：

```bash
cp .env.example .env
```

需要修改的关键项：

- `TURBK_SERVER_URL`：Turbk 服务端地址。
- `TURBK_AGENT_ID`：服务端生成的 Client ID。
- `TURBK_AGENT_SECRET`：服务端生成的 Client Secret。
- `TURBK_AGENT_SOURCE_DIR`：宿主机上要备份的目录；默认 compose 模板用于单目录。
- `TURBK_AGENT_ROOTS`：Agent 运行环境内要备份的绝对目录，多个目录用逗号分隔。
- `TURBK_AGENT_STATE_HOST_DIR`：宿主机上保存 agent 本地 catalog 的目录，必须持久化。
- `TURBK_AGENT_BACKUP_INTERVAL`：daemon 本地定期备份间隔，默认 `24h`；服务端手动运行会通过轮询 command 触发。
- `TURBK_AGENT_EXCLUDES`：可选，逗号或换行分隔的排除规则，规则相对被备份目录，例如 `overlay2/*/merged/proc/**`。
- `TURBK_AGENT_SKIP_PSEUDO_FS`：可选，默认 `true`，自动跳过 procfs、sysfs、cgroup 等 Linux 伪文件系统。

## 运行

使用已经推送到 GHCR 的 agent 镜像：

```bash
docker compose pull
docker compose up -d
```

从源码构建 agent 镜像：

```bash
docker compose build
docker compose up -d
```

compose 会把 `TURBK_AGENT_SOURCE_DIR` 只读挂载到容器内相同的绝对路径，并把 `TURBK_AGENT_STATE_HOST_DIR` 持久挂载到 `/var/lib/turbk-agent`。agent 默认以 daemon 模式常驻运行，本地 SQLite catalog 会记录文件元数据、文件 chunk 列表和服务端已确认 chunk 状态。删除 state 目录不会损坏服务端数据，但下次启动会退化为重新扫描和重新向服务端确认 chunk。

daemon 默认每 10 分钟轮询一次服务端，领取 Web 手动运行产生的 command；本地定期备份由 `TURBK_AGENT_BACKUP_INTERVAL` 控制，默认 `24h`。同一时间只执行一个备份，忙碌时收到的多余手动 command 会被标记为 dropped。

## 多目录

多个目录会作为同一次 run、同一个 snapshot、同一个 manifest 提交。目录必须是绝对路径，不能重复或互相嵌套。

二进制方式可以重复传 `-root`：

```bash
turbk-agent \
  -server http://backup.example.com:8080 \
  -client-id "$TURBK_AGENT_ID" \
  -client-secret "$TURBK_AGENT_SECRET" \
  -root /data/app \
  -root /var/log/myapp \
  -once
```

Docker/Compose 方式需要让 `TURBK_AGENT_ROOTS` 和只读 volume 保持一致：

```yaml
environment:
  TURBK_AGENT_ROOTS: "/data/app,/var/log/myapp"
volumes:
  - "./agent-state:/var/lib/turbk-agent"
  - "/data/app:/data/app:ro"
  - "/var/log/myapp:/var/log/myapp:ro"
```

如果要备份 `/etc`、`/usr`、`/bin` 等系统路径，同路径挂载可能影响容器内运行环境，优先使用 Linux binary 或 systemd 方式。

如果备份 Docker 数据目录，运行中的 overlay `merged` 目录可能包含容器内的 `/proc`、`/sys`、`/dev` 等挂载视图。agent 默认会按文件系统类型跳过 procfs、sysfs、cgroup 等伪文件系统；仍需要额外排除路径时，可以在 `.env` 中配置：

```env
TURBK_AGENT_EXCLUDES=overlay2/*/merged/proc/**,overlay2/*/merged/sys/**
```

## Web UI 接入方式

主机详情里的“客户端接入”面板会按当前主机生成可复制的接入配置，适合像 Tunnel 接入页一样直接交给被备份主机执行：

- Docker Compose：生成 `compose.yaml` 并运行常驻客户端服务，适合长期保留配置。
- Docker run：直接运行常驻客户端容器，适合不保留 compose 文件的主机。
- Linux 二进制：在不能使用 Docker 的主机上直接运行 `turbk-agent -daemon`。
- systemd 服务：安装长期运行的 `turbk-agent.service`。

无论选择哪种方式，客户端都只需要 `TURBK_SERVER_URL`、`TURBK_AGENT_ID`、`TURBK_AGENT_SECRET` 和被备份目录。Agent host 与 agent job 在服务端一对一绑定，客户端不需要也不能选择任务。Docker 方式会把被备份目录只读挂载到容器内相同绝对路径，方便 manifest 与宿主机路径保持一致。
