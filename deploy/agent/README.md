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
- `TURBK_AGENT_SOURCE_DIR`：宿主机上要备份的目录。

## 运行

使用已经推送到 GHCR 的 agent 镜像：

```bash
docker compose pull
docker compose run --rm turbk-agent
```

从源码构建 agent 镜像：

```bash
docker compose build
docker compose run --rm turbk-agent
```

compose 会把 `TURBK_AGENT_SOURCE_DIR` 只读挂载到容器内固定路径 `/backup/source`，并执行一次备份。需要定时备份时，可以在被备份主机上用 cron 或 systemd timer 定时运行同一条 `docker compose run --rm turbk-agent` 命令。

## Web UI 接入方式

主机详情里的“客户端接入”面板会按当前主机生成可复制的接入配置，适合像 Tunnel 接入页一样直接交给被备份主机执行：

- Docker Compose：生成 `compose.yaml` 并运行一次客户端容器，适合长期保留配置。
- Docker run：直接运行一次客户端容器，适合临时验证或脚本化调用。
- Linux 二进制：在不能使用 Docker 的主机上直接运行 `turbk-agent`。
- systemd 定时器：安装 `turbk-agent.service` 和 `turbk-agent.timer`，默认每天 02:00 执行一次。

无论选择哪种方式，客户端都只需要 `TURBK_SERVER_URL`、`TURBK_AGENT_ID`、`TURBK_AGENT_SECRET` 和被备份目录。Agent host 与 agent job 在服务端一对一绑定，客户端不需要也不能选择任务。Docker 方式会把被备份目录只读挂载到容器内固定路径 `/backup/source`，这个路径是实现细节，不需要用户自行选择。
