# Turbk Agent 部署

`turbk-agent` 部署在被备份主机上。它通过 HTTP(S) 主动连接 Turbk 服务端，服务端不需要能反向访问被备份主机。

## 配置

先在 Turbk Web UI 或 API 中创建 Agent 凭据，然后把生成的 Client ID 和 Client Secret 写入 `.env`：

```bash
cp .env.example .env
```

需要修改的关键项：

- `TURBK_SERVER_URL`：Turbk 服务端地址。
- `TURBK_AGENT_ID`：服务端生成的 Client ID。
- `TURBK_AGENT_SECRET`：服务端生成的 Client Secret。
- `TURBK_AGENT_SOURCE_DIR`：宿主机上要备份的目录。
- `TURBK_AGENT_JOB_NAME`：稳定的任务名，会显示在 Turbk 的运行记录和快照中。

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

compose 会把 `TURBK_AGENT_SOURCE_DIR` 只读挂载到容器内的 `TURBK_AGENT_ROOT`，并执行一次备份。需要定时备份时，可以在被备份主机上用 cron 或 systemd timer 定时运行同一条 `docker compose run --rm turbk-agent` 命令。
