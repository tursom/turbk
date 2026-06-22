# Agent 多目录备份设计与实现基线

日期：2026-06-22

状态：实现基线。

## 1. 背景

当前 Agent 已经有两类运行方式：

- 一次性模式：`turbk-agent -root ... -once`，适合手工执行、cron、systemd timer、CI 等场景。
- 常驻 daemon 模式：`turbk-agent -root ... -daemon` 或 `TURBK_AGENT_DAEMON=true`，适合长期在线、heartbeat、服务端下发命令和本地定期备份。

当前产品和实现的主要限制是：Agent 运行时只接受一个 `root`。之前尝试把目录保存到 Host 级别的设计不合适，因为 Host 应只表示 Agent 身份、认证和最近状态，不能把“被备份目录”固化成 Host 的全局属性。

新的需求是：同一个 Agent 能够备份多个目录，并且这个能力要同时覆盖一次性模式和 daemon 模式。

## 2. 产品原则

1. Agent Host 只表示身份。
   Host 保存名称、source type、Client ID / Secret、最近心跳状态，不保存被备份目录。

2. 被备份目录属于运行配置。
   一次性模式的目录来自本次命令；daemon 模式的目录来自客户端本地配置或安装脚本。服务端可以记录运行时上报的目录信息，但不应把它当成 Host 的唯一事实来源。

3. 一次备份可以覆盖多个绝对目录。
   多个目录应作为同一次 run、同一个 snapshot、同一个 manifest 提交，目录之间用规范化绝对路径隔离。

4. 兼容单目录。
   现有 `root` 字段、`-root` 单次传参、旧 manifest 的 `source_root` 都继续支持。多目录是向后兼容扩展。

5. Agent 仍然单任务执行。
   多目录不是多任务并发。一个 run 内顺序扫描多个目录；Agent 同一时间仍只执行一个备份任务。

## 3. 目标

- Agent CLI 支持配置多个 root。
- Once 模式可以一次性备份多个目录并退出。
- Daemon 模式可以按本地配置备份多个目录，也可以响应服务端手动运行命令。
- 服务端 Agent run API 能接收多个 root 的运行信息。
- Manifest 能表达多目录快照，并保持旧快照可读。
- Web UI 接入页能生成覆盖一次性和 daemon 的多目录脚本。
- Docker/Compose 场景能把多个宿主机目录挂载到容器内，并尽量保持容器内路径与宿主机绝对路径一致。

## 4. 非目标

- 不做 Agent 端并发扫描多个目录。
- 不把目录保存到 `hosts` 表。
- 不让服务端主动探测 Agent 主机上的真实路径是否存在。
- 不在本次设计中实现 Agent 端恢复回写。
- 不为不同目录配置不同 Client ID / Secret。
- 不把多目录拆成多个独立 Host。

## 5. 目录模型

目录直接使用规范化绝对路径，不再额外引入用户可编辑 prefix。

```json
"/data/app"
```

字段含义：

- `root`：Agent 运行环境内可访问的实际绝对目录。
- manifest entry path 由绝对路径派生：去掉开头 `/` 后作为快照内路径。

单目录兼容：

```json
{
  "root": "/data/app"
}
```

多目录：

```json
{
  "roots": [
    "/data/app",
    "/var/log/myapp"
  ]
}
```

规则建议：

- root 必须是绝对路径。
- root 需要清理重复 `/`、`.` 等路径片段。
- 不允许重复 root。
- 不允许互相嵌套的 root，例如同时备份 `/data` 和 `/data/app`。这种配置会产生重复 entry path，首版直接拒绝。
- 单目录时可以继续使用旧 manifest 路径，不强制把 entry path 改成绝对路径派生形式。

## 6. Manifest 表达

现有 manifest：

```json
{
  "source_type": "agent",
  "source_root": "/data/app",
  "entries": [
    { "path": ".", "type": "dir" },
    { "path": "config.yaml", "type": "file" }
  ]
}
```

多目录 manifest 建议扩展为：

```json
{
  "source_type": "agent",
  "source_root": "",
  "source_roots": [
    "/data/app",
    "/var/log/myapp"
  ],
  "entries": [
    { "path": "data/app", "type": "dir" },
    { "path": "data/app/config.yaml", "type": "file" },
    { "path": "var/log/myapp", "type": "dir" },
    { "path": "var/log/myapp/current.log", "type": "file" }
  ]
}
```

兼容策略：

- 旧消费者继续读取 `source_root`。
- 新消费者优先读取 `source_roots`；如果不存在，则回退到 `source_root`。
- 单目录 Agent 可以继续只写 `source_root`。
- 多目录 Agent 写 `source_roots`，`source_root` 可以留空或填第一个 root。建议留空，避免误导为单根目录。

## 7. Agent Once 模式

一次性模式支持多个目录后，典型命令形态：

```bash
turbk-agent \
  -server http://backup.example.com \
  -client-id "$TURBK_AGENT_ID" \
  -client-secret "$TURBK_AGENT_SECRET" \
  -root /data/app \
  -root /var/log/myapp \
  -once
```

也可以支持环境变量：

```bash
TURBK_AGENT_ROOTS=/data/app,/var/log/myapp
turbk-agent -once
```

行为：

1. Agent 发送 once heartbeat。
2. Agent 调用 `/agent/v1/runs`，上报 `roots`。
3. 服务端创建一个 run 和一个 Agent job 记录。
4. Agent 顺序扫描多个 root。
5. Agent 生成一个 manifest，entry path 使用绝对路径派生路径隔离。
6. Agent 上传 chunk、提交 manifest、finish run。
7. 任一 root 不存在或扫描失败，整个 run 失败。

是否允许“部分成功”暂不建议引入。首版保持 all-or-nothing，行为更容易理解，也和当前单 root 失败语义一致。

## 8. Agent Daemon 模式

Daemon 模式的目录仍来自客户端本地配置，而不是 Host 字段。

典型二进制命令：

```bash
turbk-agent \
  -server http://backup.example.com \
  -client-id "$TURBK_AGENT_ID" \
  -client-secret "$TURBK_AGENT_SECRET" \
  -state-dir /var/lib/turbk-agent \
  -root /data/app \
  -root /var/log/myapp \
  -daemon
```

行为：

- heartbeat 继续上报 daemon 状态。
- 服务端手动运行下发 `run-backup` 命令，并在 payload 中携带服务端保存的 roots。
- Agent 收到命令后优先使用 command payload 的 roots 发起 run；旧命令或无 roots payload 时回退到本地配置的 roots。
- 本地 `backup_schedule` 命中时也使用同一组 roots 发起 run。
- 多余命令仍按现有策略 `dropped: agent_busy`。

服务端保存的 roots 用于生成接入配置和手动触发命令。Docker 模式下服务端仍无法验证宿主机路径，用户必须确保 compose/docker/systemd 配置把这些路径挂载或暴露给 Agent。

## 9. 服务端 API 合约

### 9.1 创建 Agent run

旧请求继续支持：

```json
{
  "hostname": "agent-host",
  "root": "/data/app",
  "trigger": "once"
}
```

新请求：

```json
{
  "hostname": "agent-host",
  "roots": [
    "/data/app",
    "/var/log/myapp"
  ],
  "trigger": "once"
}
```

服务端行为：

- 如果存在 `roots`，优先使用 `roots`。
- 如果没有 `roots`，回退到旧字段 `root`。
- `source_config` 记录实际运行配置：

```json
{
  "roots": [
    "/data/app",
    "/var/log/myapp"
  ],
  "hostname": "agent-host",
  "run_key": ""
}
```

### 9.2 Job source_config

旧格式：

```json
{ "root": "/backup/source" }
```

新格式：

```json
{
  "roots": [
    "/data/app",
    "/var/log/myapp"
  ]
}
```

对于 Agent job，`source_config` 是“最近一次运行或服务端记录的配置”，不是 Host 的静态目录配置。Daemon 运行时仍以客户端当前本地配置为准。

## 10. Docker / Compose 映射

Docker 场景优先把宿主机目录挂载到容器内相同的绝对路径。这样 Agent 看到的 root、manifest 中记录的 root、用户理解的宿主机路径三者一致。

用户输入的是宿主机路径：

```text
/data/app
/var/log/myapp
```

Compose 示例：

```yaml
services:
  turbk-agent:
    image: ghcr.io/tursom/turbk-agent:latest
    environment:
      TURBK_SERVER_URL: "http://backup.example.com"
      TURBK_AGENT_ID: "agt_replace_me"
      TURBK_AGENT_SECRET: "ags_replace_me"
      TURBK_AGENT_DAEMON: "true"
      TURBK_AGENT_STATE_DIR: "/var/lib/turbk-agent"
      TURBK_AGENT_ROOTS: "/data/app,/var/log/myapp"
    volumes:
      - "./agent-state:/var/lib/turbk-agent"
      - "/data/app:/data/app:ro"
      - "/var/log/myapp:/var/log/myapp:ro"
```

Docker run 示例：

```bash
docker run -d --name turbk-agent --restart unless-stopped \
  -e TURBK_SERVER_URL="http://backup.example.com" \
  -e TURBK_AGENT_ID="agt_replace_me" \
  -e TURBK_AGENT_SECRET="ags_replace_me" \
  -e TURBK_AGENT_DAEMON=true \
  -e TURBK_AGENT_STATE_DIR="/var/lib/turbk-agent" \
  -e TURBK_AGENT_ROOTS="/data/app,/var/log/myapp" \
  -v "turbk-agent-state:/var/lib/turbk-agent" \
  -v "/data/app:/data/app:ro" \
  -v "/var/log/myapp:/var/log/myapp:ro" \
  ghcr.io/tursom/turbk-agent:latest
```

一次性 Docker run 去掉 `-d`、`--restart` 和 `TURBK_AGENT_DAEMON=true`，并显式加 `-once` 或依赖非 daemon 执行后退出。

注意：如果用户要备份 `/etc`、`/usr`、`/bin` 等会覆盖容器运行环境的路径，同路径挂载可能影响容器内 CA、shell 或系统文件。首版 Web UI 可以给出提示，建议这类场景优先使用 Linux binary/systemd 方式运行 Agent。后续如必须覆盖这类 Docker 场景，再扩展显式 runtime mount 映射。

## 11. Web UI 需求

Agent 接入页需要覆盖两个维度：

- 运行方式：Once / Daemon。
- 安装方式：Docker Compose / Docker run / Linux binary / systemd。

目录输入建议是列表，而不是单个输入框：

| 字段 | 含义 |
| --- | --- |
| 绝对目录 | Docker/Compose 下的 host path，binary 下就是实际 root |

交互建议：

- 默认给一行 `/srv/data`。
- 用户可以添加多行目录。
- 非绝对路径、重复路径、嵌套路径在前端直接提示。
- Docker/Compose 代码块根据目录列表生成多条 volume。
- Binary/systemd 代码块根据目录列表生成多个 `-root`。
- Once 模式展示一次性执行命令。
- Daemon 模式展示常驻服务命令。

不建议在创建 Agent Host 的抽屉里收集目录。创建 Host 只生成身份；目录配置放在 Host 详情的 Agent 接入页。

## 12. Restore / 浏览语义

多目录 snapshot 的浏览根目录展示绝对路径派生出的顶层目录：

```text
data/
var/
```

下载或恢复 `data/app/config.yaml` 时按 manifest entry path 工作。UI 可以在 snapshot 详情中显示 `source_roots`，让用户知道它对应原始目录 `/data/app`。

单目录旧快照保持原样：

```text
config.yaml
```

## 13. Catalog 语义

Agent 本地 catalog 现有表以 `root_id + path` 作为文件记录主键，天然适合多目录。

建议：

- `root_id` 使用清理后的实际 root 路径。
- catalog 内部 path 仍是相对该 root 的路径。
- manifest path 在生成阶段转换为绝对路径派生路径。
- chunk 确认状态仍按 hash 全局复用，不绑定 root。

这样多个目录中相同内容的文件仍能复用 chunk。

## 14. 错误处理

首版建议：

- roots 为空：报错。
- root 不是绝对路径：报错。
- root 不存在：run 失败。
- root 不是目录：run 失败。
- root 重复或互相嵌套：客户端启动失败；服务端 API 返回 400。
- 扫描任一目录失败：整个 run 失败。
- 多 root 中的 entry path 冲突：视为 root 校验 bug，run 失败。

## 15. 兼容性

必须保留：

- `-root /path` 单目录命令。
- `TURBK_AGENT_ROOT=/path`。
- `/agent/v1/runs` 请求里的 `root` 字段。
- `source_config.root`。
- manifest 的 `source_root`。

新增：

- 重复 `-root` 或 `TURBK_AGENT_ROOTS`。
- `/agent/v1/runs` 请求里的 `roots`。
- `source_config.roots`。
- manifest 的 `source_roots`。

旧数据无需迁移。

## 16. 实施阶段建议

### 阶段一：后端与 Agent 核心

- 定义 root 绝对路径校验、规范化和嵌套检测规则。
- Agent CLI 支持多个 root。
- Agent once/daemon 使用同一组 roots 顺序扫描。
- Agent run API 接收 `roots`。
- Manifest 支持 `source_roots`。
- 保持单 root 行为不变。

### 阶段二：Web UI 接入页

- 接入页增加 Once / Daemon 切换。
- 目录输入改成多行列表。
- 生成多目录 Docker Compose、Docker run、binary、systemd 示例。
- 不在 Host 创建流程中保存目录。

### 阶段三：部署模板与文档

- 更新 `deploy/agent` 示例，说明单目录默认和多目录手动扩展方式。
- 更新 README 的 Agent once、daemon、多目录示例。
- 补充恢复浏览多目录 snapshot 的说明。

## 17. 测试要求

- roots 解析：单 root、多个 root、相对路径、重复路径、嵌套路径。
- Agent 扫描：两个目录内有同名文件时 manifest path 不冲突。
- Agent API：旧 `root` 请求和新 `roots` 请求都能创建 run。
- Manifest：旧 `source_root` 快照继续可读，新 `source_roots` 快照可浏览。
- Daemon：手动 run command 使用本地多 roots。
- Docker/Compose 生成：多目录 volume 和 `TURBK_AGENT_ROOTS` 一致。
- 前端：`npm run build`。
- 后端：`go test ./...`。

## 18. 已落地决策

1. 多目录是否确定合并为一个 snapshot？
   结论：是。一个 Agent 一次运行生成一个 snapshot，目录用绝对路径区分。

2. 多目录中某个 root 失败时，是否允许部分成功？
   结论：否。首版 all-or-nothing。

3. 是否允许相对路径？
   结论：否。多目录首版只接受绝对路径，避免不同运行目录下语义变化。

4. Daemon 的服务端手动运行是否需要下发 roots？
   结论：首版不下发，Agent 使用本地配置。服务端只记录运行时上报的 roots。

5. `source_root` 在多目录 manifest 中应该留空还是填第一个 root？
   结论：留空，新逻辑读取 `source_roots`。旧逻辑看到空值时只是不显示单 root，不会误导。

6. Docker 同路径挂载是否覆盖所有目录场景？
   结论：覆盖普通数据目录；备份 `/etc`、`/usr`、`/bin` 等可能影响容器运行环境的路径时，优先推荐 binary/systemd。后续如必须支持，再扩展 runtime mount 映射。
