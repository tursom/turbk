# Host / Credential Model Design

## 1. 背景

Turbk 同时支持服务端主动拉取和 Agent 主动连接两类备份方式。早期实现里，凭据、主机和任务之间的职责容易混在一起：Pull job 直接选择 credential，Credentials 页面也展示 Agent credential。这会导致几个问题：

- 凭据页面无法明确区分“上游连接材料”和“客户端身份”。
- 主机页面看不到该主机实际使用哪组凭据。
- 同一主机有多个备份任务时，每个任务都可能重复配置凭据。
- Agent 的 Client ID / Secret 被当成普通凭据展示，不符合一对一主机身份的直觉。

本文定义新的目标模型：后端分为凭据表和主机表，某个主机使用某个凭据访问上游；任务引用主机，不直接承担连接身份配置。

当前开发阶段不考虑历史数据兼容。允许直接调整 SQLite schema、API 请求结构和前端状态结构，部署时可以重新初始化 `state_dir`。

## 2. 核心原则

- Credential 表示“如何认证”：用户名、密码、SSH 私钥、Bearer token、Agent Client Secret 等敏感材料。
- Host 表示“连接到哪里”：主机名、协议类型、地址、最近心跳、状态，以及当前使用的 credential。
- Job 表示“备份什么和何时备份”：源目录、调度、启停、超时、重试策略。
- Pull 类型 credential 独立存在，可以被多个 host 复用。
- Agent 类型 credential 由服务端生成，并绑定到一个 Agent host；它不出现在 Credentials 页面。
- Web UI 中，Credentials 页面只管理 Pull 凭据；Agent 接入信息只在 Hosts 页面管理。

## 3. 数据模型

### 3.1 Credentials

`credentials` 表保存认证材料。

建议字段：

```text
id
name
type              -- sftp | ftp | ftps | webdav | agent
encrypted_payload
created_at
updated_at
```

Pull credential payload 只保存认证相关字段，不保存上游地址：

```json
{
  "username": "root",
  "password": "...",
  "private_key": "..."
}
```

FTP/FTPS：

```json
{
  "username": "backup",
  "password": "...",
  "tls": true,
  "explicit_tls": true,
  "skip_tls_verify": false
}
```

WebDAV：

```json
{
  "username": "backup",
  "password": "...",
  "bearer_token": "..."
}
```

Agent：

```json
{
  "client_id": "agt_...",
  "client_secret": "ags_...",
  "secret_hash": "...",
  "subject": "host-name"
}
```

Agent credential 仍复用 credentials 表的加密存储能力，但不属于 Credentials 管理页的展示范围。

### 3.2 Agent Credential Lookup

Agent 高频访问不能依赖解密扫描。`client_id` 必须有可索引的查找路径。

推荐新增独立索引表：

```text
agent_credentials
credential_id     -- primary key, references credentials(id)
host_id           -- unique, references hosts(id)
client_id         -- unique, indexed
secret_hash       -- hash(client_id + "\0" + client_secret)
subject
created_at
updated_at
last_used_at
revoked_at
```

说明：

- `client_id` 是公开标识，不是 secret，可以明文存储并建立唯一索引。
- 如果希望数据库中不直接暴露 client_id，也可以存 `client_id_hash = HMAC(server_lookup_key, client_id)`，并对该字段建立唯一索引。
- `secret_hash` 用于认证比较，不需要解密 credential payload。
- `credentials.encrypted_payload` 仍可保存 `client_secret`，用于 Host 页面重复显示 Secret。
- `agent_credentials.host_id` 保证 Agent credential 和 host 一对一。

Agent 认证流程：

1. Agent 通过 HTTP Basic Auth 或等价 header 提交 `client_id` 和 `client_secret`。
2. Server 用 `client_id` 查询 `agent_credentials` 的唯一索引。
3. 如果不存在、已 revoked，直接拒绝。
4. Server 计算 `hash(client_id + "\0" + client_secret)`。
5. 使用 constant-time compare 对比 `secret_hash`。
6. 认证成功后得到 credential_id 和 host_id，后续权限检查只允许访问该 host 下的 job/run/snapshot。

这个路径只需要一次索引查询和一次 hash 计算，不能遍历 credentials 表，也不能逐条解密 payload。

### 3.3 Hosts

`hosts` 表保存上游来源和凭据绑定。

建议字段：

```text
id
name
source_type       -- local | sftp | ftp | ftps | webdav | agent
address           -- endpoint, URL, mount label, or latest agent hostname
credential_id     -- nullable for local, required for pull/agent
status
last_seen_at
created_at
updated_at
```

约束：

- `local` host 不需要 credential。
- `sftp` host 的 address 是 SSH/SFTP endpoint，例如 `example.com:22`。
- `ftp` / `ftps` host 的 address 是 FTP endpoint，例如 `example.com:21`。
- `webdav` host 的 address 是 WebDAV base URL，例如 `https://storage.example.com/dav`。
- `agent` host 的 credential 由服务端生成并绑定；address 可由心跳更新为 Agent hostname；认证查找走 `agent_credentials.client_id` 索引。
- host.source_type 必须和 credential.type 一致，除 `local` 外必须有 credential_id。

### 3.4 Jobs

`jobs` 表保存备份计划，并引用 host。

建议字段：

```text
id
host_id
name
source_type       -- 可冗余保存 host.source_type，便于查询和快照归档
source_config     -- root/path 等备份源配置
enabled
schedule
timezone
max_runtime_seconds
retry_attempts
created_at
updated_at
```

原则：

- Job 创建时必须选择 host。
- Job 不直接选择 credential。
- 非 local job 执行时从 host.credential_id 获取认证材料。
- 修改某台主机使用的凭据，会影响该 host 下后续运行的 jobs。
- Snapshot 和 Run 继续记录 host_id/job_id，便于审计和恢复。

## 4. 运行时连接流程

### 4.1 Pull Job

1. Scheduler 或用户手动触发 job。
2. Server 读取 job.host_id。
3. Server 读取 host，得到 source_type、address、credential_id。
4. 如果 source_type 不是 local，Server 读取 credential 并校验类型一致。
5. Server 组合 connector 配置：
   - host.address 提供 endpoint/base URL。
   - credential.payload 提供认证材料。
   - job.source_config.root 提供远端备份根目录。
6. Connector 执行 Walk/Open。
7. Repository 生成增量 snapshot。

### 4.2 Local Job

local host 不需要 credential。job.source_config.root 是容器或宿主机内可访问路径。

### 4.3 Agent Job

1. 创建 Agent host 时，Server 同时生成 agent credential。
2. Host 页面展示 Client ID / Secret 和部署参数。
3. Agent 使用 Client ID / Secret 登录 Server。
4. Server 根据 agent credential 找到绑定 host。
5. Server 按绑定 host 自动创建或复用唯一的 agent job；Agent 不提交 job id/name。
6. Agent 只能创建或更新该 host 绑定 job 范围内的 runs/snapshots。
7. Heartbeat 更新 host.status、host.address、host.last_seen_at。

## 5. API 设计

### 5.1 Credentials API

`GET /api/v1/credentials`

- 默认只返回 Pull credentials：sftp、ftp、ftps、webdav。
- 不返回 agent credentials。

`POST /api/v1/credentials`

- 创建 Pull credential。
- 请求不包含 address/base_url；这些字段属于 host。
- agent credential 不通过该页面的普通创建流程创建。

示例：

```json
{
  "name": "prod sftp root",
  "type": "sftp",
  "payload": {
    "username": "root",
    "private_key": "..."
  }
}
```

### 5.2 Hosts API

`GET /api/v1/hosts`

返回 host 列表，包含 credential_id，并可返回非敏感 credential 摘要用于页面展示：

```json
{
  "id": 1,
  "name": "prod-db",
  "source_type": "sftp",
  "address": "prod-db.example.com:22",
  "credential_id": 3,
  "credential": {
    "id": 3,
    "name": "prod sftp root",
    "type": "sftp"
  },
  "status": "unknown"
}
```

`POST /api/v1/hosts`

创建 Pull host 时必须提供 credential_id：

```json
{
  "name": "prod-db",
  "source_type": "sftp",
  "address": "prod-db.example.com:22",
  "credential_id": 3
}
```

创建 Agent host 时由服务端生成 credential 并返回 Client ID / Secret：

```json
{
  "name": "edge-node-1",
  "source_type": "agent"
}
```

响应：

```json
{
  "host": {
    "id": 2,
    "name": "edge-node-1",
    "source_type": "agent",
    "credential_id": 4
  },
  "agent": {
    "client_id": "agt_...",
    "client_secret": "ags_..."
  }
}
```

`PATCH /api/v1/hosts/:id`

用于修改 host 名称、地址、绑定 credential、状态管理字段。修改 credential_id 时必须校验类型一致。

### 5.3 Jobs API

`POST /api/v1/jobs`

Job 创建时选择 host_id，不再要求用户选择 credential_id：

```json
{
  "name": "prod-db daily",
  "host_id": 1,
  "source_config": {
    "root": "/var/lib/postgresql"
  },
  "enabled": true,
  "schedule": "0 2 * * *",
  "timezone": "Asia/Shanghai"
}
```

后端根据 host.source_type 填充 job.source_type，根据 host.credential_id 执行连接；请求不接受 source_type 或 credential_id。

`PATCH /api/v1/jobs/:id`

允许修改任务名称、source_config、enabled、schedule、timezone、max_runtime_seconds、retry_attempts。请求不接受 host_id、source_type 或 credential_id 修改，连接关系入口应回到 Hosts 页面。

## 6. Web UI 设计

### 6.1 Credentials 页面

定位：Pull 凭据资产管理。

默认视图：

- 只显示凭据列表。
- 不显示 Agent credential。
- 创建入口放在弹窗或抽屉中。

列表字段：

- 名称。
- 类型。
- 引用主机数。
- 引用任务数。
- 创建时间。
- 更新时间。

创建表单：

- 类型：SFTP、FTP、FTPS、WebDAV。
- SFTP：username、password/private_key。
- FTP/FTPS：username、password、TLS 选项。
- WebDAV：username/password 或 bearer_token。
- 不填写上游地址。

### 6.2 Hosts 页面

定位：上游来源和连接关系管理。

默认视图：

- 主机列表。
- 主机详情。
- 创建主机入口。

Pull host 创建流程：

1. 选择连接类型。
2. 填写主机名称和 address。
3. 选择已有匹配类型 credential。
4. 可从该流程跳转创建新 Pull credential，创建后回填选择。

Agent host 创建流程：

1. 输入主机名称。
2. 服务端创建 host 和 agent credential。
3. 页面展示 Client ID / Secret、Server URL、被备份目录和 Docker/Compose 配置。
4. Secret 可以重复显示。

Host 详情展示：

- source_type。
- address。
- 当前 credential 名称和类型。
- Agent host 的 Client ID / Secret。
- 关联 jobs。
- 最近心跳和状态。

### 6.3 Jobs 页面

定位：备份计划管理。

创建任务时：

- 选择 host。
- 填写 root/path。
- 配置 schedule、timezone、timeout、retry。
- 页面不再直接选择 credential。

任务列表可展示 host 名称和 credential 摘要，但连接配置入口应回到 Hosts 页面。

## 7. 校验规则

后端必须执行以下校验：

- 创建 Pull credential 时，payload 必须满足该类型认证要求。
- 创建 Pull host 时，address 必填，credential_id 必填，credential.type 必须等于 host.source_type。
- 创建 local host 时，address 和 credential_id 必须为空。
- 创建 Agent host 时，不允许客户端提交 address、client_id/client_secret；服务端生成 Client ID / Secret，address 由心跳更新。
- Agent client_id 必须通过唯一索引查找，认证路径不得扫描或解密所有 credentials。
- Agent secret 校验必须使用 hash + constant-time compare。
- 创建 job 时，host_id 必填，host 必须存在，不能直接提交 source_type 或 credential_id。
- 执行 Pull job 时，host/credential 类型不一致应直接失败并记录 run error。
- Agent 认证后只能操作绑定到该 credential/host 的资源。
- Agent host 与 agent job 一对一；Agent 创建 run 时不得由客户端选择 job id/name。

## 8. 实施步骤

1. 调整 SQLite schema：hosts 增加 credential_id，jobs 改为以 host_id 为主。
2. 新增 agent_credentials 索引表，对 client_id 或 client_id_hash 建唯一索引。
3. 调整 state 层结构：Host 包含 CredentialID；Job 创建和查询支持 HostID；Agent 认证按 client_id 索引查询。
4. 调整管理 API：Credentials 过滤 Agent；Hosts 创建 Agent 时生成 credential 和 agent_credentials 记录；Jobs 创建使用 host_id。
5. 调整 connector 创建逻辑：host.address + credential.payload + job.source_config.root。
6. 调整 Agent API：认证后解析绑定 host，并确保 run/job 属于该 host；agent job 按 host 一对一生成。
7. 调整前端状态：Credentials 过滤 Agent；Hosts 管理 credential 绑定；Jobs 选择 host。
8. 调整测试：覆盖类型校验、host credential 绑定、job 从 host 继承连接、Agent 一对一凭据、client_id 索引查找。

## 9. 非目标

首版不处理：

- 旧数据库在线迁移。
- 多个 credential 同时绑定一台 host。
- 单个 job 临时覆盖 host credential。
- 在 Credentials 页面管理 Agent credential。
- 按用户或租户隔离 credential。
- 客户端零信任加密。

## 10. 待确认事项

- `GET /api/v1/credentials` 是否需要管理员参数显示 agent credentials，默认建议不提供。
- Job 是否允许修改 host_id，默认建议首版不在 UI 暴露。
- Pull host 是否允许无 credential 暂存草稿，默认建议后端不允许，前端可在创建流程中先创建 credential。
