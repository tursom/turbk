# Agent Chunk 批量接口设计

日期：2026-06-22

状态：设计草案。

本文定义 Turbk Agent 在公网高延迟环境下的 chunk 批量查询与批量上传方案。目标是让新 agent 的 chunk 网络交互主路径不再逐 chunk 发起 `GET /agent/v1/chunks/{hash}` 和 `PUT /agent/v1/chunks/{hash}`，而是使用批量接口摊薄 RTT。

本文中的“所有 chunk 接口批量化”指：新 agent 的扫描、上传、manifest 缺失修复和 generation 失效校验路径都必须使用批量接口。旧单 chunk GET/PUT 只作为兼容旧 agent 的 legacy endpoint，不作为新 agent 的正常路径。

## 1. 背景

Agent 当前已经支持批量 chunk check：

```http
POST /agent/v1/chunks/check
```

但这个接口只在“文件元数据未变化、复用本地 catalog 的旧 chunk 列表”时使用。首次备份或文件变化后重新分块时，agent 仍然对每个 chunk 执行：

1. `GET /agent/v1/chunks/{hash}` 判断服务端是否已有 chunk。
2. 如果不存在，再 `PUT /agent/v1/chunks/{hash}` 上传该 chunk。

公网环境下，逐 chunk GET/PUT 会被 RTT 放大。即使单个 chunk 只有 1 MiB，几千个 chunk 也会产生几千次 HTTP 往返，实际耗时会被延迟主导，而不是带宽或压缩/加密吞吐主导。

## 2. 目标

- 新 agent 扫描路径中的 chunk 查询必须批量化。
- 新 agent 上传 missing chunks 必须批量化。
- 新 agent 自动修复 `missing_chunks` 时必须批量补传。
- 保留旧单 chunk GET/PUT 接口作为兼容接口，但新 agent 默认不再使用。
- 批量协议避免 JSON/base64 放大 chunk body，优先使用二进制请求体。
- 服务端继续校验每个 chunk 的 hash，不能信任客户端声明。
- 服务端继续保持幂等去重：重复上传已有 chunk 不追加写 segment。
- 批量接口必须受现有备份写入 gate 保护，不能与 compact 冲突。
- 批量大小由服务端配置约束，agent 根据 heartbeat 下发的上限自动切片。

## 3. 非目标

- 不改变 manifest 提交流程。服务端仍在 `POST /agent/v1/manifests` 中 canonicalize chunk 引用并校验缺失 chunk。
- 不要求批量上传在服务端事务层面全有全无。chunk 上传是内容寻址、幂等写入，失败后 agent 可以重试整批或拆批。
- 不实现并发多批上传。首版先减少 RTT，保持单备份任务内串行批次，避免压垮源主机和服务端。
- 不删除旧 `/agent/v1/chunks/{hash}` 接口。

## 4. 现有接口问题

### 4.1 单 chunk GET

```http
GET /agent/v1/chunks/{hash}
```

问题：

- 每个未知 chunk 一次 HTTP 往返。
- 对服务端已有 chunk 也必须等待一次 RTT 才能继续。
- 多数响应很小，HTTP header 和 TLS 往返占比高。

### 4.2 单 chunk PUT

```http
PUT /agent/v1/chunks/{hash}
Content-Type: application/octet-stream
```

问题：

- 每个 missing chunk 一次 HTTP request。
- 每个请求独立进入写入 gate 和 repo 写入路径。
- 公网 TLS/HTTP 开销与请求数线性相关。

### 4.3 批量 check 覆盖不足

```http
POST /agent/v1/chunks/check
```

当前只用于 catalog stale chunk 确认。变化文件重新分块时没有先收集一批 hash 再 check，而是逐 chunk 调 GET。

## 5. 总体方案

新 agent 扫描流程改为：

1. 扫描文件并分块。
2. 对每个 chunk 计算 hash。
3. 如果本地 catalog 已确认该 hash 在当前 generation 下存在，直接复用。
4. 否则把 chunk 放入当前批次。
5. 批次达到 hash 数量上限、字节上限或扫描结束时 flush。
6. flush 时先调用批量 check。
7. 对 check 返回的 missing chunks 调批量 upload。
8. 将 check/upload 结果批量写入本地 catalog。
9. 文件的所有 pending chunks 都完成后，再把该文件加入 manifest。
10. manifest 提交返回 `missing_chunks` 时，按缺失 hash 重新读取对应文件并继续使用批量 upload 补传。

旧 agent 仍可继续使用单 chunk GET/PUT。服务端必须同时支持旧接口和新批量接口。

## 6. 批量 Check 接口

继续使用现有路径：

```http
POST /agent/v1/chunks/check
Content-Type: application/json
```

请求：

```json
{
  "repository_id": "repo_x",
  "base_chunk_generation": 12,
  "hashes": ["..."]
}
```

响应保持兼容：

```json
{
  "repository_id": "repo_x",
  "chunk_generation": 13,
  "exists": ["..."],
  "missing": ["..."]
}
```

可选扩展响应：

```json
{
  "chunks": [
    {
      "hash": "...",
      "exists": true,
      "ref": {
        "hash": "...",
        "original_size": 1048576
      }
    }
  ]
}
```

首版 agent 不强依赖 `chunks[].ref`。manifest 可以继续只携带 `hash` 和 `original_size`，最终由服务端 manifest canonicalize 改写为权威 `ChunkRef`。

限制：

- `len(hashes) <= agent.max_chunk_check_batch`。
- 默认 `max_chunk_check_batch = 10000`。
- agent 如果本地批次超过上限，必须自动切片。

## 7. 批量 Upload 接口

新增接口：

```http
POST /agent/v1/chunks/upload
Content-Type: application/vnd.turbk.chunk-batch.v1
```

不使用 JSON/base64。请求体使用二进制格式。

### 7.1 请求体格式

所有多字节整数使用 big-endian。

```text
8 bytes  magic = "TBKCHB1\n"
u32      chunk_count
repeat chunk_count:
  32 bytes hash        // blake3(data)
  u64      data_length
  bytes    data
```

说明：

- hash 使用二进制 32 字节，不传 hex 字符串。
- 服务端必须重新计算 `blake3(data)` 并与请求 hash 比对。
- `data_length` 为单个 chunk 原始字节数。
- `chunk_count` 不能超过服务端 `max_chunk_upload_batch_chunks`。
- 请求体总大小不能超过服务端 `max_chunk_upload_batch_bytes`。
- repository/run 归属由 agent 认证和服务端 active run 校验决定，不放在二进制 body 中。

首版可复用现有 `agent.max_chunk_check_batch` 作为 chunk 数上限，并新增字节上限：

```yaml
agent:
  max_chunk_check_batch: 10000
  max_chunk_upload_batch_bytes: "64MiB"
```

如果暂不新增配置，服务端内部先使用固定默认值 `64MiB`。

### 7.2 响应格式

响应用 JSON，便于调试：

```json
{
  "status": "accepted",
  "repository_id": "repo_x",
  "chunk_generation": 13,
  "chunks": [
    {
      "hash": "...",
      "exists": true,
      "uploaded": true,
      "ref": {
        "hash": "...",
        "original_size": 1048576,
        "compressed_size": 123456,
        "segment_id": 3,
        "offset": 456,
        "length": 123789
      }
    }
  ]
}
```

字段：

- `exists`：上传后服务端确认该 chunk 存在。
- `uploaded`：本次请求是否实际写入新 chunk。重复上传已有 chunk 时为 `false`。
- `ref`：服务端当前 chunk index 中的引用。

### 7.3 错误语义

- magic 不正确：`400 Bad Request`。
- hash 格式或 data hash 不匹配：`400 Bad Request`，整批失败。
- chunk 数量超过上限：`400 Bad Request`。
- 请求体超过字节上限：`413 Request Entity Too Large` 或 `400 Bad Request`。
- 写入 gate 忙：`409 Conflict`。
- 服务端写入中途失败：`500 Internal Server Error`。

批量上传是幂等的。agent 遇到网络失败或 5xx 可以重试整个批次；服务端 `PutChunk` 会对已有 hash 去重。

## 8. Agent 扫描侧批处理

### 8.1 批次边界

agent 维护一个 chunk batcher：

```text
max_chunks = heartbeat.agent.max_chunk_check_batch
max_bytes  = 64MiB
```

flush 条件：

- pending chunk 数达到 `max_chunks`。
- pending chunk 原始字节数达到 `max_bytes`。
- 当前 root 扫描结束。
- 整次扫描结束。

### 8.2 跨文件合批

批次应该跨文件累计，而不是每个文件单独 flush。否则大量小文件仍会产生大量请求。

文件 entry 在批次完成前处于 pending 状态：

```text
file A -> pending chunk count 2
file B -> pending chunk count 1
batch flush -> check/upload -> 填充 refs -> finalize file A/B manifest entries
```

如果一个文件很大，批次可以在文件内部多次 flush。文件只有在全部 chunks 都完成后才能加入 manifest。

### 8.3 Catalog 交互

每个 chunk：

1. 先查询本地 catalog。
2. 如果 `status=confirmed` 且 `confirmed_generation >= heartbeat.chunk_generation`，直接复用，不进入网络 batch。
3. 其他情况进入 batch check。

批量 check 后：

- `exists`：批量标记 catalog 为 confirmed。
- `missing`：进入批量 upload。

批量 upload 后：

- 返回 chunks 全部标记 catalog 为 confirmed。
- `uploaded=true` 计入 uploaded chunk 进度。
- `uploaded=false` 计入 reused chunk 进度。

本地 catalog 写入也应批量化，避免网络批量后又在本地逐 chunk 写 SQLite/Pebble。

### 8.4 Manifest 缺失修复

manifest 提交仍然是最终一致性防线。服务端返回 `missing_chunks` 后，agent 不能退回单 chunk PUT。

修复流程：

1. 按缺失 hash 查找本次 manifest 中引用它的文件。
2. 重新读取这些文件并重新分块。
3. 文件元数据仍一致时，只收集缺失 hash 对应的 chunk body。
4. 使用 `/agent/v1/chunks/upload` 批量补传。
5. 批量写 catalog confirmed 状态。
6. 重试提交 manifest。

如果缺失 chunk 分散在多个文件中，也应该跨文件合批补传。只有在服务端明确不支持批量 upload 的版本兼容场景下，才允许 fallback 到 legacy 单 chunk PUT。

## 9. 服务端实现要点

### 9.1 路由

新增：

```text
POST /agent/v1/chunks/upload
```

保留：

```text
GET /agent/v1/chunks/{hash}
PUT /agent/v1/chunks/{hash}
POST /agent/v1/chunks/check
```

### 9.2 写入 gate

批量 upload 整个请求只进入一次备份写入 gate：

```text
tryEnterBackupWrite()
defer releaseRunGate()
```

这样避免每个 chunk 独立抢占 gate。

### 9.3 Repository 写入

首版可以循环调用现有 `repo.PutChunk(ctx, data)`，保持去重和 segment 写入逻辑不变。

后续优化：

- 增加 `repo.PutChunks(ctx, chunks)`，在 repository 内只加一次 mutex。
- chunk index 批量写。
- segment writer 连续写多个 record 后再 sync 或按策略 sync。

首版不改变 repository 写入一致性，优先降低公网 RTT。

## 10. 兼容与灰度

- 旧 agent：继续使用单 chunk GET/PUT。
- 新 agent：默认使用批量 check/upload，扫描、修复、generation 失效校验都不走单 chunk GET/PUT。
- 服务端可以在 heartbeat agent 配置中返回：

```json
{
  "agent": {
    "max_chunk_check_batch": 10000,
    "max_chunk_upload_batch_bytes": 67108864,
    "chunk_batch_upload": true
  }
}
```

如果服务端未返回 `chunk_batch_upload=true`，agent 回退旧单 chunk 路径。

如果批量 upload 返回 404，agent 可以记录 warning 并回退单 chunk PUT。这个 fallback 只用于版本兼容，不作为常规路径。

## 11. 测试计划

### 11.1 服务端测试

- 批量上传两个新 chunk，返回两个 `uploaded=true`。
- 重复批量上传同一批 chunk，返回 `uploaded=false`，repo stats 不增长。
- 批量上传中 hash 与 body 不匹配，返回 400，不能写入错误 chunk。
- 批量上传超过大小/数量限制，返回错误。
- 批量上传与 compact 写入 gate 冲突时返回 409。
- 旧单 chunk GET/PUT 测试继续通过。

### 11.2 Agent 测试

- 新扫描文件时不调用单 chunk GET，先调用 `/chunks/check`。
- missing chunks 通过 `/chunks/upload` 一次上传。
- manifest `missing_chunks` 修复路径也通过 `/chunks/upload` 批量补传。
- 多个小文件合并到同一批 check/upload。
- 大文件超过 batch bytes 后拆成多批。
- 批量 upload 404 时回退单 chunk PUT。
- catalog confirmed 的 chunk 不进入网络 batch。

### 11.3 端到端测试

- 首次备份：chunk check 和 upload 请求数明显小于 chunk 数。
- 第二次备份：catalog 复用，不读取未变化文件内容。
- 断网重试：批量上传中断后重新运行，服务端去重，不重复追加已有 chunk。
- 公网模拟：增加人工 RTT，验证批量路径耗时优于逐 chunk 路径。

## 12. 验收标准

- 新 agent 默认不再调用 `GET /agent/v1/chunks/{hash}` 检查新扫描 chunk。
- 新 agent 默认不再调用 `PUT /agent/v1/chunks/{hash}` 上传新扫描 chunk。
- 首次备份请求数量约等于：

```text
ceil(unknown_chunks / max_chunk_check_batch)
+ ceil(missing_chunk_bytes / max_chunk_upload_batch_bytes)
```

而不是 `unknown_chunks + missing_chunks`。

- 旧 agent 兼容接口仍通过测试。
- manifest 提交缺失 chunk 时仍返回 `missing_chunks`，agent 能补传并重试。
- `go test ./...` 通过。

## 13. 与 Pebble Catalog 设计的关系

批量 chunk API 解决公网 RTT 和 HTTP 请求数问题；Pebble catalog 解决 agent 本地写入热点问题。两者应该配合，但可以分阶段独立落地。

推荐顺序：

1. 先落地批量 check/upload，立刻降低公网延迟影响。
2. 再把批量 check/upload 的结果批量写入 catalog。
3. 最后将 catalog 写热点迁到 Pebble。

如果先迁 Pebble 但仍逐 chunk GET/PUT，公网延迟问题不会消失。如果先做批量 API，即使 catalog 仍用 SQLite，也能显著减少跨公网请求数量。
