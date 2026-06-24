# Agent 高延迟网络吞吐优化设计

日期：2026-06-24

状态：设计草案。

本文定义 Turbk Agent 在跨洲公网、高 RTT 网络下的数据面吞吐优化方案。结论是：WebSocket 可以作为控制面增强，但数据面首选 HTTP/2 + 有界并发批量流水线。真正提升吞吐的不是 WebSocket 连接形态本身，而是减少同步等待、增加 in-flight 数据、让扫描、check、upload 和 manifest 组装并行推进。

相关设计：

- [Agent Chunk 批量接口设计](agent-batch-chunk-api-design.md)
- [Agent 常驻与本地索引设计](agent-daemon-design.md)
- [Agent 小文件备份优化设计](agent-small-file-optimization-design.md)

## 1. 背景

当前 Agent 已经支持批量 chunk check 和批量 chunk upload：

```text
POST /agent/v1/chunks/check
POST /agent/v1/chunks/upload
```

这已经解决了“每个 chunk 一次 HTTP 请求”的主要 RTT 放大问题。但在跨洲网络中，RTT 可能稳定在 200ms 以上。当前批量 batcher 的 flush 路径仍是同步模型：

```text
scan/chunk -> fill batch -> check batch -> wait -> upload missing batch -> wait -> continue
```

如果一个 batch 中有 missing chunk，则一次 flush 至少需要两个网络往返：

```text
check:  1 RTT
upload: 1 RTT
```

在 200ms RTT 下，仅网络等待底座就是约 400ms。batch 越小、flush 越频繁，固定等待占比越高。即使服务端和本地磁盘都很快，Agent 也可能因为等待上一批响应而无法持续填满链路。

## 2. WebSocket 判断

WebSocket 可以支持长连接和双向消息，但它不是吞吐优化的充分条件。

适合使用 WebSocket 的场景：

- Agent daemon 控制面：服务端实时推送 `run-backup`、`cancel-run`、`refresh-config`。
- Agent 实时上报 progress 和 logs。
- 降低手动触发命令的响应延迟，避免等待 heartbeat poll。

不建议首选 WebSocket 承载大块数据上传的原因：

- 当前 Go `http.Client` 已经复用 TCP 连接，HTTP/2 也能在一条连接上多路复用请求。
- 如果 WebSocket 内仍然按 `check -> wait -> upload -> wait` 串行处理，吞吐提升很小。
- 单 WebSocket 仍是单 TCP 连接，遇到丢包会有 TCP head-of-line blocking。
- 大块二进制上传走 HTTP batch 更容易做超时、重试、限流、日志、网关代理和兼容回退。
- 服务端仓库写入当前由 repository 级 mutex 串行化，多路上传最终仍可能在写入层排队。

因此首版数据面优化不引入 WebSocket tunnel。只有当 HTTP/2 batch + 有界并发仍不能满足需求，且确实需要自定义双向异步协议时，再评估 WebSocket 数据面。

## 3. 目标

- 在 200ms+ RTT 的公网环境中提高有效上传吞吐。
- 减少 Agent 在 `check` 和 `upload` 响应上的空等时间。
- 允许多个 batch 同时 in-flight，但严格限制内存、网络和服务端压力。
- 保持现有 HTTP API、认证、manifest canonicalize 和 repository 内容寻址模型。
- 继续支持旧串行路径作为回退。
- 提供清晰的配置、指标和灰度开关，便于按部署环境调优。

## 4. 非目标

- 不在首版实现 WebSocket 数据上传协议。
- 不让一个 Agent 同时运行多个备份任务。
- 不改变 snapshot manifest schema。
- 不改变 repository chunk 格式、segment 格式和加密压缩流程。
- 不在首版拆分 repository 写入锁。
- 不要求服务端主动连接 Agent；Agent 仍主动发起出站连接。

## 5. 吞吐模型

一个串行 flush 的近似耗时：

```text
flush_time ~= check_rtt + upload_rtt + upload_transfer_time + server_write_time
```

如果 RTT = 200ms，且 batch 有 missing chunk，则：

```text
check_rtt + upload_rtt ~= 400ms
```

有效吞吐近似为：

```text
throughput ~= uploaded_bytes_per_flush / flush_time
```

当 batch 较小时，RTT 会主导吞吐。例如每批上传 8MiB，忽略服务端处理和传输时间，仅 400ms 等待就把上限压到约 20MiB/s。若链路带宽更高，就需要多个 batch 并发 in-flight 或更大的 batch 才能填满链路。

用带宽时延积估算窗口：

```text
BDP = bandwidth * RTT
```

示例：

- 1Gbps 链路、200ms RTT，BDP 约 25MiB。
- 如果 upload batch 是 8MiB，至少需要 4 个左右 upload in-flight 才可能填满链路。
- 如果 upload batch 是 64MiB，单个 upload 已能覆盖 BDP，但仍会受到串行 check/upload 等待影响。

因此优化方向是：

- 增大有效 batch。
- 让 check 和 upload 重叠。
- 让下一批扫描/chunking 在上一批网络请求等待时继续推进。
- 用有界窗口控制 in-flight 总字节数。

## 6. 总体方案

分四个阶段推进：

1. HTTP transport 和配置基础：确认连接复用、HTTP/2、多连接上限、超时和灰度开关。
2. Agent 有界并发流水线：扫描、check、upload、manifest 组装异步化。
3. 服务端批量查询和写入路径优化：减少逐 hash index 查询和内存放大。
4. 可选 WebSocket 控制面：降低命令和状态延迟，不承载大块数据上传。

首版重点是阶段一和阶段二。

## 7. 阶段一：HTTP Transport 和配置

### 7.1 Agent Transport

Agent 创建 HTTP client 时显式配置 transport：

```text
MaxIdleConns
MaxIdleConnsPerHost
MaxConnsPerHost
IdleConnTimeout
TLSHandshakeTimeout
ResponseHeaderTimeout
ExpectContinueTimeout
```

建议默认值：

```yaml
agent:
  max_chunk_check_inflight: 2
  max_chunk_upload_inflight: 2
  max_chunk_pipeline_bytes: "256MiB"
  max_chunk_upload_batch_bytes: "64MiB"
```

说明：

- `max_chunk_check_inflight` 控制同时进行的 check batch 数。
- `max_chunk_upload_inflight` 控制同时进行的 upload batch 数。
- `max_chunk_pipeline_bytes` 控制 Agent 内存中 pending 和 in-flight chunk 数据总量。
- `max_chunk_upload_batch_bytes` 继续作为单请求大小上限。

默认值应保守。高 RTT、高带宽部署可以手动调大。

### 7.2 服务端下发能力

heartbeat 响应中新增或复用 agent 配置字段：

```json
{
  "agent": {
    "chunk_batch_upload": true,
    "max_chunk_check_batch": 10000,
    "max_chunk_upload_batch_bytes": 67108864,
    "max_chunk_response_bytes": 67108864,
    "max_chunk_check_inflight": 2,
    "max_chunk_upload_inflight": 2,
    "max_chunk_pipeline_bytes": 268435456
  }
}
```

Agent 本地 flag/env 可以覆盖服务端默认值，但不能超过服务端硬上限。

### 7.3 验收标准

- Agent 在高 RTT 环境中可以维持多个 keep-alive 连接。
- HTTP/2 可用时同 host 请求可以多路复用。
- 调低 in-flight 配置后可退化为当前串行行为。
- 日志中能看到 check/upload in-flight、batch bytes、等待时长和吞吐。

## 8. 阶段二：Agent 有界并发流水线

### 8.1 当前问题

当前 batch flush 是同步调用：

```text
Flush()
  checkChunks(...)
  uploadChunksBatch(...)
  fill waiters
```

该模型简单可靠，但网络等待期间扫描和下一批 hash 收集无法持续推进。高 RTT 下，Agent 容易在每个 batch 边界停顿。

### 8.2 新流水线模型

建议引入 windowed batcher：

```text
scanner/chunker
  -> batch accumulator
  -> check queue
  -> check workers
  -> upload queue
  -> upload workers
  -> result applier
  -> manifest assembler
```

核心约束：

- 每个 batch 一旦提交到队列就不可变。
- 每个 batch 有 `batch_id`，用于日志、指标、错误定位。
- 每个 chunk hash 在全局 pending 表中去重；重复 hash 只追加 waiter。
- 每个文件 entry 维护 pending chunk 计数；所有 chunk resolved 后才能进入 manifest。
- manifest 输出顺序保持确定性，推荐按 root/path 排序或复用现有 manifest canonicalize 规则。
- 任一 worker 返回不可恢复错误时取消整个 run context，停止扫描并等待 in-flight 请求退出。

### 8.3 数据结构

Agent 内部新增概念：

```text
chunkPipeline
  pendingByHash map[hash]*pipelineChunk
  checkQueue    chan *pipelineBatch
  uploadQueue   chan *pipelineBatch
  results       chan pipelineResult
  bytesWindow   weighted semaphore
  errgroup      worker lifecycle
```

`pipelineChunk`：

```text
hash
data
original_size
waiters
state: queued | checking | uploading | confirmed | failed
```

`pipelineBatch`：

```text
id
chunks
request_bytes
created_at
```

`waiter` 继续表示某个 file entry 的某个 chunk ordinal，result applier 根据服务端结果填充 `repository.ChunkRef{Hash, OriginalSize}`。

### 8.4 Check 和 Upload 重叠

流水线允许如下并发：

```text
batch 1: check -> upload -> apply
batch 2:        check -> upload -> apply
batch 3:               check -> upload -> apply
scanner: continues while network waits
```

check worker 处理：

1. 调 `/agent/v1/chunks/check`。
2. 校验 `missing` 是请求集合子集。
3. 已存在 chunk 直接产生 confirmed result。
4. missing chunk 进入 upload queue。

upload worker 处理：

1. 调 `/agent/v1/chunks/upload`。
2. 校验响应只包含请求 chunk。
3. 写入本地 catalog confirmed 状态。
4. 产生 upload result。

### 8.5 内存控制

流水线必须以字节窗口为硬约束：

```text
in_memory_chunk_bytes <= max_chunk_pipeline_bytes
```

进入 pipeline 前按 chunk data 大小 acquire；result applied 后 release。

对 small-file pack 场景，窗口统计 pack chunk 数据。对普通大文件，窗口统计原始 chunk 数据。这样可以避免高 RTT 下 worker 堆积导致 Agent 内存失控。

### 8.6 错误和重试

可重试错误：

- 网络临时错误。
- HTTP 429。
- HTTP 5xx。
- response header timeout。

策略：

- 对单 batch 使用指数退避和抖动。
- 达到最大重试次数后取消 run。
- HTTP 413 或 `MaxBytesError` 时拆小 batch 重试。
- 连续 pipeline 错误超过阈值时降级为串行 flush，便于兼容代理和低资源环境。

不可接受的服务端响应：

- check 返回未知 hash。
- upload 响应遗漏请求 chunk。
- upload 响应 repository_id 不匹配。
- compact response 下无法推导完整请求结果。

这些错误必须失败当前 run，不能静默继续。

### 8.7 Catalog 一致性

本地 catalog 仍是缓存，不是权威数据源。

流水线中的 catalog 更新规则：

- 已确认存在的 hash 标记为 `confirmed`。
- 上传成功的 hash 标记为 `confirmed`。
- generation 使用服务端 check/upload 响应中的 `chunk_generation`，缺失时回退 heartbeat 下发值。
- catalog 写入可以批量化，但必须在 result applied 前或同时完成；失败时允许 run 失败，避免 manifest 引用本地状态不确定的 chunk。

### 8.8 Manifest 组装

异步结果会打乱 chunk 完成顺序，因此 manifest 组装必须独立于网络完成顺序。

建议：

- 每个文件 entry 初始化时记录 expected chunk 数量。
- 每个 chunk result 根据 waiter ordinal 填入固定位置。
- pending 计数归零后，文件 entry 进入 ready 列表。
- 最终 manifest 按稳定路径顺序输出。

这样可以保持重复备份的 manifest 可比性，并避免网络完成顺序影响 snapshot 内容。

### 8.9 验收标准

- 在人工注入 200ms RTT 的环境中，pipeline 模式吞吐明显高于串行 flush。
- `max_chunk_check_inflight=1`、`max_chunk_upload_inflight=1`、低窗口配置可退化为旧行为。
- 大量重复 chunk 只产生一次 check/upload，其他 waiter 正确复用结果。
- 任一 batch 失败后 run 可停止并返回明确错误。
- Agent 内存峰值受 `max_chunk_pipeline_bytes` 限制。
- manifest 内容与串行模式等价。

## 9. 阶段三：服务端批量查询和写入路径优化

### 9.1 批量 HasChunks

当前 check 接口对每个 hash 调一次 repository index 查询。建议新增 repository 内部方法：

```text
HasChunks(ctx, hashes []string) (exists map[string]struct{}, err error)
```

目标：

- 在 repository/index 层集中处理 batch。
- 减少重复 hash 查询。
- 为 Pebble prefix/range 或批量 get 优化留接口。

HTTP 协议不必变化，仍使用 `/agent/v1/chunks/check`。

### 9.2 Upload 内存放大

当前 upload handler 会先读取完整 batch 到内存，再调用 `repo.PutChunks()`。在 pipeline 模式下，多个 upload in-flight 会放大服务端内存。

短期控制：

- 服务端限制 `max_chunk_upload_inflight_per_agent`。
- 服务端限制全局 upload in-flight bytes。
- HTTP 429 或 503 返回 `Retry-After`，Agent 按退避重试。

中期优化：

- `readAgentChunkBatch` 支持边读边校验，减少临时复制。
- `repo.PutChunks()` 支持 streaming 或 sub-batch 写入。
- 对超大 batch 自动切成 repository 写入子批次。

### 9.3 Repository 写入锁

当前 repository 写入路径使用 repository 级 mutex 保证 index、segment writer 和维护任务一致性。首版不拆锁，因为：

- 内容寻址写入需要保持 segment writer 顺序。
- compact/maintenance 与备份写入已有 gate 协调。
- 多 Agent 并发上传时，盲目拆锁可能引入重复写入或 index/segment 不一致。

可后续评估：

- 单写入队列合并多个 Agent 的 pending writes。
- hash/index 查询与 segment append 分离锁。
- 多 segment writer 或 shard writer，但这会改变 repository 写入模型，风险更高。

## 10. 阶段四：WebSocket 控制面

WebSocket 控制面可以作为 daemon 模式增强，不作为数据面首选。

建议路径：

```text
GET /agent/v1/connect
Upgrade: websocket
```

认证仍使用 Agent Client ID / Secret，连接建立后服务端发送控制消息：

```json
{
  "type": "command",
  "command": {
    "id": 123,
    "type": "run-backup"
  }
}
```

Agent 上报：

```json
{
  "type": "progress",
  "run_id": 456,
  "scanned_files": 1000,
  "uploaded_chunks": 200
}
```

保留 heartbeat poll 作为兜底：

- WebSocket 断开时 Agent 回退 poll。
- 代理或防火墙不支持 WebSocket 时不影响备份。
- 服务端重启后 Agent 自动重连。

控制面验收标准：

- 手动触发命令延迟从 poll interval 降到秒级。
- WebSocket 断开不影响正在进行的数据上传。
- 同一 Agent 只能有一个有效控制连接；旧连接被替换或关闭。

## 11. 不推荐的 WebSocket 数据面首版

如果未来确实需要 WebSocket 数据面，必须是异步 request/response 协议，而不是把 HTTP 请求简单包进 WebSocket。

最低要求：

```json
{
  "id": "req-1",
  "type": "chunk.check",
  "hashes": ["..."]
}
```

```text
binary frame:
  request_id
  message_type = chunk.upload
  chunk_batch_body
```

服务端响应必须带 request id，允许乱序返回。Agent 必须有流控窗口、重试、断线恢复、幂等上传和协议版本协商。

这套协议复杂度接近重新设计一层 RPC。除非 HTTP/2 batch 在实测中无法满足目标，否则不建议首版投入。

## 12. 指标和可观测性

Agent 指标：

- `chunk_check_requests`
- `chunk_upload_requests`
- `chunk_check_inflight`
- `chunk_upload_inflight`
- `chunk_pipeline_bytes`
- `chunk_pipeline_wait_seconds`
- `chunk_check_rtt_seconds`
- `chunk_upload_rtt_seconds`
- `chunk_upload_bytes_per_second`
- `chunk_batch_retry_count`
- `chunk_batch_split_count`

服务端指标：

- `agent_chunk_check_requests`
- `agent_chunk_check_hashes`
- `agent_chunk_upload_requests`
- `agent_chunk_upload_bytes`
- `agent_chunk_upload_inflight`
- `agent_chunk_upload_inflight_bytes`
- `repo_put_chunks_duration`
- `repo_put_chunks_pending_writes`
- `repo_write_lock_wait_seconds`

日志需要包含：

- agent id / host id
- run id
- batch id
- hashes count
- request bytes
- response bytes
- check/upload duration
- retry count
- compact response flag

## 13. 灰度和回滚

新增配置默认保守：

```yaml
agent:
  chunk_pipeline_enabled: false
  max_chunk_check_inflight: 1
  max_chunk_upload_inflight: 1
  max_chunk_pipeline_bytes: "128MiB"
```

灰度步骤：

1. 在测试环境开启 pipeline，使用 `tc netem` 注入 200ms RTT 和少量丢包。
2. 在单 Agent 上开启，观察吞吐、内存、服务端 repo 写入耗时。
3. 扩大到少量高 RTT Agent。
4. 根据服务端 CPU、内存、磁盘和 repository lock wait 调整 in-flight 默认值。

回滚方式：

- 服务端下发 `chunk_pipeline_enabled=false`。
- Agent 本地设置 `TURBK_AGENT_CHUNK_PIPELINE_ENABLED=false`。
- 保留现有串行 batcher 代码路径，直到 pipeline 在生产稳定一段时间。

## 14. 测试计划

单元测试：

- pipeline batch 去重。
- waiter 按 ordinal 填充。
- check 响应校验。
- upload 响应校验。
- 失败取消和 in-flight drain。
- 字节窗口 acquire/release。

集成测试：

- mock server 注入 200ms 延迟，比较串行与 pipeline 的总耗时。
- mock server 乱序返回 batch 结果，manifest 仍稳定。
- mock server 返回 429/5xx，Agent 重试后成功。
- mock server 返回 413，Agent 拆批重试。
- pipeline 内存窗口达到上限时 scanner 阻塞，不继续堆积 chunk data。

性能测试：

- 大文件场景：少量大 chunk，验证吞吐和内存。
- 小文件 pack 场景：大量小文件，验证 pack 后 batch 数和吞吐。
- 高重复率场景：验证本地 catalog 和 pendingByHash 去重收益。
- 多 Agent 并发：验证服务端 upload in-flight bytes 和 repository lock wait。

## 15. 落地顺序

建议按以下顺序实现：

1. 增加 transport 配置和基础指标。
2. 增加服务端下发 pipeline 配置字段，默认关闭。
3. 实现 Agent windowed pipeline，但保留串行 batcher。
4. 增加高 RTT mock 集成测试。
5. 实现服务端 `HasChunks()` 内部批量查询。
6. 增加服务端 upload in-flight bytes 限流。
7. 小范围灰度 pipeline。
8. 评估 WebSocket 控制面需求。

## 16. 结论

在 200ms+ RTT 场景下，提升吞吐的关键是有界并发和流水线化，而不是 WebSocket 本身。数据面优先沿用现有 HTTP batch 协议，补齐 HTTP/2、in-flight batch、字节窗口和服务端批量查询。WebSocket 更适合控制面，用来降低命令触发和状态更新延迟。
