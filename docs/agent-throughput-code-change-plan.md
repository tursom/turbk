# Agent 备份吞吐后续代码改造计划

日期：2026-06-24

状态：设计草案。

本文只整理“需要改代码”的备份吞吐提升方案。纯配置调优不纳入本文，例如开启 `chunk_pipeline_enabled`、调大 `max_chunk_*`、开启 `small_file_pack_enabled`、调整 `repository.chunk_avg_size` 等。

相关设计：

- [Agent Chunk 批量接口设计](agent-batch-chunk-api-design.md)
- [Agent 高延迟网络吞吐优化设计](agent-high-latency-pipeline-design.md)
- [Agent 小文件备份优化设计](agent-small-file-optimization-design.md)

## 1. 当前基线

当前 Agent 数据面已经具备以下能力：

- 批量 chunk check 和批量 chunk upload。
- compact chunk check/upload response。
- HTTP transport 显式连接池和 HTTP/2 尝试。
- 可灰度开启的 chunk pipeline。
- Agent 侧 `max_chunk_pipeline_bytes` 字节窗口。
- Small-file pack。
- 服务端 `HasChunks()` 内部批量查询入口。

后续瓶颈主要会从 RTT 等待转向：

- 公网错误下 batch 重试能力不足。
- pipeline 开大后服务端 upload in-flight 内存放大。
- 多个 upload handler 最终仍在 repository 写入锁处排队。
- 扫描、读文件、chunking 仍偏串行。
- repository/index batch 查询接口存在，但底层仍可继续优化。

## 2. 设计原则

- 优先保留现有 HTTP batch 协议，避免重新设计数据面 RPC。
- 不改变 snapshot manifest schema。
- 不改变 repository chunk/segment 格式。
- 不让单 Agent 同时运行多个备份任务。
- 所有新增并发必须有字节、请求数或 worker 数上限。
- 服务端压力过高时明确返回 429/503，Agent 必须按退避重试。
- 先做单写入队列合并，不优先拆 repository 写入锁。
- 每个阶段都必须能通过配置回滚到当前路径。

## 3. P0：Agent batch retry 和 413 split

### 3.1 背景

Pipeline 提高了 in-flight batch 数，但公网链路更容易出现临时失败。当前失败处理更接近“失败即停”，这会降低公网有效完成率。吞吐优化不能只看理想路径，还要减少高 RTT 网络中的整 run 重试。

### 3.2 改造范围

Agent 侧：

- `checkChunks()` 和 `uploadChunksBatch()` 增加可重试调用包装。
- 对单个 batch 增加 retry state，记录 `batch_id`、attempt、backoff、last_error。
- 对 HTTP 429、HTTP 5xx、网络临时错误、response header timeout 进行指数退避。
- 读取 `Retry-After`，服务端给出时优先使用。
- 对 HTTP 413 或 `MaxBytesError` 触发 batch split。
- 达到最大重试次数后取消当前 run。

服务端侧：

- 保持 batch upload 幂等。
- 对明确超限场景返回 413。
- 对压力限流场景返回 429 或 503，并尽量带 `Retry-After`。

### 3.3 新增配置

建议新增：

```yaml
agent:
  chunk_batch_max_retries: 5
  chunk_batch_retry_initial_backoff: "500ms"
  chunk_batch_retry_max_backoff: "30s"
  chunk_batch_split_on_413: true
```

### 3.4 验收标准

- Mock server 前两次返回 500，第三次成功，Agent run 成功。
- Mock server 返回 429 + `Retry-After`，Agent 按等待后重试。
- Mock server 返回 413，Agent 自动拆分 batch 并完成上传。
- 不可接受响应仍失败 run：未知 hash、遗漏 chunk、repository_id 不匹配。
- 日志包含 `batch_id`、attempt、backoff、split_count。

## 4. P0：服务端 upload in-flight 限流

### 4.1 背景

Pipeline 开启后，多个 upload request 可以同时到达服务端。当前 handler 会先读取完整 batch body，再调用 `repo.PutChunks()`。如果多个 Agent 或高 in-flight 配置同时运行，服务端内存会被 upload body 放大。

### 4.2 改造范围

服务端新增 upload admission control：

- 限制单 Agent upload in-flight request 数。
- 限制单 Agent upload in-flight bytes。
- 限制全局 upload in-flight bytes。
- 进入 `readAgentChunkBatch()` 前先按 `Content-Length` 预占窗口。
- 请求结束后释放窗口。
- 无 `Content-Length` 或超过可用窗口时拒绝。

### 4.3 新增配置

建议新增：

```yaml
agent:
  max_chunk_upload_inflight_per_agent: 2
  max_chunk_upload_inflight_bytes_per_agent: "256MiB"
  max_chunk_upload_inflight_bytes_global: "1GiB"
  chunk_upload_retry_after: "2s"
```

### 4.4 响应语义

- 单请求超过 `max_chunk_upload_batch_bytes`：返回 413。
- admission control 暂时无可用窗口：返回 429 或 503。
- 压力型拒绝应带 `Retry-After`。

### 4.5 验收标准

- 单 Agent 超过 request 并发上限时，后续 upload 被限流。
- 全局 bytes 窗口耗尽时，不继续读取 request body。
- handler 正常、错误、客户端断开路径都能释放窗口。
- Agent 能根据 429/503 retry 后成功。

## 5. P1：Repository 单写入队列合并

### 5.1 背景

即使 upload request 并发到达，最终仍会在 repository 写入锁处排队。直接拆 repository 锁风险高，因为 segment writer、index 更新、compact/maintenance gate 都依赖当前一致性模型。

低风险方向是引入单写入队列：

```text
upload handler(s)
  -> repository write queue
  -> single writer goroutine
  -> repo.PutChunks(coalesced batch)
  -> per request result channel
```

### 5.2 改造范围

Repository 或 HTTP 层新增 write coordinator：

- upload handler 将待写 chunks 和 result channel 提交到队列。
- writer goroutine 在短时间窗口内合并多个请求。
- 合并后的 chunk slice 调用一次 `repo.PutChunks()`。
- writer 按原请求拆回结果。
- 队列长度和 queued bytes 必须有上限。
- run gate、compact/maintenance 仍保持现有互斥语义。

### 5.3 新增配置

建议新增：

```yaml
agent:
  repo_write_queue_enabled: false
  repo_write_queue_max_requests: 64
  repo_write_queue_max_bytes: "512MiB"
  repo_write_coalesce_window: "5ms"
  repo_write_coalesce_max_bytes: "128MiB"
```

### 5.4 关键约束

- 不改变 `repo.PutChunks()` 的幂等语义。
- 同一个 upload request 的响应必须只包含该 request 的 chunks。
- writer goroutine 退出时必须让等待中的 request 返回明确错误。
- 队列满时返回 429/503，而不是无限堆积。
- 不能绕过 compact/maintenance 写入 gate。

### 5.5 验收标准

- 多个并发 upload request 被合并成更少的 `repo.PutChunks()` 调用。
- 重复 chunk 在合并批内仍只写一次，重复方得到 existed 语义。
- 队列满时 handler 返回限流错误，Agent retry 后可成功。
- compact/maintenance 运行时仍拒绝写入或等待既有 gate。
- race test 覆盖 write coordinator。

## 6. P1：Upload handler 子批次写入和内存降低

### 6.1 背景

当前 batch upload body 读取完成后一次性进入 `repo.PutChunks()`。当单 batch 很大时，内存峰值和一次写入锁持有时间都会变大。

### 6.2 改造范围

第一阶段：

- 保持现有 wire format。
- `readAgentChunkBatch()` 完成校验后，按 `repo_write_sub_batch_bytes` 切分。
- 多个 sub-batch 顺序调用 repository 写入。
- 汇总每个 chunk 的 response。

第二阶段：

- 评估 `readAgentChunkBatch()` 边读边校验。
- 对已读 chunk 逐步进入 write queue。
- 需要解决 response 必须覆盖完整 request 的结果收集问题。

### 6.3 新增配置

建议新增：

```yaml
agent:
  repo_write_sub_batch_bytes: "64MiB"
```

### 6.4 验收标准

- 一个超大 upload request 可以拆成多个 repository write sub-batch。
- 响应仍完整覆盖原始 request。
- 任一 sub-batch 失败时 request 返回失败，不能返回部分成功。
- 内存峰值低于未拆分路径。

## 7. P2：Agent 扫描、读文件和 chunking 并行化

### 7.1 背景

Pipeline 解决的是网络等待，但本地扫描、读文件和 chunking 仍可能成为瓶颈。SSD、本地高速盘或高带宽链路下，单线程 WalkDir + 读文件可能无法持续喂满 pipeline。

### 7.2 建议模型

```text
walker
  -> file job queue
  -> read/chunk workers
  -> chunk pipeline submitter
  -> manifest assembler
```

### 7.3 改造范围

Agent 侧：

- WalkDir 只负责发现文件和目录，生成 file jobs。
- 多个 read/chunk worker 并行读取 regular file。
- small-file pack 需要单独处理：可以先保持 pack batcher 单线程，普通大文件并行。
- 每个 file entry 维护 expected chunk count 和 chunk refs。
- manifest 最终按稳定路径排序输出。
- 对源端 IO 增加 worker 数和 bytes 窗口限制。

### 7.4 新增配置

建议新增：

```yaml
agent:
  scan_parallel_enabled: false
  file_read_workers: 2
  file_read_pipeline_bytes: "512MiB"
```

### 7.5 风险

- 机械盘或网络盘上可能因随机读变慢。
- manifest 顺序不能受 worker 完成顺序影响。
- 同一路径的错误要能定位到具体 file job。
- 与 small-file pack 复用 catalog 的交互要保持确定性。

### 7.6 验收标准

- 并行模式和串行模式 manifest 内容等价。
- 注入慢网络时，扫描不会无限堆积 file data。
- 注入慢磁盘时，可通过配置退回串行或低 worker。
- 多 root 场景下 manifest path 稳定。

## 8. P2：Repository/index batch lookup 深化

### 8.1 背景

服务端已经有 `HasChunks()` 入口，但底层仍可继续优化。真正的 batch lookup 应进入 index 层，减少重复 map/build 和 Pebble 调用开销，并为后续 range/prefix 优化留接口。

### 8.2 改造范围

- `chunkIndex` 增加 `GetBatch()` 或 `HasBatch()`。
- Repository `HasChunks()` 调用 index batch 方法。
- 对重复 hash 在 index 层去重。
- 保持返回顺序由 HTTP handler 控制。

### 8.3 验收标准

- check request 内重复 hash 只查一次 index。
- 大批量 check 的 CPU 和分配低于逐个 `Get()`。
- compact response 下仍能正确推导 exists/missing。

## 9. P2：指标和压测工具

### 9.1 背景

后续改造会涉及多个并发窗口。没有指标时，很难判断瓶颈是在 Agent 扫描、网络、服务端 admission control、write queue 还是 repository 写入锁。

### 9.2 改造范围

Agent 指标：

- check/upload request 数、in-flight、duration。
- pipeline bytes、wait seconds。
- retry count、split count。
- read/chunk worker queue depth。

服务端指标：

- upload in-flight requests/bytes。
- write queue depth/bytes。
- coalesced batch size。
- repo put duration。
- write gate wait duration。

压测工具：

- 本地生成 N 个固定大小文件。
- 可配置 RTT、丢包、HTTP 500/429/413 注入。
- 输出吞吐、请求数、峰值内存、manifest 等价性。

### 9.3 验收标准

- 能用同一条命令对比串行、pipeline、write queue、parallel scan。
- 高 RTT mock 测试给出可重复的吞吐差异。
- 指标能定位当前瓶颈层。

## 10. 不建议优先做

### 10.1 WebSocket 数据面

WebSocket 数据面需要自定义异步 request/response、断线恢复、幂等上传和流控窗口。复杂度接近重新设计 RPC。除非 HTTP batch + pipeline + write queue 实测仍不足，否则不建议优先投入。

### 10.2 直接拆 repository 写入锁

Repository 写入锁保护 segment writer、index 更新和 maintenance 一致性。直接拆锁容易引入重复写入、index/segment 不一致或 compact 冲突。应先做单写入队列合并，再评估是否需要更激进的 shard writer。

### 10.3 多备份任务并发

单 Agent 同时运行多个备份任务会放大源端 IO、内存和服务端写入压力，也会复杂化 daemon command 语义。吞吐优化应先保证单 run 能持续填满链路。

## 11. 推荐落地顺序

1. Agent retry/backoff 和 413 split。
2. 服务端 upload in-flight request/bytes 限流。
3. Repository 单写入队列合并。
4. Upload handler 子批次写入。
5. Agent 扫描、读文件和 chunking 并行化。
6. Repository/index batch lookup 深化。
7. 指标和压测工具补齐。

第 1 和第 2 是 pipeline 开启后的稳定性底座。第 3 是服务端吞吐的主要后续收益点。第 5 只有在本地扫描/读文件成为瓶颈时再做。
