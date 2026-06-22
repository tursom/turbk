# Agent Pebble Catalog 写入优化设计

日期：2026-06-22

状态：阶段一和阶段二已落地。

本文定义 Turbk Agent 使用 Pebble 优化本地 catalog 写入的方案。该设计基于当前 daemon catalog、服务端 command 触发、多目录备份、chunk generation 同步和 manifest canonicalize 机制。

相关设计：

- [Agent Chunk 批量接口设计](agent-batch-chunk-api-design.md)
- [Agent 常驻与本地索引设计](agent-daemon-design.md)

## 1. 背景

当前 agent daemon 使用本地 SQLite catalog 保存扫描与服务端确认状态。备份运行时，agent 会频繁写入：

- `files`：每个目录、symlink、文件的元数据。
- `file_chunks`：每个文件对应的 chunk hash 列表。
- `server_chunks`：每个 chunk 在服务端的确认状态、generation、最近检查时间。
- `agent_runs`：本地 run 开始和结束状态。
- `server_state`：服务端 repository/generation/command 水位。

SQLite 当前开启 WAL。大量文件和 chunk 场景下，`catalog.db-wal` 会持续写入。首次备份、源目录大量小文件、chunk 数量很多、服务端 chunk check 结果批量返回时，agent 本地磁盘写入会非常明显。

本地 catalog 不是权威数据源。它的目标是减少重复读取源文件、重复询问服务端和重复上传 chunk。catalog 丢失或部分落后时，agent 必须能通过重新扫描和服务端校验恢复正确性。

## 2. 问题

当前写入热点主要来自两类模式：

1. 逐文件重写。
   `replaceFile` 每次对文件执行 `files` upsert，然后删除旧 `file_chunks`，再按 chunk 数逐条插入新的 `file_chunks`。一个大文件会产生多条 SQL 写入；大量小文件会产生大量事务。

2. 逐 chunk 标记。
   `markChunk` 对每个 chunk 写 `server_chunks`。首次备份或服务端 batch check 后，chunk 数量越多，本地写入越多。

SQLite 的关系模型适合查询表达清晰的数据，但当前 agent catalog 的热点访问更接近 key-value：

- 按 `hash` 查询或更新 chunk 状态。
- 按 `(rootID, path)` 查询文件元数据和该文件的 chunk 列表。
- 扫描期间追加/覆盖大量独立 key。

Pebble 更适合这些写多读多的 KV 访问，但 LSM compaction 也会产生写放大。因此优化重点不是“把 SQLite 换成 Pebble”，而是：

- 减少事务次数。
- 合并一文件多行写入。
- 批量写 chunk 状态。
- 对非权威 cache 使用较弱 fsync 策略。

## 3. 目标

- 降低 agent 备份期间 `state_dir` 的前台写入延迟和随机写压力。
- 减少 SQLite WAL 增长，避免 SQLite 成为大目录备份的主要本地写入热点。
- 保持 catalog 非权威、可删除、可重建的性质。
- 保持现有备份正确性：服务端仍是 snapshot、manifest、chunk index 和 segment record 的最终真相。
- 允许灰度和回滚：新版本可以从旧 SQLite catalog 旁路重建 Pebble catalog。
- 分阶段落地，先迁移最热路径，避免一次性重写 daemon catalog。

## 4. 非目标

- 不把 agent 本地 catalog 变成权威数据。
- 不在 agent 本地保存 chunk 内容。
- 不改变服务端 manifest canonicalize 和缺失 chunk 校验。
- 不改变 agent 单任务执行策略。
- 不要求无损迁移旧 SQLite catalog；允许首次运行后逐步重建 cache。
- 不要求首版完全移除 SQLite。

## 5. 总体方案

采用 hybrid catalog：

- SQLite 暂时保留小表和低频状态：
  - `server_state`
  - `agent_runs`
  - `agent.lock` 仍使用当前 lock file
- Pebble 承担高频写热点：
  - 第一阶段：`server_chunks`
  - 第二阶段：`files` + `file_chunks`

目录结构：

```text
/var/lib/turbk-agent
  agent.lock
  catalog.db
  catalog.db-wal
  catalog.db-shm
  catalog.pebble/
    MANIFEST-...
    OPTIONS-...
    *.sst
    *.log
```

如果 `catalog.pebble/` 不存在，agent 创建空 Pebble catalog。旧 SQLite 数据可以保留，不做阻塞迁移。Pebble miss 时按现有逻辑重新扫描、重新 check 或重新上传。

## 6. Pebble 编码约定

Pebble key 首版就使用二进制编码，不使用 `chunk:<hash>` 这类文本 key。原因：

- hash、时间、generation 都是固定宽度或可变整数，二进制 key 更短。
- prefix scan 可以通过单字节类型前缀完成，边界清晰。
- 不需要处理路径分隔符、转义和字符串 split 的歧义。
- 后续 value 切换格式时，key 不需要迁移。

### 6.1 Key 通用格式

所有 key 以 1 字节 record type 开头：

```text
0x01 chunk status
0x02 file record
0x03 server state   // reserved, 首版仍可留在 SQLite
0x04 agent run      // reserved, 首版仍可留在 SQLite
```

多字节整数统一使用 big-endian。这样 key 字节序和数值序一致，方便范围扫描。

字符串使用：

```text
uvarint byte_length + raw UTF-8 bytes
```

路径在进入 key 前必须使用现有规范化逻辑，不在 key 编码层做路径清理。

### 6.2 Value 通用格式

value 也使用二进制编码：

```text
1 byte value version
payload fields...
```

首版 value version 为 `0x01`。字段顺序固定，变长字段使用 uvarint length。时间统一保存 Unix nano 或 Unix seconds，具体按字段精度需要决定。

不使用 JSON 作为 Pebble 存储格式。需要排查时另写调试 dump 工具，把二进制 value 解码后输出 JSON。

## 7. 阶段一：迁移 `server_chunks`

### 7.1 数据模型

Key：

```text
0x01 | 32 bytes blake3 hash
```

说明：

- hash 在业务层已有 hex 字符串，写入 Pebble 前 decode 成 32 字节。
- key 固定 33 字节，适合高频 Get/Set。
- `chunk:` prefix scan 对应范围为 `[0x01]` 到 `[0x02)`。

Value：

```text
u8   version = 1
u8   status              // 1 unknown, 2 confirmed, 3 missing
u64  original_size       // big-endian
u64  confirmed_generation// big-endian, 0 means unset
i64  last_checked_unix   // big-endian, 0 means unset
i64  last_uploaded_unix  // big-endian, 0 means unset
```

字段说明：

- `original_size`：原始 chunk 大小。
- `status`：`confirmed`、`missing`、`unknown`。
- `confirmed_generation`：确认该 chunk 存在时对应的服务端 chunk generation。
- `last_checked_unix`：最近一次向服务端确认的时间。
- `last_uploaded_unix`：最近一次上传成功的时间。

因为 catalog 可重建，value 解码失败时可以把该 key 视为 miss 或删除后重建，不需要阻塞备份。

### 7.2 接口变化

新增内部类型：

```go
type agentChunkCatalog interface {
    ChunkStatus(hash string) (status string, confirmedGeneration int64, ok bool, err error)
    MarkChunk(hash string, originalSize int64, status string, generation int64, uploaded bool) error
    MarkChunks(chunks []chunkStatusUpdate) error
    ApplyInvalidations(hashes []string, generation int64) error
}
```

当前函数映射：

- `chunkStatus` 读 Pebble。
- `markChunk` 写 Pebble。
- `ensureCatalogChunksConfirmed` 使用 `MarkChunks` 批量写服务端 check 结果。
- `applyInvalidations` 使用 Pebble batch 把指定 hash 标记为 `unknown`。
- repository id 变化时，用 Pebble prefix scan 将所有 `0x01` key 标记为 `unknown`，或直接删除 `catalog.pebble/` 后重建。

### 7.3 写入策略

`server_chunks` 是 cache，不是权威数据。首版策略：

- 单条写入：`db.Set(key, value, pebble.NoSync)`。
- 批量写入：`batch.Set(..., nil)` 后 `batch.Commit(pebble.NoSync)`。
- run 结束：调用一次 `db.Flush()`，尽量把 memtable 推进到稳定文件。
- 进程正常退出：`db.Close()`。
- 如果崩溃导致部分 chunk 状态丢失，下次运行会重新 check 或上传，不影响 correctness。

不建议对每个 chunk 使用 `pebble.Sync`。那会把 SQLite 的逐条 fsync 问题转移到 Pebble WAL。

### 7.4 预期收益

- 服务端 batch check 后的 `server_chunks` 更新从 N 次 SQLite exec 变成一次 Pebble batch commit。
- 首次备份上传大量 chunk 时，前台写入更顺序化。
- SQLite WAL 不再承载 chunk 级别热点写入。

## 8. 阶段二：迁移 `files` + `file_chunks`

### 8.1 数据模型

当前 SQLite 把一个文件拆成：

- `files` 一行元数据。
- `file_chunks` N 行 chunk 映射。

Pebble 中合并为一个 value。

Key 使用二进制长度前缀编码，避免路径中出现分隔符导致冲突：

```text
0x02 | uvarint len(rootID) | rootID bytes | uvarint len(path) | path bytes
```

实现时不要手写字符串 split 解析 key，提供统一函数：

```go
func encodeFileKey(rootID, path string) []byte
func decodeFileKey(key []byte) (rootID string, path string, ok bool)
```

Value：

```text
u8      version = 1
u8      entry_type            // 1 file, 2 dir, 3 symlink
u64     size                  // big-endian
u32     mode                  // big-endian
i64     uid                   // big-endian
i64     gid                   // big-endian
i64     mtime_ns              // big-endian
i64     dev                   // big-endian
i64     inode                 // big-endian
bytes   link_target           // uvarint length + bytes
bytes   chunk_fingerprint     // uvarint length + bytes
uvarint chunk_count
repeat chunk_count:
  [32]byte hash
  u64      original_size      // big-endian
```

目录和 symlink 的 `chunks` 为空。

字段必须按固定顺序编码。解码时如果遇到 version 不支持、字段截断或 trailing bytes 异常，视为 catalog miss，并允许后续扫描重建。

### 8.2 接口变化

新增内部类型：

```go
type agentFileCatalog interface {
    FileRecord(rootID, path string) (catalogFileRecord, []catalogChunkRecord, bool, error)
    ReplaceFile(record catalogFileRecord, chunks []catalogChunkRecord) error
    ReplaceFiles(updates []fileCatalogUpdate) error
}
```

当前函数映射：

- `fileRecord` + `fileChunks` 合并为一次 Pebble `Get`。
- `replaceFile` 变成一次 Pebble `Set`。
- 扫描时可以按 batch 缓冲多个 `ReplaceFile`，例如每 1000 个文件提交一次。

### 8.3 写入策略

- 变化文件：一次 `Set` 写完整 metadata + chunks。
- 未变化且成功复用：不写文件记录。
- 目录和 symlink：只有 metadata 变化时才写。
- batch 大小按文件数或 value 总字节数截断，例如：
  - 1000 个文件；
  - 或 16 MiB value 总量；
  - 或 2 秒 flush 一次。

### 8.4 预期收益

- 大文件不再产生 `DELETE file_chunks + INSERT N rows`。
- 小文件不再每个文件一个 SQLite transaction。
- 文件复用路径从两次 SQL 查询减少为一次 KV get。

## 9. 一致性与崩溃恢复

catalog 非权威，因此一致性目标是“不会产生错误快照”，不是“本地 cache 永不丢”。

允许的情况：

- 崩溃后丢失最近一部分 Pebble cache 更新。
- Pebble catalog 不完整导致下一次备份重新读取文件。
- chunk 状态丢失导致下一次重新请求服务端 check。

不允许的情况：

- catalog 误判服务端存在不存在的 chunk 并绕过服务端 manifest 校验。
- catalog 误用已变化文件的旧 chunk 列表。
- catalog 损坏导致 agent 无法启动且不能降级重建。

防线：

- 文件复用必须继续比较 size、mode、uid、gid、mtime、dev、inode、link target 等元数据。
- manifest 提交时服务端仍校验 chunk 存在，并返回 missing chunks 让 agent 补传。
- Pebble 打开失败时，agent 可以记录 warning，并回退到无 catalog 或 SQLite-only 模式。
- 提供手动删除 `catalog.pebble/` 的恢复路径。

## 10. 配置与回滚

新增环境变量：

```text
TURBK_AGENT_CATALOG_BACKEND=hybrid
```

取值：

- `sqlite`：保持旧 SQLite catalog。
- `hybrid`：SQLite 小表 + Pebble 写热点，推荐默认。
- `pebble`：后续全量 Pebble，首版不启用。

首版实现：

1. 默认使用 `hybrid`。
2. deploy 示例和 Web UI 生成配置显式写入 `TURBK_AGENT_CATALOG_BACKEND=hybrid`。
3. 若线上发现 Pebble catalog 问题，改回 `sqlite` 并删除 `catalog.pebble/` 即可回滚。

## 11. 迁移策略

不做阻塞式全量迁移。

阶段一上线后：

- 已存在的 SQLite `server_chunks` 不主动搬迁到 Pebble。
- Pebble miss 时按现有流程向服务端 check 或上传。
- 新确认的 chunk 写 Pebble。
- 旧 SQLite chunk 状态可以保留一段时间，后续版本再清理。

当前实现采用上述策略：`hybrid` backend 下 `server_chunks` 读写进入 `catalog.pebble/`，SQLite 仍保留表结构用于 `sqlite` 回滚模式和旧数据兼容。

阶段二上线后：

- 已存在的 SQLite `files/file_chunks` 不主动搬迁。
- 第一次扫描某个文件时，Pebble miss 会读取文件并写入 Pebble。
- 如果需要降低首次重新扫描成本，可以增加可选后台迁移命令，但不作为首版要求。

当前实现采用上述策略：`hybrid` backend 下 `files/file_chunks` 合并写入 `catalog.pebble/` 的 `0x02` record；SQLite 仍保留表结构用于 `sqlite` 回滚模式和旧数据兼容。

## 12. 测试计划

### 12.1 单元测试

- Pebble chunk catalog：
  - chunk key 固定 33 字节，hash hex 和二进制 hash 往返正确。
  - `MarkChunk` 后 `ChunkStatus` 可读。
  - `MarkChunks` 批量写 confirmed/missing。
  - invalidation 后状态变 unknown。
  - repository id 变化后 chunk cache 失效。

- Pebble file catalog：
  - `encodeFileKey` / `decodeFileKey` 对 rootID/path 往返正确。
  - 单目录文件 metadata + chunks 可读回。
  - 多目录 rootID 隔离。
  - symlink metadata 可读回。
  - 路径包含空格、冒号、中文、重复斜杠时 key 编码不冲突。
  - value version 不支持或 payload 截断时按 miss 处理。

### 12.2 集成测试

- 首次备份写入 Pebble catalog。
- 第二次备份复用未变化文件，不重新读取文件内容。
- 服务端 chunk generation 增加后，agent 应用 invalidation 并重新确认 stale chunk。
- 删除 `catalog.pebble/` 后，agent 退化为重新扫描，备份仍成功。
- `TURBK_AGENT_CATALOG_BACKEND=sqlite` 时旧测试仍通过。
- `TURBK_AGENT_CATALOG_BACKEND=hybrid` 时现有 agent daemon command 触发链路仍通过。

### 12.3 压测/观测

准备一个测试数据集：

- 10 万个小文件。
- 100 个大文件。
- 重复内容文件，用于验证 chunk 复用。

比较指标：

- 第一次备份总耗时。
- 第二次备份总耗时。
- agent `state_dir` 写入字节数。
- `catalog.db-wal` 增长量。
- `catalog.pebble/` 写入和 compaction 情况。
- 服务端 chunk check 请求数。
- 服务端 chunk upload 请求数。

网络请求数主要由批量 chunk API 优化，Pebble catalog 的职责是降低本地 catalog 写入热点。压测时两类指标要分开观察。

## 13. 验收标准

阶段一完成标准：

- `server_chunks` 热点写入走 Pebble。
- `go test ./...` 通过。
- 相同数据集下，SQLite WAL 增长明显下降。
- 首次备份期间本地写入延迟不高于旧实现。
- 删除 Pebble catalog 后下次备份仍成功。

阶段二完成标准：

- `files/file_chunks` 热点写入走 Pebble。
- 第二次备份能通过 Pebble file catalog 复用未变化文件。
- 大文件对应 chunk 列表不再以多行 SQL 方式重写。
- 大量小文件场景下，agent 本地写入耗时明显下降。

## 14. 实施顺序

1. 增加 catalog 写入统计日志，不改变存储 backend。
2. 引入 `agentChunkCatalog` 接口和 SQLite 适配器，保持行为不变。
3. 实现 Pebble chunk catalog。
4. 增加 `TURBK_AGENT_CATALOG_BACKEND`，支持 `sqlite` 和 `hybrid`。
5. 将 `server_chunks` 读写切到接口。
6. 补齐阶段一测试和压测脚本。
7. 引入 `agentFileCatalog` 接口和 SQLite 适配器。
8. 实现 Pebble file catalog。
9. 将 `files/file_chunks` 读写切到接口。
10. 更新 deploy 文档和 Web UI 生成的 agent env。

## 15. 风险与取舍

- Pebble compaction 会带来后台写入。需要通过压测确认总写入和前台延迟是否符合预期。
- 二进制 value 排查不如 JSON 直观，需要配套 dump/debug 工具。
- hybrid 模式会同时存在 SQLite 和 Pebble 两套本地状态，排查时需要明确哪些数据在哪个 backend。
- 使用 `pebble.NoSync` 可能丢失最近写入，但 catalog 可重建，正确性由服务端校验保证。
- 一次性全量迁移旧 SQLite cache 可能导致升级时长不可控，首版不做自动迁移。
- 如果网络层仍逐 chunk GET/PUT，Pebble 只能降低本地写入，不能解决公网 RTT 放大。因此公网场景应同时落地批量 chunk API。

## 16. 当前结论

当前实现已完成两个阶段：

- `server_chunks` 使用 Pebble `0x01` 二进制 key，并通过 batch + `NoSync` 写入。
- `files/file_chunks` 合并为 Pebble `0x02` 二进制 record，未变化文件复用路径一次读取 metadata 和 chunk 列表。
- `TURBK_AGENT_CATALOG_BACKEND=hybrid` 为默认 backend；`sqlite` 保留为回滚模式。

chunk 批量 check/upload 和 Pebble catalog 已一起落地：公网 RTT 由批量 API 摊薄，本地写入热点由 Pebble catalog 承接，避免网络请求批量化后又退化为本地逐 chunk/逐 chunk-row SQLite 写入。
