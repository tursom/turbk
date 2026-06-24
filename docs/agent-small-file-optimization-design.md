# Agent 小文件备份优化设计

日期：2026-06-23

状态：设计草案。

本文定义 Turbk Agent 在“海量小文件”场景下的三阶段优化方案。目标是降低 chunk 数量、HTTP 响应体积、manifest/catalog 元数据体积和服务端写入放大，同时保持现有内容寻址、去重、manifest canonicalize 和恢复能力。

相关设计：

- [Agent Chunk 批量接口设计](agent-batch-chunk-api-design.md)
- [Agent Pebble Catalog 写入优化设计](agent-pebble-catalog-design.md)
- [Agent 常驻与本地索引设计](agent-daemon-design.md)

## 1. 背景

当前 agent 使用内容定义分块，`agentChunkAvgSize = 1MiB`。这个值是大文件的平均 chunk 目标，不是空间预分配大小。小于 chunk 最小切分条件的文件会在 EOF 时按实际文件大小形成一个 chunk，不会补齐到 1MiB。

因此小文件场景的主要问题不是“每个小文件占 1MiB”，而是：

- 每个非空小文件至少产生一个 chunk。
- 每个 chunk 都会出现在 check/upload、catalog、manifest 和服务端 chunk index 中。
- 批量接口降低了公网 RTT，但大量 chunk 的 JSON 响应、manifest 提交和本地/服务端索引仍会被放大。
- 小文件内容本身可能很小，但每个 chunk 都有 segment record header、加密认证 tag、zstd frame、index value、manifest 引用和 catalog 记录等固定开销。

## 2. 问题

### 2.1 响应体积放大

`/agent/v1/chunks/upload` 当前对每个 chunk 返回完整结果，包括 `hash`、`uploaded` 和 `ref`。`ref` 包含 `segment_id`、`offset`、`length`、`original_size`、`compressed_size`、`created_at` 等字段。小文件越多，响应 JSON 越大，agent 端解码和内存压力越明显。

`/agent/v1/chunks/check` 当前返回 `exists` 和 `missing` 两个数组。对大批量请求而言，服务端把“存在”和“不存在”都显式返回，也会放大响应体。

### 2.2 chunk 数量放大

当前扫描以文件为边界调用 chunker。小文件不会跨文件合并，同一批中可以合并网络请求，但不能减少最终 chunk 数。10 万个小文件通常会产生接近 10 万个 chunk。

### 2.3 manifest 和 catalog 放大

每个文件要保存文件元数据，每个 chunk 要保存 hash 和 size。大量小文件时，即使内容总量不大，manifest 和 catalog 也会非常大。Pebble hybrid catalog 已经降低了本地写入热点，但不能减少记录数量本身。

### 2.4 restore 访问模式改变

如果把多个小文件打包成一个 pack chunk，单文件恢复不再是“直接按文件 chunk 顺序拼接”，而是要从 pack 数据中按 offset/length 切片。这个改动会影响 manifest schema、restore API 和维护任务。

## 3. 目标

- 降低海量小文件备份时的前台耗时和内存占用。
- 减少 agent/server 之间的 JSON 响应体积。
- 减少小文件场景下的 chunk index、manifest 和 catalog 记录数量。
- 保持服务端为最终真相：chunk index、segment record、manifest canonicalize 仍由服务端校验。
- 保持旧 agent 和旧 snapshot 可读。
- 分阶段落地，先做不改变 restore 模型的协议瘦身，再评估 small-file pack。

## 4. 非目标

- 不改变大文件的内容定义分块策略。
- 不用固定大小 tar 包替代现有 repository。
- 不让 agent 本地 catalog 成为权威数据源。
- 不要求 small-file pack 首版覆盖所有文件类型；目录、symlink、特殊文件仍按 manifest 元数据表达。
- 不在同一个变更中同时重写 restore、maintenance 和 Web UI。

## 5. 总体方案

分三阶段推进：

1. 短期：精简批量 check/upload 响应。
2. 中期：引入 small-file pack，减少 chunk 数量。
3. 配套：增加动态切批、压缩和可观测性，避免极端批次再次触发大响应或大内存问题。

## 6. 阶段一：精简批量响应

### 6.1 目标

不改变 manifest 和 restore 模型，只减少 agent/server 协议体积。这个阶段风险最低，应优先落地。

### 6.2 Upload 响应瘦身

当前响应：

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
        "segment_id": 1,
        "offset": 123,
        "length": 456,
        "original_size": 100,
        "compressed_size": 90,
        "created_at": "..."
      }
    }
  ]
}
```

优化后新增精简响应格式：

```json
{
  "status": "accepted",
  "repository_id": "repo_x",
  "chunk_generation": 13,
  "chunks": [
    {
      "hash": "...",
      "uploaded": true,
      "original_size": 100
    }
  ]
}
```

agent 在 manifest 中继续只需要 `hash + original_size`。服务端提交 manifest 时会按 chunk index canonicalize 为权威 `ChunkRef`，因此 upload 响应不需要把完整 `ref` 回传给新 agent。

兼容策略：

- 心跳新增能力位：`compact_chunk_upload_response=true`。
- 新 agent 默认请求精简响应。
- 旧 agent 不带能力位时，服务端继续返回完整 `ref`。
- agent 解码时兼容完整响应和精简响应。

### 6.3 Check 响应瘦身

当前响应：

```json
{
  "exists": ["hash-a", "hash-b"],
  "missing": ["hash-c"]
}
```

优化后支持只返回 `missing`：

```json
{
  "missing": ["hash-c"]
}
```

agent 根据请求 hash 列表推导：未出现在 `missing` 中的 hash 都视为 exists。

兼容策略：

- 心跳新增能力位：`compact_chunk_check_response=true`。
- 服务端只在新 agent 请求时启用精简 check。
- agent 校验逻辑必须确认 `missing` 是请求集合子集，不能接受未知 hash。

### 6.4 验收标准

- 新 agent 首次备份小文件目录时，upload/check 响应体明显小于旧格式。
- 新 agent 不依赖 upload 响应里的完整 `ref` 也能成功提交 manifest。
- 旧 agent 仍可使用完整响应。
- `missing_chunks` 修复路径继续可用。

## 7. 阶段二：Small-File Pack

### 7.1 目标

把多个小文件合并为少量 pack stream，再对 pack stream 分块上传，从根本上减少小文件产生的 chunk 数量。

### 7.2 判定规则

首版建议只打包普通小文件：

- 文件类型：regular file。
- 文件大小：`0 < size <= small_file_pack_max_file_size`，默认 `64KiB`。
- pack 目标大小：`small_file_pack_target_size`，默认 `8MiB` 或 `16MiB`。
- 文件元数据稳定：扫描时记录 size、mtime、mode、uid、gid、dev、inode。
- 排除已被 fsfilter 跳过的路径。

不进入 pack 的对象：

- 空文件。空文件不需要 chunk。
- 目录和 symlink。继续只写 manifest 元数据。
- 大文件。继续走当前 chunker。
- 扫描期间元数据变化的文件。回退普通文件路径或本轮跳过并报错。

### 7.3 Pack 格式

pack stream 使用二进制格式，便于服务端校验和 restore 切片：

```text
magic     "TBKPACK1\n"
u32       file_count
repeat file_count:
  u32     path_length
  bytes   path
  u64     original_size
  u64     data_offset
  u64     data_length
  u32     mode
  i64     mtime_ns
repeat file_count:
  bytes   file_data
```

pack 本身再交给现有 chunker 生成 repository chunks。pack 内容作为普通 chunk 数据上传，仍使用 BLAKE3、zstd、AES-GCM 和 segment append-only 写入。

### 7.4 Manifest 扩展

新增 manifest entry 表达 pack 内文件：

```json
{
  "path": "dir/a.txt",
  "type": "packed_file",
  "size": 1234,
  "mode": 420,
  "mod_time": "...",
  "pack": {
    "id": "pack-...",
    "offset": 1024,
    "length": 1234
  }
}
```

manifest 顶层新增 pack 列表：

```json
{
  "packs": [
    {
      "id": "pack-...",
      "format": "TBKPACK1",
      "chunks": [
        {"hash": "...", "original_size": 1048576}
      ]
    }
  ]
}
```

服务端 canonicalize manifest 时：

- 校验 pack chunks 存在。
- 把 pack chunks 改写为权威 `ChunkRef`。
- 校验每个 packed file 的 offset/length 不越界。
- 保持旧 `file` entry 的读取逻辑不变。

### 7.5 Restore 设计

恢复普通文件：

1. 按现有逻辑读取 entry chunks。
2. 拼接并写出文件内容。

恢复 packed file：

1. 找到 `pack.id` 对应 pack chunks。
2. 读取并拼接 pack stream。
3. 按 entry 的 `offset/length` 切片。
4. 写出文件内容并恢复 mode/mtime 等元数据。

优化项：

- 批量恢复同一 pack 内多个文件时，pack 只读取一次。
- 下载单个 packed file 时，可以先实现整 pack 读取，再后续优化为 range-aware 读取。

### 7.6 去重影响

small-file pack 会改变去重粒度：

- 当前模式：每个小文件独立 hash，重复小文件可按文件内容去重。
- pack 模式：多个文件组成 pack，pack 中任一文件变化可能影响 pack chunk 切分和去重。

首版应优先优化“海量小文件吞吐”，接受部分去重粒度变粗。后续可以按目录、文件扩展名或稳定排序降低 pack 改动范围。

### 7.7 兼容和迁移

- 旧 snapshot 不含 `packs`，按旧 restore 逻辑读取。
- 新 snapshot 含 `packs`，需要新服务端支持 restore。
- 不要求旧服务端接受 packed manifest；agent 必须通过 heartbeat 能力位确认服务端支持后才启用。
- 配置开关默认可灰度：

```yaml
agent:
  small_file_pack_enabled: false
  small_file_pack_max_file_size: "64KiB"
  small_file_pack_target_size: "8MiB"
```

### 7.8 验收标准

- 10 万个 4KiB 文件的 chunk 数显著下降。
- manifest 体积和 catalog file chunk refs 显著下降。
- 单文件下载、目录恢复、整 snapshot 恢复都支持 packed file。
- packed snapshot 经过 verify/compact 后仍可恢复。
- 关闭 pack 开关后行为回到当前模式。

## 8. 阶段三：动态切批、压缩和可观测性

### 8.1 动态切批

当前批次主要受 `max_chunk_check_batch` 和 `max_chunk_upload_batch_bytes` 约束。小文件场景下，单个 chunk 很小，可能在 chunk 数上限内产生很大的响应体。

新增响应体估算：

```text
estimated_upload_response_bytes =
  base_json_bytes + chunk_count * avg_upload_response_bytes
```

agent flush 条件增加：

- pending chunk 数达到上限。
- pending upload body 达到上限。
- 估算 upload 响应体达到上限。
- 估算 check 响应体达到上限。

服务端心跳下发：

```json
{
  "agent": {
    "max_chunk_check_batch": 10000,
    "max_chunk_upload_batch_bytes": 67108864,
    "max_chunk_response_bytes": 67108864
  }
}
```

### 8.2 HTTP 压缩

agent 请求头增加：

```http
Accept-Encoding: gzip
```

服务端对 JSON 响应启用 gzip，优先覆盖：

- `/agent/v1/chunks/check`
- `/agent/v1/chunks/upload`
- `/agent/v1/manifests`
- `/agent/v1/chunks/invalidations`

注意事项：

- Go 默认 transport 会自动处理 gzip；如果手动设置 `Accept-Encoding`，需要确认解压路径。
- 错误响应体仍保持可读，避免排查困难。
- 二进制 chunk upload body 不做 gzip；chunk 内容会在 repository 写入时 zstd 压缩。

### 8.3 可观测性

agent run 日志新增：

- `chunk_check_requests`
- `chunk_upload_requests`
- `chunk_upload_request_bytes`
- `chunk_upload_response_bytes`
- `manifest_bytes`
- `packed_files`
- `packed_bytes`
- `pack_count`

服务端日志或 metrics 新增：

- batch upload chunk count。
- batch upload request/response bytes。
- `repo.PutChunks` 写入耗时。
- segment sync 耗时。
- manifest canonicalize 耗时和 manifest bytes。

### 8.4 验收标准

- 极端小文件批次不会因为响应体过大导致 agent JSON decode 失败。
- 日志能定位瓶颈是扫描、上传、服务端写入、manifest 还是 restore。
- HTTP gzip 开启后，批量 JSON 响应网络传输体积明显下降。

## 9. 推荐落地顺序

1. 阶段一：精简 upload/check 响应。
   这是兼容性最好、回归面最小的优化，能直接降低大批量小 chunk 响应体。

2. 阶段三的一部分：动态响应体切批和 response bytes 日志。
   先把极端批次保护好，避免后续 pack 开发期间继续碰到大响应问题。

3. 阶段二：small-file pack。
   这是根治 chunk 数放大的方案，但涉及 manifest、restore、verify、compact 和 Web UI 展示，需要独立实现和审计。

4. 阶段三剩余部分：HTTP gzip 和更完整 metrics。
   作为协议和运维侧的持续优化。

## 10. 风险

- 精简响应如果和旧 agent 兼容处理不完整，可能导致旧 agent 解码失败。
- small-file pack 会降低小文件级别的跨 snapshot 去重效果。
- packed file restore 需要谨慎处理 offset/length 校验，避免错误切片。
- manifest schema 扩展必须保证旧 snapshot 可读。
- pack 读取如果一次加载整 pack，单文件下载延迟可能上升；需要后续 range-aware 优化。

## 11. 测试计划

### 11.1 单元测试

- 精简 upload 响应：agent 能解析无 `ref` 的响应。
- 精简 check 响应：agent 能从 `missing` 推导 exists。
- pack 编码/解码：路径、offset、length、数据校验。
- packed manifest 校验：越界 offset/length 被拒绝。

### 11.2 集成测试

- 10 万小文件首次备份。
- 第二次备份复用 catalog，不重新读取未变化小文件。
- 单个 packed file 下载。
- 整目录恢复，包含普通文件、packed file、空文件、目录、symlink。
- compact 后 packed snapshot 仍可恢复。
- 服务端不支持 pack 时 agent 自动回退当前模式。

### 11.3 性能测试

测试数据集：

- 10 万个 4KiB 文件。
- 1 万个 64KiB 文件。
- 混合目录：小文件、大文件、空文件、symlink。

对比指标：

- backup wall time。
- chunk 数量。
- manifest bytes。
- upload/check response bytes。
- repository segment bytes。
- agent catalog 写入量。
- restore wall time。

## 12. 待确认问题

- small-file pack 默认是否启用，还是仅作为高级配置灰度。
- pack 阈值默认使用 `64KiB` 还是 `128KiB`。
- pack 是否按目录边界聚合，还是跨目录全局聚合。
- packed file 是否需要保留文件级内容 hash，便于单文件校验和后续去重。
- Web UI snapshot tree 是否显示 packed 状态，还是保持对用户透明。
