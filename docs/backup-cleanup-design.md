# 备份删除与清理设计

日期：2026-06-21

本文定义 Turbk 的备份清理、手动删除和错误数据自动回收设计。该设计基于当前已有的 snapshot、run、manifest、chunk index、append-only segment 和 `retention / verify / compact` 维护模式。

## 1. 目标

- 支持管理员手动删除备份恢复点。
- 支持按保留策略自动清理过期备份。
- 支持自动清理失败备份、异常中断 run 和未被任何有效 snapshot 引用的数据。
- 自动 compact 默认启用，并提供默认运行周期。
- 保持对主机管理 SMR 磁盘友好：常规备份路径只追加写 segment，空间回收放到维护窗口内顺序重写。
- 删除动作最终能够通过垃圾回收正确释放磁盘空间。

## 2. 非目标

- 不在手动删除 snapshot 时立即原地改写或截断 segment。
- 不自动删除已经发布但校验失败的 snapshot。
- 不提供任意文件级删除。删除对象是完整 snapshot。
- 不把恢复目录里的用户文件纳入备份仓库 GC 范围。

## 3. 概念边界

### 3.1 Snapshot 删除

snapshot 是用户可见的备份恢复点。删除 snapshot 的语义是：

- 设置 `snapshots.deleted_at`。
- 从 snapshot 列表、恢复入口、最新快照查询中移除。
- 不立即删除 manifest 文件、chunk index 或 segment 文件。
- 该 snapshot 不再参与后续增量基线选择。

删除 snapshot 后，实际磁盘空间由后续垃圾回收和 compact 回收。

### 3.2 垃圾回收

垃圾回收负责删除不再被 active snapshot 引用的数据。GC 只以 active snapshot 的 manifest 作为存活根。

存活根：

- `snapshots.deleted_at IS NULL` 的 snapshot。
- 这些 snapshot 指向的 manifest。
- 这些 manifest 中引用的 chunk hash。
- chunk index 中这些 hash 指向的 segment record。

可回收对象：

- deleted snapshot 独占引用的 chunk。
- failed/canceled/stale run 产生但没有发布 snapshot 的 orphan chunk。
- 不被任何 active snapshot 引用的 manifest 文件。
- 不被任何 active snapshot 引用的 chunk index 条目。
- compact 后不再包含 active chunk 的旧 segment 文件。

### 3.3 错误备份数据

错误备份数据指没有形成有效恢复点，或已经无法继续完成的临时数据：

- `failed` run 产生的数据。
- `canceled` run 产生的数据。
- 超过阈值仍处于 `pending` 或 `running` 的 stale run。
- run 已结束但没有对应 snapshot 的 manifest。
- chunk index 中未被 active snapshot 引用的 chunk。

已经发布的 snapshot 即使校验失败，也不由自动清理直接删除。系统只标记和报告，需要管理员确认后手动删除。

## 4. 默认维护策略

新增配置块：

```yaml
maintenance:
  enabled: true
  timezone: "Asia/Shanghai"

  # 每天清理过期 snapshot 和错误备份元数据。
  cleanup_schedule: "0 3 * * *"

  # 默认启用 compact，每周日凌晨执行一次。
  compact_enabled: true
  compact_schedule: "30 3 * * 0"

  # failed/canceled/stale run 结束或判定异常后，等待多久才允许 GC。
  error_grace_period: "24h"

  # pending/running 超过该时间且服务重启后仍未推进，视为 stale run。
  stale_run_after: "6h"

  # 软删除 snapshot 元数据保留时间。0 表示永久保留删除记录。
  keep_deleted_metadata_days: 30

  # 自动 compact 的跳过阈值，避免收益很小时重写大量数据。
  compact_min_reclaim_ratio: 0.15
  compact_min_reclaim_bytes: "1GiB"
```

默认行为：

- `cleanup_schedule` 每天执行 `retention + cleanup-errors`。
- `compact_schedule` 每周执行 `retention + cleanup-errors + compact`。
- compact 默认启用，但如果存在 active run、备份写入 gate 被占用，或预计回收收益低于阈值，则跳过并记录原因。
- 手动触发 compact 不受收益阈值限制，但仍受 active run 和写入 gate 保护。

## 5. 手动删除备份

### 5.1 API

新增接口：

```http
DELETE /api/v1/snapshots/{id}
```

行为：

- 只允许删除 active snapshot。
- 设置 `deleted_at`。
- 记录删除原因为 `manual`。
- 返回删除后的 snapshot 状态和推荐维护动作。

建议响应：

```json
{
  "status": "deleted",
  "snapshot": {
    "id": 12,
    "deleted_at": "2026-06-21T03:00:00Z"
  },
  "space_reclaim": {
    "requires_compact": true,
    "message": "space will be reclaimed by scheduled compact"
  }
}
```

批量删除接口：

```http
POST /api/v1/snapshots/delete
Content-Type: application/json

{
  "snapshot_ids": [10, 11, 12]
}
```

批量删除应按单个 snapshot 独立处理，返回每个 id 的结果。某个 snapshot 删除失败不影响其他 snapshot。

### 5.2 约束

- 如果 snapshot 正在被 restore task 使用，删除应返回冲突错误。
- 如果 snapshot 已经 deleted，重复删除应幂等返回 `deleted`。
- 删除最后一个 active snapshot 允许执行，但前端必须二次确认。
- 删除 snapshot 不删除 run 记录和 run log。run 是审计记录。

## 6. 自动清理错误数据

新增维护模式：

```http
POST /api/v1/storage/maintenance
Content-Type: application/json

{"mode":"cleanup-errors"}
```

`cleanup-errors` 执行：

1. 标记 stale run。
   - 查询 `pending/running` 且超过 `maintenance.stale_run_after` 的 run。
   - 只有服务端启动时间晚于 run 最近更新时间，且 run 没有活跃执行器持有时，才标记为 `failed`。
   - 错误消息写入 `stale run expired by maintenance`。

2. 计算 active snapshot 存活根。
   - 读取所有 active snapshot manifest。
   - 构建 `live_manifest_ids` 和 `live_chunk_hashes`。

3. 清理 manifest。
   - 删除不在 `live_manifest_ids` 中，且修改时间早于 `error_grace_period` 的 manifest 文件。
   - 如果 manifest 属于 deleted snapshot，可在 deleted snapshot 超过 `keep_deleted_metadata_days` 后删除。

4. 清理 chunk index。
   - 删除不在 `live_chunk_hashes` 中，且最后引用时间早于 `error_grace_period` 的 chunk index 条目。
   - 当前 chunk index 没有写入时间时，第一阶段可使用 run 完成时间和 snapshot 引用关系保守判断；后续可在 index value 中补充 `created_at`。

5. 生成报告。
   - stale run 数量。
   - 删除 manifest 数量。
   - 删除 orphan chunk index 数量。
   - 预计待 compact 回收 segment 字节数。

`cleanup-errors` 不直接删除 segment 文件。segment 只由 compact 在重写 active chunk 后删除。

## 7. Compact 设计

compact 的职责是物理回收空间：

1. 获取维护 gate，阻止新的备份写入和 agent chunk 上传。
2. 确认没有 active run。
3. 执行 retention 和 cleanup-errors。
4. 读取 active snapshot manifest。
5. 按 chunk hash 去重后，将 active chunk 顺序写入新 segment。
6. 更新 chunk index 指向新 segment record。
7. 更新 active manifest 中的 chunk ref。
8. 删除未被新 index 引用的旧 segment。

compact 必须满足：

- 不原地改写旧 segment。
- 新 segment 写入完成前，不删除旧 segment。
- manifest 和 index 更新失败时，旧 segment 仍可用于恢复。
- compact 与常规备份写入互斥。
- compact 报告必须包含跳过原因。

自动 compact 默认周期：

- 每周日 03:30 按 `maintenance.compact_schedule` 执行。
- 如果上一次 compact 仍在运行，本次跳过。
- 如果有 active run，本次跳过。
- 如果预计收益小于 `compact_min_reclaim_ratio` 且小于 `compact_min_reclaim_bytes`，本次跳过。

## 8. Verify 与损坏快照

`verify` 是只读操作，不执行删除：

- 校验 active manifest 是否可读。
- 校验 manifest 引用的 chunk 是否存在于 index。
- 校验 index 指向的 segment record 是否可读。
- 校验 chunk hash 和文件大小是否匹配。

如果 active snapshot 校验失败：

- 不自动删除 snapshot。
- 在维护报告中返回错误。
- 后续可增加 `snapshot_health` 字段或独立表，标记 `healthy / warning / corrupt`。
- UI 在 Snapshots 页面显示异常标记，并引导管理员手动删除或恢复到其他快照。

## 9. 数据模型调整

建议新增字段：

```sql
ALTER TABLE snapshots ADD COLUMN delete_reason TEXT;
ALTER TABLE snapshots ADD COLUMN deleted_by TEXT;
```

说明：

- `delete_reason`: `manual`、`retention`、`api`。
- `deleted_by`: 当前阶段可写固定值 `admin`，以后接入多用户后写真实用户。

建议新增维护记录表：

```sql
CREATE TABLE maintenance_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  mode TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at DATETIME NOT NULL,
  finished_at DATETIME,
  skipped_reason TEXT,
  report_json TEXT NOT NULL DEFAULT '{}',
  error_message TEXT
);
```

用于：

- Storage 页面展示最近维护历史。
- 排查自动清理是否执行。
- 记录 compact 跳过原因。

可选新增 chunk 元数据：

```text
-- 如果继续使用 Pebble chunk index，可把字段加到 index value 中。
{
  "hash": "...",
  "segment_id": 1,
  "offset": 0,
  "length": 123,
  "original_size": 456,
  "compressed_size": 321,
  "created_at": "2026-06-21T03:00:00Z"
}
```

`created_at` 用于更精确地实现 `error_grace_period`。

## 10. 前端交互

### 10.1 Snapshots 页面

列表增加：

- 多选框。
- 单项删除按钮。
- 批量删除按钮。
- snapshot 健康状态。
- 已删除后预计回收提示。

删除确认文案需要明确：

- 删除后不能从该 snapshot 恢复。
- 磁盘空间不会立即释放。
- 空间会由计划 compact 自动回收。

### 10.2 Storage 页面

维护操作区提供：

- 执行保留策略。
- 校验仓库。
- 清理错误备份数据。
- 压缩并回收空间。
- 查看最近维护历史。

Repository 指标增加：

- active snapshot 数量。
- deleted snapshot 数量。
- orphan chunk 估算。
- 可回收 segment 估算。
- 下次 cleanup 时间。
- 下次 compact 时间。

### 10.3 Settings 页面

新增维护配置：

- 自动维护启用开关。
- cleanup cron。
- compact 启用开关。
- compact cron。
- 错误数据宽限期。
- stale run 判定时间。
- deleted metadata 保留天数。
- compact 最小回收阈值。

## 11. API 模式汇总

`POST /api/v1/storage/maintenance` 支持：

- `retention`: 按保留策略软删除过期 snapshot。
- `verify`: 只读校验 active snapshot。
- `cleanup-errors`: 清理 stale run、orphan manifest、orphan chunk index。
- `compact`: 执行 retention、cleanup-errors 和物理回收。
- `full-cleanup`: 手动执行完整维护流程，等价于 `retention + cleanup-errors + compact`。

建议区分 `compact` 和 `full-cleanup`：

- 自动 compact 使用 `compact`，允许按阈值跳过。
- 管理员手动点击“完整清理”使用 `full-cleanup`，不按收益阈值跳过。

## 12. 实施顺序

第一阶段：删除能力

- 增加 snapshot 单个删除 API。
- 增加批量删除 API。
- Snapshots 页面增加删除入口。
- 删除后仍通过现有 compact 回收空间。

第二阶段：错误数据清理

- 增加 `cleanup-errors` 维护模式。
- 增加 stale run 判定。
- 增加 orphan manifest 清理。
- 增强 orphan chunk index 清理报告。

第三阶段：自动维护

- 增加 `maintenance` 配置块。
- 增加自动 cleanup 调度。
- 增加默认启用的自动 compact 调度。
- 增加维护历史记录。

第四阶段：可观测性和健康状态

- 增加最近维护历史 UI。
- 增加 snapshot verify 健康状态展示。
- 增加 compact 可回收空间估算。

## 13. 测试要求

后端测试：

- 删除 active snapshot 后，列表和恢复接口不再返回该 snapshot。
- 重复删除 snapshot 幂等。
- 删除最后一个 snapshot 只需要 API 层允许，前端负责二次确认。
- retention 删除的 snapshot 后续可被 compact 释放空间。
- failed run 写入 chunk 但没有发布 snapshot 时，`cleanup-errors` 能移除 orphan index。
- stale run 超时后被标记 failed。
- active run 存在时 compact 跳过。
- compact 后 active snapshot 仍可浏览、下载、恢复。

前端测试：

- Snapshots 页面先显示列表。
- 删除操作在弹窗中确认。
- 批量删除能显示部分成功和部分失败。
- Storage 页面能触发四类维护模式并展示报告。

SMR 相关验证：

- 常规备份不会改写已关闭 segment。
- 删除 snapshot 不触碰 segment。
- compact 只在维护窗口顺序写新 segment，并删除旧 segment。
- 自动 compact 跳过时必须写明原因。
