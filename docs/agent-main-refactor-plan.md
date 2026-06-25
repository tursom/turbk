# Agent main.go 重构计划

日期：2026-06-25

状态：计划草案。

本文整理 `cmd/turbk-agent/main.go` 的结构重构方案。目标是降低 Agent 入口文件的认知复杂度、减少后续吞吐和 daemon 改造的 merge 冲突，并保持现有运行行为不变。

相关设计：

- [Agent 常驻与本地索引设计](agent-daemon-design.md)
- [Agent 高延迟网络吞吐优化设计](agent-high-latency-pipeline-design.md)
- [Agent 小文件备份优化设计](agent-small-file-optimization-design.md)
- [Agent 备份吞吐后续代码改造计划](agent-throughput-code-change-plan.md)
- [Agent Pebble Catalog 设计](agent-pebble-catalog-design.md)

## 1. 当前问题

当前 `cmd/turbk-agent/main.go` 已超过 4000 行，文件内同时承载以下职责：

- CLI flag、环境变量解析和启动分派。
- Agent HTTP client、请求/响应 DTO 和通用 HTTP 调用。
- daemon heartbeat loop、命令处理和本地 cron 调度。
- backup run 生命周期、远端 run 创建/失败/完成上报。
- 文件扫描、manifest 组装、progress 上报、skip 处理。
- chunk batch check/upload、retry、413 split 和响应校验。
- 高延迟 chunk pipeline、in-flight 字节窗口和 worker 管理。
- 并行文件读取、small-file pack、catalog 复用逻辑。
- throughput benchmark 和 mock server。

这导致几个实际维护问题：

- 单次功能改动容易触碰同一个大文件，冲突面大。
- `scanAndUpload()` 和 `runDaemon()` 既做编排又做细节处理，阅读成本高。
- 吞吐实验代码、生产备份路径和 HTTP 基础设施混在一起，不利于定位边界。
- heartbeat 下发配置到 `backupRunOptions` 的映射在多个入口重复，后续新增开关容易漏改。

## 2. 目标

- 首阶段只做同 package 拆文件，不改变包名、不移动到 `internal/agent`。
- 保持 CLI 参数、环境变量、HTTP API、manifest schema、catalog schema 和 repository 格式不变。
- 保持现有测试语义不变，优先用机械移动降低风险。
- 让 `main.go` 只保留入口分派和最小启动逻辑。
- 为后续 daemon、pipeline、small-file pack 和扫描优化提供清晰文件边界。

## 3. 非目标

- 不在本计划中改造数据面协议。
- 不在本计划中调整吞吐配置默认值。
- 不在本计划中拆 repository 写入锁或实现服务端写入队列。
- 不在首阶段把 Agent 拆成多个 Go package。
- 不为了拆文件引入过度抽象或通用框架。

## 4. 目标文件布局

首阶段建议保持 `package main`，只拆分文件：

| 文件 | 职责 |
| --- | --- |
| `main.go` | CLI 入口、flag 注册、模式分派 |
| `flags.go` | `rootFlag`、环境变量读取、byte size/duration/schedule 解析 |
| `client.go` | `agentClient`、heartbeat/progress/run/manifest/chunk API、HTTP helper |
| `types.go` | 跨模块共享 DTO 和常量，必要时再拆小 |
| `daemon.go` | `daemonOptions`、heartbeat loop、命令处理、cron 判断 |
| `backup.go` | `backupRunOptions`、run 创建/修复/提交/完成生命周期 |
| `scan.go` | 文件扫描、manifest 路径、skip 处理、catalog record 构建 |
| `chunk_batch.go` | 串行 chunk batcher、retry、413 split、batch body 编码 |
| `chunk_pipeline.go` | pipeline batcher、worker、pipeline 字节窗口 |
| `file_read.go` | 并行文件读取和 file-read 字节窗口 |
| `small_pack.go` | small-file pack batcher、pack fingerprint、pack manifest helper |
| `benchmark.go` | throughput benchmark、mock server、benchmark manifest fingerprint |
| `catalog.go` | 继续承载现有本地 catalog 实现 |

如果 `types.go` 变成新的大杂烩，应回退为更明确的文件，例如 `protocol.go`、`progress.go` 或 `chunk_types.go`。

## 5. 分阶段计划

### P0：基线和保护线

执行前先确认：

- `git status --short` 干净，或只存在明确无关改动。
- `go test ./cmd/turbk-agent` 通过。
- 记录当前 `cmd/turbk-agent/main.go` 行数，作为拆分效果对比。

验收标准：

- 未改动行为逻辑。
- 当前 Agent 单包测试通过。

### P1：机械拆文件

按职责从 `main.go` 移动连续代码块到目标文件。此阶段只允许：

- 移动 type、const、var、func。
- 调整 import。
- 运行 `gofmt`。

不允许：

- 重命名函数。
- 改变函数签名。
- 合并重复逻辑。
- 改变错误信息、日志字段或默认值。

建议顺序：

1. 先拆 `benchmark.go`，因为它和生产路径耦合最弱。
2. 再拆 `flags.go` 和 `client.go`，降低入口文件噪音。
3. 拆 `daemon.go` 和 `backup.go`，保留现有函数签名。
4. 拆 `chunk_batch.go`、`chunk_pipeline.go`、`file_read.go`。
5. 最后拆 `scan.go` 和 `small_pack.go`，因为它们和 manifest/catalog/chunk path 耦合更强。

验收标准：

- `go test ./cmd/turbk-agent` 通过。
- `main.go` 只保留启动分派，目标行数控制在 300 行以内。
- Git diff 主要表现为代码移动和 import 调整。

### P2：消除重复配置映射

在机械拆分稳定后，抽取 heartbeat 到 `backupRunOptions` 的映射逻辑。

建议新增：

```go
func backupOptionsFromHeartbeat(heartbeat heartbeatResponse, base backupRunOptions) backupRunOptions
```

用途：

- `main()` 的 once backup 路径复用该函数。
- `runDaemon()` 的手动命令 backup 路径复用该函数。
- `runDaemon()` 的 schedule backup 路径复用该函数。

保留 `base` 参数是为了让调用方仍能设置：

- `Catalog`
- `CommandID`
- `Trigger`
- `MaxManifestRepairAttempts`

验收标准：

- 新增 heartbeat 字段时只需要改一个映射点。
- once、manual、schedule 三条路径的配置差异在调用处清晰可见。
- 现有测试通过，必要时补一个 focused unit test 覆盖映射默认值。

### P3：拆解 scanAndUpload

`scanAndUpload()` 是最大复杂度来源。建议引入窄作用域结构承载一次扫描上下文：

```go
type backupScanner struct {
	client             *agentClient
	runID              int64
	roots              []string
	logger             *slog.Logger
	scanOptions        fsfilter.Options
	opts               backupRunOptions
	manifest           *repository.SnapshotManifest
	progress           agentProgress
	chunkUploader      agentChunkUploader
	packBatcher        *agentSmallFilePackBatcher
	fileCatalogBatcher *agentFileCatalogBatcher
}
```

再把大函数拆成方法：

- `scan()`
- `walkRoot(root string)`
- `handleDir(...)`
- `handleSymlink(...)`
- `handleRegularFile(...)`
- `queueParallelFile(...)`
- `processParallelJobs()`
- `flushPendingFiles()`
- `flushPackFiles()`
- `finalizeFileEntry(...)`
- `sendProgress(force bool)`

约束：

- 不改变 manifest entry 排序规则。
- 不改变 small-file pack 的 pack ID、fingerprint 和 catalog 写入语义。
- 不改变 skip policy 和现有错误包装。
- 不改变 progress 节流行为。

验收标准：

- `scanAndUpload()` 退化为创建 scanner 并调用 `scan()`。
- small-file pack、parallel scan、catalog reuse、multi-root manifest 的测试继续通过。
- 每个新方法尽量围绕单一文件类型或 flush 行为。

### P4：测试文件同步拆分

当前 `main_test.go` 也超过 2000 行。生产代码拆完后再拆测试，避免同时移动太多内容。

建议布局：

| 文件 | 覆盖范围 |
| --- | --- |
| `client_test.go` | HTTP client、proxy、retry-after、response body |
| `scan_test.go` | scan/upload、多 root、skip、parallel manifest |
| `chunk_batch_test.go` | batch check/upload、retry、413 split、response validation |
| `chunk_pipeline_test.go` | pipeline、dedupe、byte window |
| `small_pack_test.go` | small-file pack 功能和性能基准 |
| `catalog_test.go` | catalog backend、Pebble/SQLite 行为 |
| `daemon_test.go` | root command payload、cron、rootFlag |
| `benchmark_test.go` | throughput benchmark 输出 |

验收标准：

- 测试名称不变，便于历史追踪。
- `go test ./cmd/turbk-agent` 通过。
- 生产代码拆分和测试拆分分开提交。

## 6. 推荐提交顺序

1. `docs: add agent main refactor plan`
2. `refactor(agent): split benchmark and flag helpers`
3. `refactor(agent): split client and daemon code`
4. `refactor(agent): split backup and chunk upload code`
5. `refactor(agent): split scan and pack code`
6. `refactor(agent): deduplicate backup options mapping`
7. `refactor(agent): break scan uploader into scanner methods`
8. `test(agent): split agent test files`

每个提交都应能单独通过：

```bash
go test ./cmd/turbk-agent
```

阶段性完成后再跑：

```bash
go test ./...
```

## 7. 风险和回滚

主要风险：

- 机械移动时遗漏 import 或移动到错误文件，导致编译失败。
- 抽取 `backupOptionsFromHeartbeat()` 时改变 once/manual/schedule 的细节差异。
- 拆解 `scanAndUpload()` 时改变 pending manifest entry、pack flush 或 progress 上报顺序。
- pipeline 代码存在 goroutine 和 channel 状态，重构时容易引入关闭时序问题。

控制方式：

- P1 只做移动，不做语义调整。
- P2 之后才允许抽函数。
- `scanAndUpload()` 拆解必须保留现有测试覆盖，并优先补齐缺口。
- 每个阶段小步提交，发现行为差异时可以回滚单个提交。

## 8. 完成标准

本计划完成时应满足：

- `cmd/turbk-agent/main.go` 只保留入口分派。
- 生产代码按 daemon、backup、scan、chunk、client、benchmark 分区。
- `backupRunOptions` 的 heartbeat 映射只有一个主实现。
- `scanAndUpload()` 不再是巨型函数。
- `go test ./cmd/turbk-agent` 和 `go test ./...` 通过。
