# Bolt BufferManager

本文档描述 `bolt/common/memory/bm` 当前实现。它只覆盖现存机制：`BufferManager` owner thread 负责提交任务和提交状态，进程级服务负责 spill/prefetch 的磁盘执行、completion 发布、文件布局和 I/O 调度。

## 目标和边界

BufferManager 是 Bolt 内部面向算子的内存块管理层。它提供：

- 以 `BlockHandle` 为单位的分配、pin、unpin、reload、prefetch。
- 按 `MemoryTag` 和 `ReservationKind` 统计逻辑使用量。
- 按 block policy 执行 discard、recompute、spill。
- 通过进程级服务共享 spill 目录、spill file store、disk probe 和 disk I/O 队列。
- owner-thread 状态提交：后台任务只生产 completion，不直接修改 `BlockHandle` 的最终状态。

BufferManager 不做全局 quota 管理。外部 quota / memory arbitrator 决定何时调用 `Reclaim()`；BufferManager 只响应回收请求，并维护自身使用统计。

## 模块关系

```text
Operator / Task
      |
      v
+-----------------+
| BufferManager   |
+-----------------+
      |
      +-----------------------------+
      |                             |
      v                             v
+-------------+              +----------------+
| BlockHandle |              | BlockEvictor   |
+-------------+              +----------------+
      |                             |
      | resident memory             | spill candidate
      v                             v
+-----------------+          +---------------------+
| AccountedMemory |          | SpillCoordinator |
+-----------------+          +---------------------+
      |                             |
      v                             | submit DiskIoTask
+-----------------+                 v
| BufferAllocator |          +----------------------+
+-----------------+          | DiskIoTaskExecutor |
      |                      +----------------------+
      v                             |
+-----------------+                 | worker executes task
| BufferPool      |                 v
| accounting      |          +----------------+
+-----------------+          | SpillFileStore     |
      |                      +----------------+
      v                             |
+-----------------+                 +--> SpillCompression
| Bolt MemoryPool |                 +--> SmallSpillAllocator
+-----------------+                 +--> DiskIoScheduler
                                           |
                                           v
                                    UringDiskIoEngine
```

主要职责：

- `BufferManager`：对外入口；创建 block；维护 live block weak list；执行 `Reclaim()` / `Prefetch()`；drain completion；在 owner thread 提交 block 状态。
- `BlockHandle`：单个 block 的状态机；持有 resident memory 或 spill location；提供 prepare/commit/failure 方法。
- `BufferHandle`：RAII pin。初始写 handle 析构时 seal block，普通 pin handle 析构时 unpin。
- `BufferPool`：逻辑使用量统计，不拒绝分配，也不主动触发 reclaim。
- `BufferAllocator` / `AccountedMemory`：把 BufferPool 逻辑 reservation 和 Bolt `MemoryPool` 物理分配绑在一起。
- `BlockEvictor`：按 cost class 和 priority 组织 eviction candidates。
- `SpillCoordinator`：进程级 spill domain 服务；维护 completion queues、owner-token progress、single `SpillFileStore`。
- `DiskIoTaskExecutor`：进程级中心 I/O task queue；执行 spill write 和 prefetch read task。
- `SpillFileStore`：spill 文件布局、压缩、小块 slab slot、大块 dedicated file、read/write/release。
- `DiskIoScheduler`：在单个 disk engine 上做 priority 选择、adaptive queue depth 观测和请求执行。

## 线程模型

```text
BufferManager owner thread
      |
      | public API:
      |   Allocate / Pin / Reclaim / Prefetch / Snapshot
      v
+-----------------------------+
| block state prepare/commit  |
+-----------------------------+
      |
      | submit spill/prefetch task
      v
+-----------------------------+
| DiskIoTaskExecutor worker |
+-----------------------------+
      |
      | read/write through SpillFileStore
      v
+-----------------------------------+
| SpillCoordinator queues        |
| completed_ / prefetchCompleted_   |
+-----------------------------------+
      |
      | owner later drains
      v
+-----------------------------+
| BlockHandle commit/failure  |
+-----------------------------+
```

规则：

- `BufferManager` 非静态 public API 必须由构造它的 owner thread 调用。
- 多个 task 可以各自持有一个 `BufferManager`，在不同线程并发运行。
- 进程级 `SpillCoordinator` 和 `DiskIoTaskExecutor` 是线程安全的。
- disk worker 不持有 `BlockHandle`，不直接提交 block 最终状态。
- completion 通过 owner token 归属到对应 BufferManager。
- `BlockHandle` 内部仍使用 mutex / condition variable 来协调 pin、unpin、loading、spilling 和 manager destruction。

## 配置入口

进程级服务通过以下接口初始化：

```cpp
BufferManager::InitializeProcessServices(BufferManagerProcessServicesConfig)
```

关键配置：

- `spill.enabled`：是否配置进程级 spill coordinator。
- `spill.spillDir`：spill 文件根目录。
- `spill.workerThreadCount`：进程级 disk task worker 数，必须大于 0。
- `spill.diskProbeDuration` / `spill.diskProbe` / `forcedKind` / `unknownFallbackKind`：磁盘分类探测。
- `spill.diskIo`：io_uring ring entries、disk task worker、priority weights、adaptive queue depth 参数。
- `spill.smallSpill`：小块 slab 文件策略。
- `spill.compression`：spill payload 压缩策略。
- `metrics`：进程级 metrics sink。

每个 `BufferManager` 的构造配置包括：

- `poolName`：Bolt MemoryPool 子树名称。
- `reserveWaitTimeout`：`Reclaim()` / `Pin()` 等待 async progress 的超时时间。
- `metrics`：该 BufferManager 的 BufferPool / BlockHandle metrics sink。
- `spillEnabled`：是否允许分配 `EvictPolicy::kSpillToDisk` block。

`EvictPolicy::kSpillToDisk` 需要进程级 spill coordinator 已初始化，否则分配会抛 `BoltUserError`。

## Allocate 流程

```text
Operator
   |
   | BufferManager.Allocate(options)
   v
+-------------------------+
| AssertOwnerThread       |
+-------------------------+
   |
   | policy == kSpillToDisk ?
   |      |
   |      +--> EnsureSpillCoordinator()
   v
+-------------------------+
| create BlockHandle      |
+-------------------------+
   |
   v
+-------------------------+
| BufferAllocator         |
|  - BufferPool.Reserve   |
|  - Bolt MemoryPool alloc|
+-------------------------+
   |
   v
+-------------------------+
| AccountedMemory         |
+-------------------------+
   |
   v
+-------------------------+
| BlockHandle.InstallMemory
| state = kLoaded
| pinCount = 1
+-------------------------+
   |
   v
+-------------------------+
| RegisterBlock(weak_ptr) |
| Enqueue eviction node   |
+-------------------------+
   |
   v
BufferHandle(initialWrite=true)
```

初始写 handle 结束时，`BufferHandle::Reset()` / 析构会调用 `BlockHandle::Unpin(initialWrite=true)`。block 被 seal 后才是正常可回收候选。

## Block 状态机

```text
                         InstallMemory()
        +---------------------------------------------+
        |                                             v
   +----------+                                  +---------+
   | kInvalid |                                  | kLoaded |
   +----------+                                  +---------+
        ^                                        /   |   \
        |                                       /    |    \
        |                                      /     |     \
        |                         discard evict      |      spill prepare
        |                                    v        |          v
        |                              +------------+ |    +-----------+
        |                              | kDiscarded | |    | kSpilling |
        |                              +------------+ |    +-----------+
        |                                             |      |       |
        |                         recompute evict     |      |       |
        |                                    v        |      |       |
        |                       +----------------------+      |       |
        |                       | kEvictedRecomputable |      |       |
        |                       +----------------------+      |       |
        |                                    |               |       |
        |                                    | Pin           |       |
        |                                    v               |       |
        |                              +----------+          |       |
        |                              | kLoading | <--------+       |
        |                              +----------+   write failure  |
        |                                    |                       |
        |                         load/recompute success             |
        |                                    v                       |
        |                              +---------+                   |
        |                              | kLoaded |                   |
        |                              +---------+                   |
        |                                                          |
        |                                                          | write success
        |                                                          v
        |                                                     +----------+
        |                                                     | kSpilled |
        |                                                     +----------+
        |                                                          |
        |                                                          | Pin / Prefetch
        |                                                          v
        |                                                     +----------+
        |                                                     | kLoading |
        |                                                     +----------+
        |
        +------------- manager destruction from any state --------+
```

状态含义：

- `kLoaded`：resident memory 存在，可以 pin。
- `kSpilling`：resident memory 已从 block 移到 spill request，仍由 `AccountedMemory` 持有，等待 completion commit。
- `kSpilled`：resident memory 已释放，block 持有 `SpillLocation`。
- `kLoading`：正在 reload、recompute 或 prefetch。
- `kDiscarded`：数据已永久丢弃，后续 pin 返回 invalid handle。
- `kEvictedRecomputable`：resident memory 已释放，后续 pin 通过 `recoveryFn` 重建。
- `kInvalid`：manager 销毁或 block 不再可用。

## Reclaim 流程

`Reclaim(targetBytes)` 响应外部内存回收请求。`targetBytes == 0` 表示尽力回收到没有候选为止。

```text
Reclaim(targetBytes)
      |
      v
AssertOwnerThread()
      |
      v
DrainPrefetchCompletions()
DrainSpillCompletions()
      |
      v
Refresh eviction candidates from live blocks
      |
      v
+----------------------------+
| need more reclaimed bytes? |
+----------------------------+
      | no                         | yes
      v                            v
return reclaimed          BlockEvictor.TryPopAnyCandidate()
                                   |
                                   v
                         +----------------+
                         | candidate cost |
                         +----------------+
                            |           |
                   kFreeOrCheap       kSpill
                            |           |
                            v           v
                  TryEvictNodeSync   TryScheduleEvict
                  discard/recompute       |
                            |             |
                            v             v
                    reclaimed +=     scheduledSpillCount++
                    freedBytes
                            |             |
                            +------+------+
                                   |
                                   v
                     WaitForProgress / drain completions
```

`Reclaim()` 返回的是已经真实释放的 resident bytes。提交到中心 I/O 队列的 spill write 不会立即计入 reclaimed；只有 owner thread drain completion 并 commit 成功后，resident memory 被释放，才计入返回值。

## Eviction 顺序

`BlockEvictor` 使用二维 FIFO bucket：

```text
Pop order:

  1. kFreeOrCheap
       1. kLow FIFO
       2. kNormal FIFO
       3. kHigh FIFO
       4. kCritical FIFO

  2. kSpill
       1. kLow FIFO
       2. kNormal FIFO
       3. kHigh FIFO
       4. kCritical FIFO

Within each bucket:

  enqueue back ---> [ oldest ... newest ] ---> pop front
```

队列允许 stale node。执行前会重新校验 weak block、concrete type、eviction sequence、pin count 和 policy；不匹配则跳过。

## Spill Write 流程

```text
Owner thread
    |
    | Reclaim() chooses spill candidate
    v
+------------------------------+
| BlockEvictor.TryScheduleEvict|
+------------------------------+
    |
    | validate node and mark scheduled
    v
+------------------------------+
| SpillCoordinator.SubmitSpill
+------------------------------+
    |
    | BlockHandle.PrepareAsyncSpill(seq)
    |   - state = kSpilling
    |   - memory_ moves into SpillRequest
    v
+------------------------------+
| DiskIoTaskExecutor.SubmitTask
| priority = kHigh
+------------------------------+
    |
    v
DiskIoTaskExecutor worker
    |
    | ExecuteSpill(request)
    v
+------------------------------+
| SpillFileStore.Write             |
|  - compress or raw payload   |
|  - choose small slot/file    |
|  - DiskIoScheduler write     |
+------------------------------+
    |
    v
+------------------------------+
| SpillCompletion              |
| owner, blockId, seq, memory, |
| location or error            |
+------------------------------+
    |
    v
SpillCoordinator.completed_
    |
    v
Owner thread later drains
    |
    v
+------------------------------+
| CommitAsyncSpillSuccess      |
|  - spillLocation_ = location |
|  - state = kSpilled          |
|  - memory is released        |
+------------------------------+
```

失败路径：

- task 执行失败时，completion 携带 error 和原 memory。
- owner thread drain 后调用 `CommitAsyncSpillFailure()`，在 sequence 匹配时恢复 `memory_`，状态回到 `kLoaded`。
- completion 到达时 block 已经不存在，BufferManager 释放 memory 或 spill location，避免泄漏。

## Prefetch 流程

`Prefetch(blocks)` 是 best-effort 异步 reload。它把已经 spilled 的 block 提前读回 resident memory，但不 pin block。

```text
Owner thread
    |
    | Prefetch(blocks)
    v
+------------------------------+
| for each block               |
+------------------------------+
    |
    +-- null / not spilled / in flight
    |      |
    |      +--> skippedCount++
    |
    +-- kLoaded
    |      |
    |      +--> alreadyLoadedCount++
    |
    +-- kSpilled
           |
           v
    +--------------------------+
    | PrepareAsyncPrefetch     |
    |  - allocate memory       |
    |  - state = kLoading      |
    |  - memory -> request     |
    +--------------------------+
           |
           v
    +--------------------------+
    | SpillCoordinator      |
    | SubmitPrefetch           |
    +--------------------------+
           |
           v
    +--------------------------+
    | DiskIoTaskExecutor     |
    | SubmitTask(priority=Low) |
    +--------------------------+
```

```text
DiskIoTaskExecutor worker
    |
    | ExecutePrefetch(request)
    v
+------------------------------+
| SpillFileStore.Read              |
|  - DiskIoScheduler read      |
|  - decompress if needed      |
+------------------------------+
    |
    v
+------------------------------+
| PrefetchCompletion           |
| owner, blockId, seq, memory, |
| error                        |
+------------------------------+
    |
    v
SpillCoordinator.prefetchCompleted_
    |
    v
Owner thread drains:
  - Prefetch({})
  - Pin()
  - Reclaim()
  - BufferManager destruction
    |
    v
+------------------------------+
| CommitAsyncPrefetchSuccess   |
|  - memory moves to block     |
|  - state = kLoaded           |
|  - old spill location release|
|  - pinCount unchanged        |
+------------------------------+
```

`Prefetch({})` 是显式 drain-only 调用。非空 `Prefetch(blocks)` 的返回值只描述本次提交结果，不混入已经完成但尚未 drain 的 completion。

`Pin()` 在调用 `BlockHandle::Pin()` 前会先 drain prefetch completions；如果目标 block 正处于 prefetch 的 `kLoading`，owner thread 会等待中心 I/O progress 并继续 drain，避免重复读盘。

## Pin / Reload 流程

```text
Pin(block)
    |
    v
BufferManager.DrainPrefetchCompletionsBeforePin(block)
    |
    v
BlockHandle.Pin()
    |
    v
+----------------+
| current state? |
+----------------+
    |
    +-- kLoaded
    |      |
    |      +--> pinCount++ -> return BufferHandle
    |
    +-- kSpilled
    |      |
    |      +--> state = kLoading
    |      +--> allocate resident memory
    |      +--> SpillCoordinator.Read(location)
    |      +--> state = kLoaded
    |      +--> release old spill location
    |      +--> pinCount++ -> return BufferHandle
    |
    +-- kEvictedRecomputable
    |      |
    |      +--> state = kLoading
    |      +--> allocate resident memory
    |      +--> recoveryFn(data, size)
    |      +--> state = kLoaded
    |      +--> pinCount++ -> return BufferHandle
    |
    +-- kSpilling / kLoading
    |      |
    |      +--> wait on cv_ -> retry
    |
    +-- kDiscarded / kInvalid
           |
           +--> return invalid BufferHandle
```

`loadGeneration_` 和 `lastLoadError_` 用于让同一代 reload 的等待者看到一致的失败结果。

## SpillFileStore 文件布局

写 spill 时先准备 payload：

```text
logical bytes
    |
    v
+--------------------------------------------+
| compression enabled and bytes >= minBytes? |
+--------------------------------------------+
      | yes                         | no
      v                             v
try zstd compression             raw payload
      |
      v
+------------------------------+
| saved enough by ratio?       |
+------------------------------+
      | yes                         | no
      v                             v
zstd payload                  raw payload
storedBytes = compressed      storedBytes = logical
      |                             |
      +--------------+--------------+
                     |
                     v
+--------------------------------------+
| storedBytes fits a small size class? |
+--------------------------------------+
      | yes                         | no
      v                             v
SmallSpillAllocator slot       dedicated file
path + offset + slotBytes      bm_<tag>_<id>.spill
      |                             |
      +--------------+--------------+
                     |
                     v
              DiskIoScheduler write
                     |
                     v
              SpillLocation
```

`SpillLocation` 记录：

- `path`
- `offset`
- `logicalBytes`
- `storedBytes`
- `slotBytes`
- `smallSlot`
- `compressionCodec`
- `disk`

小块 spill 使用 size class slab 文件；release 时 slot 回到 free list，如果 slab 文件所有 slot 都释放，整个文件会删除。大块 spill 使用 dedicated file，release 时删除对应文件。

## Disk Probe 和 Disk I/O

`SpillCoordinator` 初始化 `SpillFileStore` 前会执行 disk probe：

```text
forcedKind != kUnknown ?
    |
    +-- yes --> use forced kind
    |
    +-- no
         |
         v
diskProbeDuration <= 0 ?
    |
    +-- yes --> use fallback kind
    |
    +-- no
         |
         v
active O_DIRECT probe
    |
    v
classify by min(writeIops, readIops)
```

I/O 执行层：

```text
DiskIoTaskExecutor
    |
    | central task queue
    |   - spill write task: priority High
    |   - prefetch read task: priority Low
    v
worker thread
    |
    v
SpillFileStore.Read / Write
    |
    v
DiskIoScheduler.SubmitAndWait(request)
    |
    | priority weights
    | adaptive queue depth Observe()
    v
UringDiskIoEngine.Execute(request)
```

`DiskIoTaskExecutor` 控制 task 入口；`DiskIoScheduler` 控制单个 engine 上的 request 选择和 completion 观测。

## Completion 和生命周期

```text
DiskIoTaskExecutor task
    |
    v
SpillCoordinator completion queue
    |
    v
owner thread drain
    |
    +-- block alive and sequence matches
    |      |
    |      +--> commit success/failure
    |
    +-- block gone or sequence stale
           |
           +--> drop memory / release spill location
```

BufferManager 析构时会持续 drain spill 和 prefetch completion，并等待该 owner token 的 active task 完成，然后再 invalidate live blocks。

## Metrics 和日志

常见 metrics：

- `bm_allocate_requests_total`
- `bm_reclaim_requests_total`
- `bm_reclaim_bytes_total`
- `bm_used_memory_bytes`
- `bm_pinned_memory_bytes`
- `bm_allocate_duration_us`
- `bm_reclaim_duration_us`
- `bm_spill_submit_total`
- `bm_spill_scheduled_total`
- `bm_spill_backpressured_total`
- `bm_spill_skipped_total`
- `bm_spill_failed_total`
- `bm_spill_executed_total`
- `bm_spill_freed_bytes_total`
- `bm_spill_queue_depth`
- `bm_spill_bytes_written{disk=...}`
- `bm_spill_bytes_stored{disk=...}`
- `bm_spill_bytes_read{disk=...}`
- `bm_spill_release_total{disk=...}`
- `bm_spill_small_slot_total{disk=...}`
- `bm_spill_dedicated_file_total{disk=...}`
- `bm_spill_compress_attempt_total{disk=...}`
- `bm_spill_compress_saved_bytes{disk=...}`
- `bm_spill_compress_fallback_raw_total{disk=...}`

日志分层：

- `INFO`：BufferManager 创建/销毁，Reclaim 汇总，SpillCoordinator / SpillFileStore 初始化，DiskProbe 结果。
- `VLOG(1)`：block allocate/pin/unpin/enqueue/spill/prefetch、SpillFileStore 写入细节、压缩 fallback 等高频路径。
- `WARNING`：reload/spill/prefetch 失败、cleanup 失败、double release 等异常但可恢复事件。

## 当前行为要点

1. `BufferPool` 只统计，不限额。
2. `BufferManager` public API 是 owner-thread confined。
3. `Reclaim()` 返回实际释放字节，不返回已提交 I/O 的字节。
4. `Prefetch()` 是 best-effort；成功后 block resident，但不 pin。
5. spill write 和 prefetch read 都通过 `DiskIoTaskExecutor` 的中心 task queue 执行。
6. `SpillCoordinator` 保存 completion，owner thread 负责 commit block 状态。
7. `workerThreadCount > 0` 是必需配置。
8. `kPinnedForever` 不进入 eviction queue，使用 `ReservationKind::kPinned` 统计。
9. `kDiscard` 被回收后 pin 返回 invalid handle；`kRecompute` 被回收后 pin 调 `recoveryFn`。
10. `Snapshot().usedLoadedBytes / usedSpilledBytes` 来自 live block 状态扫描；`usedTotalBytes` 来自 BufferPool resident usage。
