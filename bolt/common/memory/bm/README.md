# Bolt BufferManager

本文档描述 `bolt/common/memory/bm` 当前代码里的 BufferManager 工作原理。内容以代码实现为准。

## 目标和边界

BufferManager 是 Bolt 内部面向算子的内存块管理层。它提供：

- 以 `BlockHandle` 为单位的内存分配、pin、unpin、reload。
- 按 `MemoryTag` 和 `ReservationKind` 统计逻辑使用量。
- 在上层内存仲裁或 task quota 要求释放内存时，按策略 discard、recompute 或 spill。
- 进程级 spill 服务，负责压缩、落盘、读回、释放 spill 文件。
- 一个共享 spill 目录，一个进程级 `ProcessSpillService`，可被多个 `BufferManager` 复用。

BufferManager 不做全局 quota 管理。外部 quota/arbitrator 决定何时调用 `Reclaim()`；BufferManager 只响应回收请求，并维护自己的使用统计。

## 模块关系

```text
Operator / Task
    |
    v
+-----------------+
| BufferManager   |
+-----------------+
    |        |        |         |
    |        |        |         +--------------------+
    |        |        |                              |
    v        v        v                              v
+--------+  +-----------------+              +---------------+
| blocks |  | BufferAllocator |              | BlockEvictor  |
| weak[] |  +-----------------+              +---------------+
+--------+      |        |                           |
                |        |                           v
                v        v                    +---------------------+
        +------------+  +----------------+     | ProcessSpillService|
        | BufferPool |  | Bolt MemoryPool|     +---------------------+
        | accounting |  | physical bytes |          |          |
        +------------+  +----------------+          |          |
                ^                                  v          v
                |                          +-----------+  +-----------+
        +----------------+                 | SpillStore|  | DiskProbe |
        | AccountedMemory|                 +-----------+  +-----------+
        +----------------+                      |
                                                +--> SpillCompression
                                                +--> SmallSpillAllocator
                                                +--> ProcessDiskIoService
                                                     / DiskIoScheduler
```

主要职责：

- `BufferManager`：对外入口，创建 block，维护 live block weak list，响应 `Reclaim()`，drain async spill completion。
- `BlockHandle`：单个 block 的状态机，持有 resident memory 或 spill location，处理 pin/reload/evict/spill commit。
- `BufferHandle`：RAII pin。初始写 handle 析构时 seal block，普通 pin handle 析构时只 unpin。
- `BufferPool`：只做逻辑使用量统计，不拒绝分配，也不主动触发 reclaim。
- `BufferAllocator` / `AccountedMemory`：把 BufferPool 逻辑 reservation 和 Bolt `MemoryPool` 物理分配绑在一起。
- `BlockEvictor`：按 cost 和 priority 组织 eviction candidates，并分发 cheap eviction 或 spill scheduling。
- `ProcessSpillService`：进程级 spill worker、completion queue、progress wait、single spill store。
- `SpillStore`：真实文件读写，默认压缩，小块 slab slot，大块 dedicated file。
- `DiskIoScheduler`：io_uring I/O engine，带 priority deficit 和 adaptive queue depth。

## 线程模型

`BufferManager` 是 thread-confined：

- 非静态 public API 必须由构造它的 owner thread 调用。
- 多个 task 可以各自持有一个 `BufferManager`，在不同线程并发运行。
- spill/disk 进程级服务是线程安全的。
- async spill worker 只写盘，不直接提交 `BlockHandle` 状态；状态提交由 owner thread 在 `DrainSpillCompletions()` 中完成。

`BlockHandle` 自身仍有 mutex 和 condition variable，因为：

- `BufferHandle::Data()` / `MutableData()` / `Reset()` 需要保护 pin 和 state。
- `Pin()` 可能等待 `kLoading` 或 `kSpilling`。
- async spill prepare/commit 需要和 pin/unpin 状态协调。

## Allocate 流程

```text
Operator
   |
   | Allocate(options)
   v
BufferManager
   |
   | AssertOwnerThread()
   |
   | if policy == kSpillToDisk:
   |     EnsureSpillService()
   |
   | create BlockHandle(context, options)
   v
BufferAllocator
   |
   +--> BufferPool.Reserve(tag, size, kind)
   |
   +--> Bolt MemoryPool.allocate(size)
   |
   v
AccountedMemory
   |
   v
BufferManager
   |
   +--> BlockHandle.InstallMemory(memory)
   |
   +--> RegisterBlock(weak_ptr)
   |
   +--> BlockEvictor.Enqueue(candidate)
   |
   v
Operator receives BufferHandle(initialWrite=true)
```

关键点：

- `Allocate()` 返回的是初始写 `BufferHandle`，此时 `BlockHandle` 为 `kLoaded`，`pinCount == 1`，`sealed == false`。
- 初始写 handle 的生命周期结束后，`BufferHandle::~BufferHandle()` 调用 `BlockHandle::Unpin(initialWrite=true)`，block 被 seal，变成可被回收候选。
- `BufferPool` 的统计随 `AccountedMemory` 生命周期变化；当 resident memory 被移动出 block 并最终析构时，逻辑使用量下降。

## Block 状态机

```text
                 InstallMemory()
        +-------------------------------+
        |                               v
    +----------+                    +---------+
    | kInvalid |                    | kLoaded |
    +----------+                    +---------+
        ^                              |  |  |
        |                              |  |  +-- TryEvict(kDiscard)
        |                              |  |          |
        |                              |  |          v
        |                              |  |    +------------+
        |                              |  |    | kDiscarded |
        |                              |  |    +------------+
        |                              |  |
        |                              |  +-- TryEvict(kRecompute)
        |                              |             |
        |                              |             v
        |                              |    +----------------------+
        |                              |    | kEvictedRecomputable |
        |                              |    +----------------------+
        |                              |             |
        |                              | Pin()/      |
        |                              | Prefetch()  |
        |                              v             |
        |                         +----------+ <-----+
        |                         | kLoading |
        |                         +----------+
        |                         |    |
        | read/recompute success  |    | failure restores previous state:
        |                         |    |   kSpilled or kEvictedRecomputable
        |                         v    |
        |                    +---------+
        |                    | kLoaded |
        |                    +---------+
        |                         |
        | SpillToDisk() or        |
        | PrepareAsyncSpill()     v
        |                    +-----------+
        |                    | kSpilling |
        |                    +-----------+
        |                     |        |
        | write success       |        | write failure
        |                     v        v
        |                +----------+ +---------+
        |                | kSpilled | | kLoaded |
        |                +----------+ +---------+
        |                     |
        |                     | Pin()/
        |                     | Prefetch()
        |                     v
        |                +----------+
        |                | kLoading |
        |                +----------+
        |
        +-- manager destruction from any state
```

`BlockState` 的含义：

- `kLoaded`：resident memory 存在，可以 pin。
- `kSpilling`：resident memory 已经从 block 移到 spill request / sync spill local 变量，仍然由 `AccountedMemory` 计入使用量，直到 commit。
- `kSpilled`：resident memory 已释放，`spillLocation_` 指向磁盘表示。
- `kDiscarded`：数据永久丢弃，后续 pin 返回 invalid handle。
- `kEvictedRecomputable`：resident memory 已释放，后续 pin 通过 recoveryFn 重建。
- `kLoading`：正在 reload 或 recompute。
- `kInvalid`：manager 销毁或 block 不再可用。

## Reclaim 流程

`Reclaim(targetBytes)` 是 BM 响应外部回收请求的主路径。`targetBytes == 0` 表示尽力回收到队列为空。

```text
Reclaim(targetBytes)
        |
        v
AssertOwnerThread()
        |
        v
DrainSpillCompletions()
        |
        v
Scan live blocks:
  - enqueue current candidates
  - erase expired weak_ptr
        |
        v
+-------------------------+
| reclaimed >= target ?   |  target==0 means best effort
+-------------------------+
   | yes                         | no
   v                             v
Update metrics            Evictor.TryPopAnyCandidate()
return reclaimed                  |
                                  v
                      +----------------------+
                      | got candidate node ? |
                      +----------------------+
                         | no                      | yes
                         v                         v
              wait for scheduled          +----------------+
              async spill progress        | node.cost ?    |
              then drain completions      +----------------+
                         |                   |             |
                         +-------------------+             |
                                             |             |
                                      kFreeOrCheap       kSpill
                                             |             |
                                             v             v
                                   TryEvictNodeSync   executionMode?
                                   discard/recompute       |
                                             |             |
                                             |     +----------------------+
                                             |     | owner thread?        |
                                             |     +----------------------+
                                             |       | yes          | no
                                             |       v              v
                                             | block.SpillToDisk  TryScheduleEvict
                                             |                    |
                                             |               scheduled?
                                             |                | yes
                                             |                v
                                             |               loop
                                             +--------------------+
```

实际返回值是“已经真实释放的 resident bytes”，不是提交给后台任务的字节数。async spill 只有在 completion 被 owner thread commit 后，才计入 reclaimed。

## Eviction 顺序

`BlockEvictor` 使用二维 FIFO bucket：

1. cost class：`kFreeOrCheap` 先于 `kSpill`。
2. priority：`kLow` 先于 `kNormal`，再到 `kHigh`，最后 `kCritical`。
3. 同一 bucket 内 FIFO。

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

  enqueue back  --->  [ oldest ... newest ]  --->  pop front
```

队列允许 stale nodes。每个 `EvictionNode` 记录 `evictionSequence`，执行前重新校验：

- block weak_ptr 是否还活着。
- concrete type 是否是 `BlockHandle`。
- sequence 是否仍匹配。
- block 是否未 pinned。

不匹配则 `kSkipped`，不做昂贵清理。

## Owner-thread Spill 路径

当 `spill.executionMode == kOwnerThread` 时，`Reclaim()` 在 BufferManager owner thread 同步调用 `BlockHandle::SpillToDisk()`。

```text
BufferManager owner thread
        |
        | block.SpillToDisk()
        v
BlockHandle
        |
        | lock and check:
        |   - policy is kSpillToDisk
        |   - state is kLoaded
        |   - pinCount == 0
        |   - owner thread matches
        |
        | state = kSpilling
        | memory_ -> local AccountedMemory
        v
ProcessSpillService.Write(tag, data, size)
        |
        v
SpillStore.Write()
        |
        | compress / choose small slot or dedicated file / write bytes
        v
SpillLocation
        |
        v
BlockHandle
        |
        | spillLocation_ = location
        | state = kSpilled
        | destroy local AccountedMemory
        v
BufferPool usage decreases
        |
        v
return freed bytes
```

owner-thread spill 必须在 BufferManager owner thread 执行。`BlockHandle::SpillToDisk()` 会校验 owner thread，避免后台线程直接改 block 状态。

## Worker-thread Spill 路径

当 `spill.executionMode == kWorkerThread` 时，worker 只负责写盘，不持有 `BlockHandle`，也不直接释放 BM 状态。它通过 block id、eviction sequence 和 owner token 交接 completion。

`kWorkerThread` 要求 `workerThreadCount > 0`。如果配置为 `workerThreadCount == 0`，进程级 spill service 初始化会直接报错。worker-thread spill submit 失败或 backpressured 时，`BufferManager` 不会 fallback 到 owner-thread spill；`Reclaim()` 只返回已经真实释放的 resident bytes。

```text
Owner thread:

  BufferManager
      |
      | TryScheduleEvict(node)
      v
  BlockEvictor
      |
      | validate node:
      |   - weak block alive
      |   - evictionSequence matches
      |   - block not pinned
      |
      | BlockHandle.TryMarkSpillScheduled(seq)
      v
  ProcessSpillService.SubmitSpill(node)
      |
      | BlockHandle.PrepareAsyncSpill(seq)
      |   state = kSpilling
      |   memory_ -> SpillRequest.memory
      |
      v
  ready_ queue

Worker thread:

  ready_ queue
      |
      | pop SpillRequest(owner, blockId, seq, tag, memory)
      v
  SpillStore.Write(memory->Data(), memory->Size())
      |
      v
  SpillCompletion(owner, blockId, seq, memory, location/error)
      |
      v
  completed_ queue
      |
      v
  NotifyProgress()

Owner thread later:

  BufferManager.DrainSpillCompletions(owner)
      |
      | find BlockHandle by blockId
      v
  BlockHandle.CommitAsyncSpillSuccess(seq, location, memory)
      |
      | if state and seq still match:
      |   spillLocation_ = location
      |   state = kSpilled
      |   destroy memory
      |
      v
  BufferPool usage decreases
```

失败路径：

- worker 写盘失败：completion 带 error 和原 memory。
- owner thread 调用 `CommitAsyncSpillFailure()`，如果 sequence 仍匹配且 block 有效，则恢复 `memory_`，状态回到 `kLoaded`，并 bump `evictionSequence`。
- 如果 completion 到达时 block 已经死掉，BufferManager 释放 spill location 或 memory，避免泄漏。

## Pin / Reload 流程

```text
Pin(block)
    |
    v
+----------------+
| current state? |
+----------------+
    |
    +-- kLoaded
    |      |
    |      +--> pinCount++
    |      +--> return BufferHandle
    |
    +-- kSpilled
    |      |
    |      +--> state = kLoading
    |      +--> allocate resident memory
    |      +--> ProcessSpillService.Read(location)
    |      +--> state = kLoaded
    |      +--> release old spill location
    |      +--> pinCount++
    |      +--> return BufferHandle
    |
    +-- kEvictedRecomputable
    |      |
    |      +--> state = kLoading
    |      +--> allocate resident memory
    |      +--> recoveryFn(data, size)
    |      +--> state = kLoaded
    |      +--> pinCount++
    |      +--> return BufferHandle
    |
    +-- kSpilling / kLoading
    |      |
    |      +--> wait on cv_
    |      +--> retry from current state
    |
    +-- kDiscarded / kInvalid
           |
           +--> return invalid BufferHandle
```

并发 reload 使用 `loadGeneration_` 和 `lastLoadError_`：

- 同一代 reload 失败时，等待者看到同一个异常。
- reload 成功后 `spillLocation_` 会被清空，并通过 spill service release 原 spill 文件/slot。

## Prefetch 流程

`Prefetch(blocks)` 是 best-effort 异步 reload 接口，用于把已经 spilled 的 block 提前读回 resident memory，但不 pin 住 block。它复用 `kLoading` 状态和 async completion 机制，因此和 `Pin()` 不会重复发起同一个 block 的读盘。

```text
Prefetch(blocks)
    |
    +--> DrainPrefetchCompletions()
    |
    +--> for each block:
            |
            +-- kLoaded
            |      |
            |      +--> alreadyLoadedCount++
            |
            +-- kSpilled
            |      |
            |      +--> BlockHandle.PrepareAsyncPrefetch()
            |      |      - allocate resident AccountedMemory
            |      |      - state = kLoading
            |      |      - memory moves into PrefetchRequest
            |      |
            |      +--> ProcessSpillService.SubmitPrefetch(request)
            |      +--> submittedCount++
            |
            +-- other states
                   |
                   +--> skippedCount++

ProcessSpillService worker
    |
    +--> Read(spillLocation, request.memory)
    +--> PrefetchCompletion(owner, blockId, seq, memory, error)
    +--> NotifyProgress()

Owner thread later:

Prefetch({})
    |
    +--> DrainPrefetchCompletions()
    +--> CommitAsyncPrefetchSuccess()
           - memory moves back to BlockHandle
           - state = kLoaded
           - old spill location is released
           - block is resident but pinCount is unchanged
```

`Prefetch({})` 是显式 drain-only 调用。带 block 的 `Prefetch(blocks)` 不会把“已经完成但还没 drain 的 prefetch completion”混入本次提交结果，避免 repeated prefetch 的去重语义变成竞态。

## SpillStore 落盘路径

每次写 spill 都会先准备 payload：

1. 如果配置启用 zstd 且满足 `minBytes`，尝试压缩。
2. 如果压缩后没有达到 `minSavingsRatio`，回退 raw payload。
3. 用 `storedBytes` 选择小块 slot 或 dedicated file。
4. 通过 `DiskIoScheduler::SubmitAndWait()` 执行写。

```text
SpillStore.Write(logical bytes)
        |
        v
+--------------------------------------------+
| compression enabled and bytes >= minBytes? |
+--------------------------------------------+
        | yes                         | no
        v                             v
try ZSTD compress                 raw payload
        |
        v
+-------------------------------+
| compressed and saved enough ? |
+-------------------------------+
        | yes                         | no
        v                             v
zstd payload                    raw payload
storedBytes = compressed size   storedBytes = logical size
        |                             |
        +-------------+---------------+
                      |
                      v
        +-----------------------------------------+
        | storedBytes fits a small size class ?   |
        +-----------------------------------------+
             | yes                         | no
             v                             v
  SmallSpillAllocator slot          dedicated file
  path + offset + slotBytes         bm_<tag>_<id>.spill
             |                             |
             +-------------+---------------+
                           |
                           v
              DiskIoScheduler write
                           |
                           v
                   return SpillLocation
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

## 小块 Spill 文件布局

默认小块策略由磁盘类型决定：

- dedicated file threshold 默认 4 MiB。
- size classes 默认：4 KiB、8 KiB、16 KiB、32 KiB、64 KiB、128 KiB、256 KiB、512 KiB、1 MiB、2 MiB、4 MiB。
- slab file size 默认：
  - NVME：256 MiB
  - SSD：128 MiB
  - HDD / NetworkFS / Unknown：64 MiB

小块写入时：

- 根据 `storedBytes` 找第一个 `slotBytes >= storedBytes` 的 size class。
- 从该 class 的 slab 文件中找 free slot 或 next slot。
- 没有可用 slot 时新建 `bm_small_<slotBytes>_<id>.spill`。
- release 时 slot 进入 free list；如果 slab 所有 slot 都释放，整个 slab file 会删除。

大块或不匹配 size class 的写入使用 dedicated file：

```text
bm_<tag>_<id>.spill
```

## Disk Probe 和 Disk I/O

`ProcessSpillService` 初始化时创建 `SpillStore`，并先执行 `ProbeDisk()`：

- 如果 `forcedKind != kUnknown`，直接使用 forced kind。
- 如果 `diskProbeDuration <= 0`，不做 active probe，使用 fallback kind。
- 否则用 `O_DIRECT` 临时文件测写/读 IOPS，并按阈值分类：
  - min(writeIops, readIops) >= `nvmeMinIops`：NVME
  - >= `ssdMinIops`：SSD
  - 否则 HDD

Disk I/O 层：

- I/O engine 固定使用 io_uring，不再提供 sync backend。
- `DiskIoScheduler` 有 priority weights，读默认 high，写默认 low。
- `AdaptiveQueueDepth` 根据窗口内 latency percentile 和 throughput 变化调节 queue depth。当前 `SubmitAndWait()` 是同步执行请求，但 scheduler 仍记录 completion 并维护 adaptive 状态。

## 配置入口

进程级服务通过：

```cpp
BufferManager::InitializeProcessServices(BufferManagerProcessServicesConfig)
```

配置内容包括：

- spill 是否启用。
- spill execution mode：owner thread 或 worker thread。
- spill directory。
- worker thread count。
- disk probe。
- disk I/O ring entries 和 queue depth。
- small spill policy。
- compression policy。
- metrics sink。

每个 `BufferManager` 自己的构造配置包括：

- pool name。
- reclaim 等待 async progress 的 timeout。
- metrics sink。
- 是否允许 spill policy block。

`EvictPolicy::kSpillToDisk` 需要进程级 spill service 先初始化，否则分配会抛 `BoltUserError`。

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
- `bm_spill_executed_total`
- `bm_spill_freed_bytes_total`
- `bm_spill_bytes_written{disk=...}`
- `bm_spill_bytes_stored{disk=...}`
- `bm_spill_bytes_read{disk=...}`
- `bm_spill_small_slot_total{disk=...}`
- `bm_spill_dedicated_file_total{disk=...}`
- `bm_spill_compress_attempt_total{disk=...}`
- `bm_spill_compress_saved_bytes{disk=...}`
- `bm_spill_compress_fallback_raw_total{disk=...}`

日志分层：

- `INFO`：BufferManager 创建/销毁，Reclaim 汇总，ProcessSpillService/SpillStore 初始化，DiskProbe 结果。
- `VLOG(1)`：block allocate/pin/unpin/enqueue/spill、SpillStore 写入细节、压缩 fallback 等高频路径。
- `WARNING`：reload/spill 失败、cleanup 失败、double release 等异常但可恢复事件。

## 当前行为和注意点

1. `BufferPool` 只统计，不限额。真正的“是否该释放内存”由外部 quota/arbitrator 通过 `Reclaim()` 触发。
2. `BufferManager` 是单线程使用模型；不要从非 owner thread 调它的 public API。
3. worker-thread spill worker 不提交 block 状态，必须由 owner thread drain completion 后释放 resident bytes。
4. `Reclaim()` 返回实际释放字节，不返回已提交 spill 的字节。
5. `kPinnedForever` 不进入 eviction queue，使用 `ReservationKind::kPinned` 统计。
6. `kDiscard` 被回收后 pin 返回 invalid handle；`kRecompute` 被回收后 pin 调 recoveryFn。
7. 默认 spill 会尝试压缩，但达不到节省比例会 raw fallback。
8. 小块 spill 按压缩后的 `storedBytes` 选择 slot，因此压缩会影响小块/大块路径。
9. `spill.executionMode == kWorkerThread` 要求 `workerThreadCount > 0`，并且不会 fallback 到 owner-thread spill。
10. `Snapshot().usedLoadedBytes / usedSpilledBytes` 是按 live block 状态额外汇总；`usedTotalBytes` 来自 BufferPool，表示当前 resident logical bytes。
