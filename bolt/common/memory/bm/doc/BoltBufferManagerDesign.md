# Bolt BufferManager 接入设计

## 目标

在不改动 Bolt 现有 `MemoryAllocator` / `MemoryPoolImpl::allocate/free` 热路径的前提下，将 `bm/doc/BufferManagerImpl.md` 中的 BufferManager 思路以可演进的方式接入 Bolt。

第一阶段落地范围是一个轻量 MVP：

1. `BufferManager` 作为 `MemoryManager` 的可选子组件，也支持独立构造。
2. 所有实际内存分配都走现有 `MemoryPool`，继续复用 Bolt 的 reservation、arbitration、stats 与 debug 能力。
3. `BufferPool` 只作为 BufferManager 内部统一计费事实源，提供 `ReservationKind`、tag 维度 usage、pinned/scratch 计费和 RAII reservation。
4. `BlockHandle` / `BufferHandle` 提供 pin/unpin、初始写窗口、sealed immutable，以及 `kDiscard` / `kRecompute` 两种同步 eviction。
5. SpillStore、异步 SpillScheduler、压缩和介质探测后续在同一接口下增量补齐，不先侵入 Bolt 现有 spill 代码。

## 与 Bolt 现有设计的关系

Bolt 现有内存系统分层为：

```text
MemoryManager
  ├── MemoryAllocator
  ├── MemoryArbitrator
  └── MemoryPool tree
```

BufferManager 接入后保持这个分层不变：

```text
MemoryManager
  ├── MemoryAllocator
  ├── MemoryArbitrator
  ├── MemoryPool tree
  └── optional bm::BufferManager
        ├── root MemoryPool: __buffer_manager__
        ├── leaf MemoryPool: blocks
        ├── bm::BufferPool logical quota
        ├── bm::BufferAllocator / AccountedMemory
        └── bm::BlockHandle registry
```

关键约束：

- 不让 BufferManager 直接调用 `MemoryAllocator`，避免绕过 `MemoryPool` 的统计与仲裁。
- 不修改 `MemoryPoolImpl::allocate/free`，避免影响现有算子热路径。
- BufferManager root pool 通过 `MemoryManager::addRootPool()` 创建，因此天然进入 `MemoryArbitrator` 的 pool 集合。
- BufferManager root pool 挂 `MemoryReclaimer`，仲裁器可以通过该 reclaimer 触发 BufferManager 回收未 pinned block。

## 第一阶段接口

主要入口在 `bolt/common/memory/bm/BufferManager.h`：

```cpp
BufferHandle Allocate(AllocateOptions);
std::shared_ptr<BlockHandle> AllocatePersistent(AllocateOptions, init);
BufferHandle Pin(const std::shared_ptr<BlockHandle>&);
std::unique_ptr<AccountedMemory> AllocateMemory(MemoryTag, ByteCount, ReservationKind);
BufferPoolReservation ReserveMemory(MemoryTag, ByteCount, ReservationKind);
ByteCount Reclaim(ByteCount targetBytes);
BufferPoolSnapshot Snapshot() const;
```

`MemoryManager` 新增可选开关：

```cpp
MemoryManager::Options options;
options.enableBufferManager = true;
options.bufferManagerCapacity = ...;
```

未开启时 `MemoryManager::bufferManager()` 返回 `nullptr`，避免对现有用户产生行为变化。

## 计费模型

MVP 中有两层计费：

1. Bolt 真实物理内存计费：`AccountedMemory` 构造时调用 BufferManager leaf `MemoryPool::allocate()`，析构时调用 `MemoryPool::free()`。
2. BufferManager 逻辑计费：`BufferPoolReservation` 记录 tag、normal/pinned/scratch/emergency scratch 维度 usage。

两层计费边界明确：

- 真实内存上限和 Bolt 仲裁仍由 `MemoryPool` 负责。
- BufferManager 内部策略、snapshot、operator budget 输入由 `BufferPool` 负责。
- `AccountedMemory` 是 block 主体内存唯一 owner；释放 `AccountedMemory` 时两层计费同时下降。

## 生命周期

`MemoryManager` 析构时先 reset 可选 `BufferManager`，再 shutdown arbitrator 和做 pool leak check。这样 BufferManager root pool 会先被释放，并从 `MemoryManager::pools_` 中 drop，避免 leak check 误报。

`BufferManager` 析构流程：

1. 设置 `shuttingDown_`。
2. 遍历 weak block registry，将仍然存活的 block 标记为 `kInvalid`。
3. 释放 leaf pool。
4. 释放 root pool，触发 MemoryManager 的 dropPool callback。

## 当前支持的 block 状态

```text
kLoaded
  ├── Reclaim(kDiscard)   -> kDiscarded
  └── Reclaim(kRecompute) -> kEvictedRecomputable

kEvictedRecomputable
  └── Pin + recoveryFn    -> kLoaded
```

第一阶段暂不引入 `kSpilling` / `kLoading` / `kCompressed`。后续增加 spill 时，状态机应按原 `BufferManagerImpl.md` 保持：spill in-flight 时原 buffer 仍在内存，新 Pin 等待，只有 spill commit 后才能释放主体内存。

## 后续演进细化

本节把 `BufferManagerImpl.md` 中的完整设计拆成可以在 Bolt 当前代码中安全落地的增量。原则仍然是：优先复用 Bolt `MemoryPool` 和 `MemoryArbitrator`，BufferManager 只管理可驱逐 block 的生命周期与策略。

### 1. SpillStore MVP

第一版 `SpillStore` 不接入 Bolt exec spill 文件格式，也不引入多目录调度；它只负责 BufferManager block 的字节级落盘：

```text
SpillStoreConfig
  spillDir: 用户指定或系统临时目录下的 bm_spill
  cleanupOnDestroy: BufferManager 析构时是否清理本 session 生成的 spill 文件

SpillLocation
  path
  logicalBytes
  storedBytes
  compressionCodec = 0
```

写入流程：

1. 为每次 write 生成唯一文件名，避免多 BufferManager 实例冲突。
2. 同步写完整 block bytes，并在 close 后返回 `SpillLocation`。
3. 任何短写、open 失败、close 失败都抛异常；上层保持原 block resident，不释放内存。

读取流程：

1. 校验目标 buffer 容量不小于 `logicalBytes`。
2. 同步读满 `logicalBytes`。
3. 读失败时抛异常，`BlockHandle` 状态恢复为 `kSpilled`，后续允许重试。

Release 流程：

- 对本 session 生成的文件执行 best-effort remove。
- double release 在生产路径中幂等，并打印 warning 方便定位生命周期问题。

### 2. BlockHandle 同步 spill / reload 状态机

新增状态：

```text
kLoaded
  ├── kDiscard              -> kDiscarded
  ├── kRecompute            -> kEvictedRecomputable
  └── kSpillToDisk          -> kSpilling -> kSpilled

kSpilled
  └── Pin                   -> kLoading -> kLoaded

kEvictedRecomputable
  └── Pin + recoveryFn      -> kLoading -> kLoaded
```

关键语义：

- `kSpilling` 期间原 `AccountedMemory` 仍然保留，不能把 scheduled/spilling 当作 freed。
- `Pin(kSpilling)` 等待 condition variable，直到状态变为 `kSpilled` 或回到 `kLoaded`。
- spill 成功后才释放 `AccountedMemory`，此时 BufferManager 逻辑计费和 Bolt `MemoryPool` 真实计费一起下降。
- reload/recompute 的耗时操作不持有 block mutex，避免阻塞其他 block 操作。
- reload 失败后状态恢复为 `kSpilled`，错误抛给当前 caller，后续 Pin 可重试。

### 3. SpillScheduler 与 progress epoch

第二步在同步 spill 能力之上增加后台调度器：

```text
SpillScheduler
  pending queue<weak_ptr<BlockHandle>>
  worker threads
  progressEpoch
  condition_variable
```

`Submit()` 语义：

- 返回 `false`：scheduler 已 shutdown 或 pending 队列满，调用方可退化为同步 reclaim 或 OOM。
- 返回 `true`：candidate 已进入 pending；此时不代表内存已释放。

worker 语义：

1. 从 pending queue 取出 block。
2. lazy 校验 weak_ptr、pin count、状态和 policy。
3. 调用 `BlockHandle::SpillToDisk()`。
4. 无论成功/失败都推进 `progressEpoch` 并 `notify_all`，避免 Reserve 慢路径 lost wakeup。

Reserve 慢路径可以使用 `WaitForProgress(timeout)` 等待后台释放内存，但必须先释放 `BufferPool` 内部锁。

### 4. 压缩与介质 profile

当前实现先预留接口而不引入真实压缩依赖：

- `EvictPolicy::kCompressThenSpill` 在 MVP 中等价于 `kSpillToDisk`，但 `SpillLocation::compressionCodec` 保留为 0。
- 后续接入压缩时，压缩 scratch 必须使用 `AllocateEmergencyScratch()`，不能走用户公开入口。
- 介质 profile 第一阶段只记录 `spillDir`；后续多目录时，目录选择、限流和介质探测全部收敛在 `SpillStore`，`BlockHandle` 不出现 HDD/NVMe 分支。

### 5. TemporaryMemoryManager

`TemporaryMemoryManager` 是 advisory budget 组件，不做硬 reservation，也不直接分配内存。它只基于 `BufferPoolSnapshot` 计算建议：

```text
TemporaryMemoryState
  tag
  estimatedRemainingBytes
  minimumReservationBytes
  reservation
  shouldExternalize
  shouldWait
```

计算规则：

1. `availableForOperators` 来自 `BufferPoolSnapshot`，是唯一预算输入。
2. 若 available 足够，按活跃 state 数平均分配，且不超过 estimated remaining。
3. 若无法满足 minimum，则返回 `shouldExternalize=true`，提示算子尽早外部化。
4. BufferManager 析构时 invalidate 所有 state；invalidated 后 reservation 为 0，析构不访问已销毁 manager。

### 6. 日志与注释策略

设计文档不会进入最终代码库时，代码必须自解释：

- 状态转换必须打印 `BOLT_MEM_LOG(INFO)` 或 `WARNING`，包含 block id、size、old/new state、policy。
- reserve/release、spill write/read/release、scheduler submit/progress、TMM register/unregister 都打印关键日志。
- 注释重点解释“为什么这样做”，尤其是：为什么 `kSpilling` 不释放内存、为什么 reload 不持锁做 I/O、为什么公开入口禁止 scratch reservation。
