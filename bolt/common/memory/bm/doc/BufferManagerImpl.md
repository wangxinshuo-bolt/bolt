# 新 OLAP 系统 BufferManager 设计与实现文档

## 1. 背景与目标

本文档描述如何在一个新的 OLAP 系统中实现一套参考 DuckDB 思路、但不强绑定 DuckDB 代码结构的 BufferManager。它的核心职责是：

1. 在全局内存上限内管理数据块、临时内存、算子工作内存与 spill scratch。
2. 支持 block pin/unpin、不可变 block、压缩、丢弃、重算与磁盘 spill。
3. 在内存压力下自动选择 victim，并通过同步释放或异步 spill 回收内存。
4. 为 hash/sort/aggregate 等 OLAP 算子提供 advisory temporary memory budget。
5. 提供可测试、可观测、可演进的接口和状态机，避免生命周期、并发和计费错误。

最初的文档按多个 Agent 并行实现拆分模块。重构后，本文不再以 Agent 划分作为设计前提，而是以“能够正确实现一套完整 BufferManager”为目标组织内容。实际工程中仍可以并行开发，但并行边界不是本文档的核心。

---

## 2. 设计原则

### 2.1 强约束优先

BufferManager 最容易出问题的地方不是单个接口，而是跨组件语义不闭环，例如：

- 内存 reservation 重复计费或漏计费。
- 异步 spill 已经排队但内存尚未释放，却被错误当作 freed。
- block 正在 kSpilling 时被 Pin 直接读取到不稳定 buffer。
- BufferManager 析构后外部残留 handle 访问已销毁的 BufferPool。
- spill 需要 scratch，而 scratch reserve 又递归触发 eviction，造成死锁或 OOM 环。

因此本文优先冻结状态机、计费模型、生命周期、锁顺序和错误传播，再描述接口。

### 2.2 单一事实来源

- 内存配额的唯一事实来源是 `BufferPool`。
- block 主体内存的唯一 owner 是 `BlockBuffer` 中的 `AccountedMemory`。
- 压缩后内存的唯一 owner 是 `compressed_`。
- spilled block 的唯一磁盘位置记录是 `SpillLocation`。
- operator budget 的唯一输入是 `BufferPoolSnapshot`。

任何实现不得为同一块内存维护多份 reservation，也不得让不同模块各自推导不同的可用内存公式。

### 2.3 热路径简单，慢路径完整

Pin 已加载 block、Reserve 未超限、Unpin 普通 block 等热路径必须足够简单。复杂逻辑集中在慢路径：eviction、spill、reload、recompute、压缩、磁盘错误恢复和生命周期清理。

### 2.4 介质感知收敛在 SpillStore

HDD、SSD、NVMe、网络盘等差异只应影响磁盘写入策略、文件布局、并发度和压缩参数。上层 BufferPool、BlockHandle、GlobalSpillScheduler 不应出现 `if (MediumKind::kHdd)` 之类的分支。

### 2.5 Spill 存储层进程级单例

为了在多线程、多 BufferManager 共存的进程中统一控制 I/O 与磁盘配额，spill 存储层抽取为进程级单例 `ProcessSpillService`。它持有：
- 一组 `SpillStore`（多目录），负责磁盘读写与介质探测；
- 一个 `GlobalSpillScheduler`，提供 DRF-lite 公平调度与全局 progress epoch；
- 一个全局 disk quota，超额直接抛 `BoltUserError`。

每个 `BufferManager` 通过 `SpillClientConfig::enableSpill = true` 申请一个 `SpillClient` 视图，作为 per-tenant 的路由对象，同时实现 `SpillRequester` 接口被 `BlockEvictor` 持有。

---

## 3. 总体架构

```text
                              ┌────────────────────┐
                              │    BufferManager    │
                              │ facade / lifecycle  │
                              └──────────┬─────────┘
                                         │
            ┌────────────────────────────┼─────────────────────────────┐
            │                            │                             │
            ▼                            ▼                             ▼
    ┌───────────────┐          ┌────────────────┐            ┌────────────────┐
    │  BufferPool   │◀────────▶│  BlockHandle   │───────────▶│  SpillClient   │
    │ quota/evict q │          │ state machine  │            │ per-tenant view│
    └───────┬───────┘          └───────┬────────┘            └────────┬───────┘
            │                          │                              │
            ▼                          ▼                              ▼
    ┌───────────────┐          ┌────────────────┐            ┌─────────────────────┐
    │BufferAllocator│          │  BlockEvictor  │            │ ProcessSpillService │
    │ accounted mem │          │   queues + req │            │  (process-wide)     │
    └───────────────┘          └────────┬───────┘            │  - SpillStore[]     │
            │                           │                    │  - GlobalSpill-     │
            ▼                           └───────────────────▶│      Scheduler      │
    ┌──────────────────────┐                                 │  - disk quota       │
    │TemporaryMemoryManager│                                 └─────────────────────┘
    │ advisory op budget   │
    └──────────────────────┘

横向能力：Common types、Metrics、测试与故障注入。
```

各组件职责：

| 组件 | 核心职责 |
| --- | --- |
| `BufferManager` | 用户入口、对象组装、生命周期管理、析构安全线 |
| `BufferPool` | 全局配额、reservation 计费、eviction candidate 队列 |
| `BufferAllocator` | 创建带配额的 `AccountedMemory` 与 scratch memory |
| `BlockHandle` / `BufferHandle` | block 状态机、pin/unpin、reload、压缩、spill、sealed immutable 语义 |
| `BlockEvictor` | per-cost / per-priority 队列、同步 evict、转发 spill 至 `SpillRequester` |
| `ProcessSpillService` | 进程级单例：拥有 `SpillStore` 集合、`GlobalSpillScheduler`、disk quota |
| `SpillClient` | per-tenant 视图，实现 `SpillRequester`，stamp `clientId` 并代理 Write/Read/Release |
| `GlobalSpillScheduler` | DRF-lite 公平调度、worker pool、全局 progress epoch |
| `SpillStore` | 磁盘 spill 文件、目录选择、介质探测、压缩、读写和清理 |
| `TemporaryMemoryManager` | 算子级 advisory memory budget |
| `Metrics` | counter/gauge/histogram、NoOp 与 mock 注入 |

---

## 4. 基础类型与全局枚举

```cpp
using ByteCount = uint64_t;
using DataPtr = uint8_t*;
using ConstDataPtr = const uint8_t*;

enum class MemoryTag : uint8_t {
    kMetadata,
    kHashTable,
    kSort,
    kShuffle,
    kScanCache,
    kOperatorState,
    kExtension,
    kInternal,
    kNumTags,
};

enum class ReservationKind : uint8_t {
    kNormal,
    kPinned,
    kScratch,
    kScratchEmergency,
};

enum class EvictPolicy : uint8_t {
    kSpillToDisk,
    kDiscard,
    kRecompute,
    kCompressThenSpill,
    kPinnedForever,
};

enum class Priority : uint8_t {
    kLow,
    kNormal,
    kHigh,
    kCritical,
};

enum class EvictionCostClass : uint8_t {
    kFreeOrCheap,
    kCompress,
    kSpill,
};

enum class BlockState : uint8_t {
    kInvalid,
    kLoaded,
    kLoading,
    kSpilling,
    kCompressed,
    kSpilled,
    kDiscarded,
    kEvictedRecomputable,
};

enum class EvictResultKind : uint8_t {
    kFreed,
    kScheduled,
    kBackpressured,
    kSkipped,
    kFailed,
};

struct EvictResult {
    EvictResultKind kind;
    ByteCount freed_bytes;
};

enum class MediumKind : uint8_t {
    kUnknown,
    kHdd,
    kSsd,
    kNvme,
    kNetworkFs,
};
```

关键语义：

- `kNormal`：普通大内存，计入 `used_total_bytes` 和 `used_bytes_by_tag`。
- `kPinned`：hard pinned memory，计入 `used_total_bytes` 和 `used_pinned_bytes`。
- `kScratch`：普通内部 scratch，计入 `used_total_bytes` 和 `used_scratch_bytes`。
- `kScratchEmergency`：仅用于 spill/compress 关键路径，计入 `used_total_bytes`、`used_scratch_bytes`、`used_emergency_scratch_bytes`，不得触发 eviction 慢路径。
- `MemoryTag::kInternal` 是归因标签，不等价于 `ReservationKind::kScratch`。
- `EvictResultKind::kScheduled` 和 `kBackpressured` 都不代表内存已经释放。

---

## 5. 内存计费模型

### 5.1 BufferPool 维护的指标

`BufferPool` 维护以下配额与用量：

```text
memory_limit_bytes
operator_memory_limit_bytes = memory_limit_bytes - emergency_scratch_bytes
pinned_limit_bytes
emergency_scratch_bytes

used_total_bytes
used_pinned_bytes
used_scratch_bytes
used_emergency_scratch_bytes
used_bytes_by_tag[MemoryTag]
```

恒等式：

```text
used_total_bytes = used_normal + used_pinned_bytes + used_scratch_bytes
used_emergency_scratch_bytes ⊂ used_scratch_bytes
used_pinned_bytes            ⊂ used_total_bytes
used_scratch_bytes           ⊂ used_total_bytes
```

### 5.2 Reserve 规则

```text
Reserve(tag, bytes, kind):

1. kind == kPinned:
   - 如果 used_pinned_bytes + bytes > pinned_limit_bytes，直接 OOM。
   - pinned 维度通过后，仍按普通内存检查 used_total_bytes。

2. kind == kScratchEmergency:
   - 如果 used_emergency_scratch_bytes + bytes > emergency_scratch_bytes，直接 OOM。
   - 如果 used_total_bytes + bytes > memory_limit_bytes，直接 OOM。
   - 不进入 eviction 慢路径。

3. kind == kNormal / kPinned / kScratch:
   - target_limit = operator_memory_limit_bytes。
   - 如果 used_total_bytes + bytes <= target_limit，直接成功。
   - 否则进入 eviction 慢路径。
```

`operator_memory_limit_bytes` 保留 emergency scratch headroom，避免普通算子吃掉 spill/compress 必需的临时空间。

### 5.3 Reservation RAII

```cpp
class QuotaSink {
public:
    virtual ~QuotaSink() = default;
    virtual BufferPoolReservation Reserve(MemoryTag tag, ByteCount bytes,
                                          ReservationKind kind) = 0;
    virtual void Release(MemoryTag tag, ByteCount bytes,
                         ReservationKind kind) noexcept = 0;
};

class BufferPoolReservation {
public:
    BufferPoolReservation();
    BufferPoolReservation(QuotaSink* sink, MemoryTag tag, ByteCount bytes,
                          ReservationKind kind);
    ~BufferPoolReservation();

    BufferPoolReservation(BufferPoolReservation&&) noexcept;
    BufferPoolReservation& operator=(BufferPoolReservation&&) noexcept;
    BufferPoolReservation(const BufferPoolReservation&) = delete;
    BufferPoolReservation& operator=(const BufferPoolReservation&) = delete;

    void Resize(ByteCount new_bytes);
    ByteCount Size() const;
    MemoryTag Tag() const;
    ReservationKind Kind() const;
};
```

实现要求：

- move 后源对象 `Size()==0`，析构无副作用。
- move assignment 覆盖已有 reservation 前必须释放旧 reservation。
- `Resize` 增大失败时原 reservation 不变。
- `ReservationKind` 必须随 reservation 一起 move、resize、release。

---

## 6. AccountedMemory 与 BufferAllocator

`AccountedMemory` 是“普通 malloc/free 用法 + BufferPool 配额计费”的 RAII 包装。

```cpp
class AccountedMemory {
public:
    static std::unique_ptr<AccountedMemory> Make(QuotaSink&, MemoryTag,
                                                 ByteCount bytes,
                                                 ReservationKind kind);
    ~AccountedMemory();

    DataPtr Data();
    ConstDataPtr Data() const;
    ByteCount Size() const;
};

class ScratchAllocator {
public:
    virtual ~ScratchAllocator() = default;
    virtual std::unique_ptr<AccountedMemory> AllocateScratch(MemoryTag tag,
                                                            ByteCount bytes) = 0;
    virtual std::unique_ptr<AccountedMemory> AllocateEmergencyScratch(MemoryTag tag,
                                                                     ByteCount bytes) = 0;
};

class BufferAllocator : public ScratchAllocator {
public:
    BufferAllocator(QuotaSink& sink, MetricsRegistry* metrics = nullptr);

    std::unique_ptr<AccountedMemory> Allocate(
        MemoryTag tag,
        ByteCount bytes,
        ReservationKind kind = ReservationKind::kNormal);

    std::unique_ptr<AccountedMemory> AllocateScratch(MemoryTag tag,
                                                     ByteCount bytes) override;
    std::unique_ptr<AccountedMemory> AllocateEmergencyScratch(MemoryTag tag,
                                                              ByteCount bytes) override;
};
```

约束：

- `AllocateScratch` 固定使用 `ReservationKind::kScratch`。
- `AllocateEmergencyScratch` 固定使用 `ReservationKind::kScratchEmergency`。
- 普通算子和扩展不得通过 emergency scratch 旁路配额。
- block 主体内存不走 scratch，必须是 `kNormal` 或 `kPinned`。

---

## 7. Eviction 队列与 Reserve 慢路径

### 7.1 EvictionNode

```cpp
class BlockHandleBase : public std::enable_shared_from_this<BlockHandleBase> {
public:
    virtual ~BlockHandleBase() = default;
    virtual uint64_t EvictionSequence() const = 0;
};

struct EvictionNode {
    std::weak_ptr<BlockHandleBase> block;
    uint64_t eviction_sequence;
    int64_t enqueue_time_ms;
};
```

`EvictionNode` 是 append-only candidate。队列允许 stale node，真正驱逐时必须 double-check。

### 7.2 队列组织

```text
queues[cost_class][priority][shard]
```

扫描顺序：

```text
kFreeOrCheap -> kCompress -> kSpill
kLow -> kNormal -> kHigh -> kCritical
```

含义是先牺牲低优先级、低成本 block；最后才考虑高优先级、需要写盘的 block。

### 7.3 Evictor

```cpp
class Evictor {
public:
    virtual ~Evictor() = default;

    // 同步路径只允许返回 kFreed / kSkipped / kFailed。
    virtual EvictResult TryEvictNodeSync(const EvictionNode&) = 0;

    // 异步 spill 路径只允许返回 kScheduled / kBackpressured / kSkipped / kFailed。
    virtual EvictResult TryScheduleEvict(const EvictionNode&) = 0;

    virtual bool WaitForProgress(ByteCount bytes_needed,
                                 std::chrono::milliseconds timeout) = 0;
};
```

### 7.4 Reserve 慢路径

```text
while used_total_bytes + bytes > target_limit:
    evictor = atomic_load(evictor_)
    if evictor == nullptr:
        throw OutOfMemoryError

    if TryPopAnyCandidate(node, cost):
        if cost == kSpill:
            result = evictor->TryScheduleEvict(node)
        else:
            result = evictor->TryEvictNodeSync(node)

        if result.kind == kFreed:
            continue

        if result.kind == kScheduled or kBackpressured:
            release all BufferPool locks
            ok = evictor->WaitForProgress(bytes, reserve_wait_timeout)
            if !ok: throw OutOfMemoryError
            continue

        if result.kind == kSkipped or kFailed:
            continue
    else:
        release all BufferPool locks
        ok = evictor->WaitForProgress(bytes, reserve_wait_timeout)
        if !ok: throw OutOfMemoryError
        continue
```

必须遵守：

- 只有 `kFreed` 可以视为释放了内存。
- `kScheduled` 表示 spill 已提交或即将启动，但内存尚未释放。
- `kBackpressured` 表示 scheduler 已接收 candidate 到内部 pending/delayed retry 队列，但暂时不能启动 I/O；Reserve 端不得重新 enqueue 该 node。
- 调用 `WaitForProgress` 前必须释放 BufferPool 内部锁，防止 worker 完成后无法 `Release` 而死锁。

---

## 8. Block 与 BufferHandle 状态机

### 8.1 对外对象

```cpp
struct AllocateOptions {
    MemoryTag tag = MemoryTag::kHashTable;
    ByteCount size = 0;
    EvictPolicy policy = EvictPolicy::kSpillToDisk;
    Priority priority = Priority::kNormal;
    std::function<void(DataPtr, ByteCount)> recovery_fn;
};

class BufferHandle {
public:
    BufferHandle();
    BufferHandle(BufferHandle&&) noexcept;
    BufferHandle& operator=(BufferHandle&&) noexcept;
    BufferHandle(const BufferHandle&) = delete;
    BufferHandle& operator=(const BufferHandle&) = delete;
    ~BufferHandle();

    bool IsValid() const;
    ConstDataPtr Data() const;
    DataPtr MutableData();
    ByteCount Size() const;
    const std::shared_ptr<BlockHandle>& Block() const;
};
```

`BufferHandle` 是 pin 的 RAII owner。析构时自动 unpin。

### 8.2 sealed immutable 契约

```text
1. Allocate 返回的初始 BufferHandle 是唯一写窗口。
2. AllocatePersistent(init) 的 init 回调是唯一写窗口。
3. 初始写 handle 第一次 Unpin 后，block sealed。
4. sealed block 后续 Pin 只能读。
5. 需要修改时，必须创建新 block，或使用算子自己的 mutable arena。
```

实现上 `BufferHandle` 内部携带私有字段 `is_initial_write_`：

- 初始 handle：`is_initial_write_ = true`。
- 普通 Pin 返回的 handle：`is_initial_write_ = false`。
- `MutableData()` 只有在 `is_initial_write_ && !block->IsSealed()` 时返回指针，否则抛 `InvalidArgumentError` 或 debug DCHECK。

### 8.3 Block 内存 owner

```text
kLoaded:
    BlockBuffer::memory_ 是唯一主体内存 owner。

kCompressed:
    compressed_ 是唯一压缩后内存 owner，原 BlockBuffer 已释放。

kSpilled / kDiscarded / kEvictedRecomputable:
    不持有 block 主体内存 reservation。

kInvalid:
    BufferManager 析构时已释放资源 owner，外部残留 shared_ptr 不得访问 BufferPool。
```

`BlockHandle` 不应再保存额外的 `initial_charge` 或 `charge_` 计费同一块主体内存。

### 8.4 状态转换

```text
Allocate:
    new block -> kLoaded

Unpin 到 0:
    kLoaded -> enqueue eviction candidate
    kPinnedForever 不入队

Evict kDiscard:
    kLoaded -> kDiscarded, release BlockBuffer

Evict kRecompute:
    kLoaded -> kEvictedRecomputable, release BlockBuffer

Evict kCompress:
    kLoaded -> kCompressed, release BlockBuffer, install compressed_
    kCompressed 可重新 enqueue 到 kSpill cost class

Async spill:
    kLoaded/kCompressed -> kSpilling -> kSpilled
    失败则恢复 kLoaded/kCompressed，并生成新 eviction_sequence 后重新入队

Pin kSpilled:
    kSpilled -> kLoading -> kLoaded

Pin kCompressed:
    kCompressed -> kLoading -> kLoaded

Pin kEvictedRecomputable:
    kEvictedRecomputable -> kLoading -> kLoaded
```

### 8.5 kSpilling 状态

`kSpilling` 的含义必须固定：

- 写出 in-flight。
- 原 buffer 仍在内存中，直到 spill 成功提交状态后才释放。
- `pin_count == 0`。
- 新 Pin 一律等待 condition variable。
- 不做 copy-on-write，不返回当前 buffer。
- spill 成功：安装 `SpillLocation`，释放 buffer，状态变为 `kSpilled`。
- spill 失败：保留 buffer，恢复 resident 状态，生成新 `eviction_sequence` 并重新入队。

### 8.6 kLoading 失败传播

`kLoading` 包括 read from spill、decompress、recompute。耗时操作必须释放 block lock。

```text
1. 发起线程在锁内把状态改为 kLoading，记录 load_generation。
2. 解锁后执行 Reserve / Read / Decompress / Recovery。
3. 成功后重锁 double-check 并安装新 buffer。
4. 失败后重锁恢复旧状态，记录 last_load_error，notify_all。
5. 等待线程如果看到当前 load_generation 的 last_load_error，必须传播同一错误，不能立即无条件重试。
```

---

## 9. Eviction 策略

### 9.1 BlockEvictor 调度前校验

对任何 candidate，驱逐前必须执行：

```text
1. weak_ptr.lock 失败 -> kSkipped。
2. 类型不是 BlockHandle -> kSkipped。
3. node.eviction_sequence != block.EvictionSequence() -> kSkipped。
4. pin_count != 0 -> kSkipped。
5. state 不可驱逐 -> kSkipped。
```

### 9.2 同步驱逐分派

```text
kDiscard:
    EvictToDiscard -> kFreed

kRecompute:
    EvictToRecomputable -> kFreed

kCompressThenSpill 且 state == kLoaded:
    EvictToCompressed -> kFreed 原 BlockBuffer，安装 compressed_

kSpillToDisk 或 kCompressThenSpill 且 state == kCompressed:
    默认不走同步路径，由 TryScheduleEvict 提交异步 spill

kPinnedForever:
    kSkipped
```

### 9.3 kPinnedForever

`kPinnedForever` 不通过伪造 `pin_count` 实现。

- Allocate 时主体内存使用 `ReservationKind::kPinned`。
- `pin_count` 只表示当前用户活跃 handle 数，可以降到 0。
- Unpin 到 0 后不入 eviction queue。
- 若 BlockEvictor 收到 stale node，也必须返回 `kSkipped`。
- 受 `pinned_limit_bytes` 限制。

---

## 10. SpillStore 设计

### 10.1 对上接口

```cpp
struct SpillLocation {
    SpillDirId  dir_id;
    SpillFileId file_id;
    SpillSlotId slot_id;
    uint64_t offset;
    ByteCount logical_bytes;
    ByteCount stored_bytes;
    ByteCount allocated_bytes;
    uint8_t compression_codec;
    bool is_blob;
};

class SpillWriteSession {
public:
    SpillWriteSession(SpillWriteSession&&) noexcept;
    SpillWriteSession& operator=(SpillWriteSession&&) noexcept;
    ~SpillWriteSession() noexcept;

    SpillLocation Write(ConstDataPtr src, ByteCount bytes);
};

class SpillStore {
public:
    SpillStore(SpillStoreConfig cfg, MetricsRegistry* metrics = nullptr);

    SpillWriteSession BeginWriteAttempt(MemoryTag tag, bool allow_compression);
    SpillLocation Write(MemoryTag tag, ConstDataPtr src, ByteCount bytes,
                        bool allow_compression);
    void Read(const SpillLocation&, DataPtr dst, ByteCount dst_capacity);
    void Release(const SpillLocation&);

    static void CleanupAtStartup(const SpillStoreConfig&);
    MediumSummary GetMediumSummary() const;
    Stats GetStats() const;
};
```

### 10.2 目录、介质与 profile

每个 spill dir 在构造期确定 effective `MediumKind` 和 `MediumProfile`：

```text
1. 如果用户 forced_kind，直接使用。
2. 否则调用 DiskProbe。
3. Probe 失败或 kUnknown 时，使用 unknown_fallback_kind，默认 kHdd。
4. profile = profile_override 或 MediumProfileRegistry::DefaultFor(kind)。
5. 构造 PerDirState：profile、token bucket、统计和 metrics label。
```

热路径不得再次 probe、statfs 或读取 sysfs。

### 10.3 写入策略

- `BeginWriteAttempt` 选择本次 spill attempt 的目标目录。
- 同一个 `SpillWriteSession` 内部所有 chunk sticky 到同一目录。
- 每次 `Write()` 入口 acquire 该目录的 I/O token，出口通过 RAII 释放。
- `SpillWriteSession` 析构不得触发 I/O，不得抛异常。
- 如果需要 small-write coalescing，flush/finalize 必须在 `Write()` 同步完成。

### 10.4 磁盘错误处理

- `Read` 容量不足抛 `InvalidArgumentError`。
- 短读、解压大小不符、校验失败抛 `SpillIOError`。
- `Release` double release 在生产环境幂等并记录 metric；invalid location 抛错或 debug DCHECK。
- ENOSPC / 连续 I/O error 使目录进入 backoff 或 quarantine，disk selector 降低其优先级。
- `AllocateEmergencyScratch` 抛 OOM 时应被标记为 transient scratch pressure，上层 scheduler 不得把它当成不可恢复磁盘错误。

### 10.5 Cleanup

`CleanupAtStartup` 只扫描配置目录下 prefix 匹配的旧 session 目录，并且只有成功获取目录 lock 后才删除。不得仅凭 pid 判断 stale，也不得误删其他进程的 session。

---

## 11. ProcessSpillService / GlobalSpillScheduler / SpillClient 设计

### 11.1 职责

Spill 存储层为进程级单例 `ProcessSpillService`，承担：

- 拥有一组 `SpillStore`（多目录），统一磁盘 I/O 与介质探测。
- 拥有一个 `GlobalSpillScheduler`（继承 `SpillRequester`），负责异步 spill 编排：
    - 接收 `BlockEvictor::TryScheduleEvict` 推送的 candidate；
    - 控制 worker 数量、active spill attempt 并发；
    - 维护单一全局 `progress_epoch`，给所有 BufferManager 的 Reserve 慢路径等待进展；
    - 跨 BufferManager 公平调度（DRF-lite，按 `SpillClientConfig::fairnessWeight`）。
- 维护进程级 disk quota，超额时 `Write()` 直接抛 `BoltUserError`。
- 对每个 `BufferManager` 颁发一个 `SpillClient`（per-tenant 视图，shared_ptr 持有），同时实现 `SpillRequester` 让 `BlockEvictor` 把 candidate 路由回正确的 fairness slot。

### 11.2 生命周期

```text
1) ProcessSpillService::ConfigureDefault(cfg)   -> 必须调用一次（包含 dirs）
2) ProcessSpillService::Instance()              -> 懒加载；未 Configure 直接抛
3) Instance().CreateClient(SpillClientConfig)   -> BufferManager 启用 spill 时调用
4) ProcessSpillService::ResetForTesting()       -> 仅测试；若仍有活 SpillClient 抛错
```

约束：
- `ConfigureDefault` 只能调用一次（含 `ResetForTesting` 之后再次调用），否则抛 `BoltUserError`。
- 进程内不允许多个 `ProcessSpillService` 实例。
- 析构走显式 `Shutdown`，不依赖 `atexit`，避免静态析构顺序问题。
- 启动时 `CleanupStaleDirsAtStartup` 扫描 `bolt_spill_<pid>_*` 目录，对 `kill(pid, 0)` 已不存在的 pid 做清理。

### 11.3 接口（公共部分）

```cpp
struct ProcessSpillServiceConfig {
    std::vector<SpillDirConfig> dirs;          // 至少一项
    ByteCount processDiskQuotaBytes = 0;       // 0 = 不限
    uint32_t workerThreadCount = 0;            // 0 = 同步路径
    uint32_t maxActiveAttempts = 16;
    MetricsRegistry* metrics = nullptr;
    MediumKind unknownFallbackKind = MediumKind::kHdd;
    bool cleanupOnDestroy = true;
};

struct SpillClientConfig {
    bool enableSpill = false;                  // 必填；false 时 BufferManager 不申请 SpillClient
    std::string tenantId;                      // metric 标签
    ByteCount diskQuotaBytes = 0;              // 0 = 不限
    uint64_t fairnessWeight = 1;
    Priority defaultPriority = Priority::kNormal;
};

class SpillRequester {
public:
    virtual ~SpillRequester() = default;
    virtual EvictResult SubmitSpill(EvictionNode node) = 0;
    virtual bool WaitForProgress(ByteCount bytes_needed,
                                 std::chrono::milliseconds timeout) = 0;
};

class SpillClient : public SpillRequester,
                    public std::enable_shared_from_this<SpillClient> {
public:
    EvictResult SubmitSpill(EvictionNode node) override;   // stamp clientId 后转发
    bool WaitForProgress(ByteCount, std::chrono::milliseconds) override;

    SpillLocation Write(MemoryTag, ConstDataPtr, ByteCount); // 走 ChargeQuota
    void Read(const SpillLocation&, DataPtr, ByteCount);
    void Release(const SpillLocation&) noexcept;

    uint64_t Id() const;
    const SpillClientConfig& Config() const;
    ByteCount UsedDiskBytes() const;
};

class GlobalSpillScheduler : public SpillRequester {
public:
    GlobalSpillScheduler(uint32_t workerThreadCount, MetricsRegistry&);
    void Start();
    void Stop();
    uint64_t RegisterClient(uint64_t weight);
    void UnregisterClient(uint64_t clientId);
    EvictResult SubmitSpill(EvictionNode node) override;
    bool WaitForProgress(ByteCount, std::chrono::milliseconds) override;
};

class ProcessSpillService {
public:
    static ProcessSpillService& Instance();
    static void ConfigureDefault(ProcessSpillServiceConfig);
    static void ResetForTesting();
    std::shared_ptr<SpillClient> CreateClient(SpillClientConfig);
    GlobalSpillScheduler& Scheduler();
    ByteCount UsedDiskBytes() const;
    ByteCount ProcessDiskQuotaBytes() const;
};
```

### 11.4 EvictionNode.clientId 与路由

`EvictionNode` 增加 `uint64_t clientId{0}` 字段。`SpillClient::SubmitSpill` 在 forward 给 `GlobalSpillScheduler` 之前 stamp 自己的 id；scheduler 据此把 candidate 放进对应 client 的 ready 队列，并按 DRF-lite 选择最低 virtualTime 的 client 出队执行。`clientId == 0` 视为非法路由（kFailed）。

### 11.5 Backpressure 语义

`SubmitSpill` 返回：

- `kScheduled`：candidate 已进入 client 的 ready 队列，worker 会异步执行。
- `kBackpressured`：worker 数量为 0 时仅入队，要求调用方走同步 SpillToDisk 或 WaitForProgress 后重试。
- `kSkipped`：candidate stale 或状态不匹配。
- `kFailed`：shutdown / 客户端未注册 / clientId 非法。

约束：
- worker 执行前必须再次 lazy 校验 weak_ptr、eviction_sequence、pin_count、state。
- 后台线程不得持有 `BufferPool` lock、`BlockHandle` lock 或 `SpillStore` lock 等待 I/O。
- DiskQuota 超限直接抛 `BoltUserError`，**不**回退到 compress-only。

### 11.6 progress_epoch

`GlobalSpillScheduler` 维护**单一全局** `progress_epoch`（不是 per-client）。`WaitForProgress` 基于其单调递增：

```text
1. 调用方进入等待前读取 current_epoch。
2. 如果等待期间 epoch 增大，返回 true。
3. 超时且 epoch 未变化，返回 false。
```

以下事件必须推进 epoch 并 `notify_all`：

- spill attempt 完成（成功或失败）。
- active-attempt token 释放。
- `UnregisterClient`（清空 client 的 ready 队列）。
- `Stop()` / shutdown。

---

## 12. TemporaryMemoryManager

`TemporaryMemoryManager` 只提供 advisory budget，不直接分配内存、不持有 buffer、不保证硬 reservation。

```cpp
struct TempMemoryRegisterOptions {
    QueryId query_id;
    OperatorId operator_id;
    std::string operator_name;
    MemoryTag tag;
    ByteCount estimated_remaining_bytes;
    ByteCount minimum_reservation_bytes;
    double spill_penalty;
};

struct ReservationDecision {
    ByteCount reservation;
    bool should_externalize;
    bool should_wait;
};

class TemporaryMemoryState {
public:
    ByteCount Reservation() const;
    ReservationDecision Decision() const;
    void UpdateRemaining(ByteCount);
    void InvalidateForManagerDestruction() noexcept;
    ~TemporaryMemoryState();
};

class TemporaryMemoryManager {
public:
    TemporaryMemoryManager(BufferPool&, MetricsRegistry* metrics = nullptr);
    std::shared_ptr<TemporaryMemoryState> Register(TempMemoryRegisterOptions);
    void InvalidateAllStatesForManagerDestruction() noexcept;
};
```

预算计算输入只能是 `BufferPoolSnapshot`：

```cpp
struct BufferPoolSnapshot {
    ByteCount memory_limit_bytes;
    ByteCount operator_memory_limit_bytes;
    ByteCount pinned_limit_bytes;
    ByteCount used_total_bytes;
    ByteCount used_pinned_bytes;
    ByteCount used_scratch_bytes;
    ByteCount used_emergency_scratch_bytes;
    ByteCount emergency_scratch_bytes;
    ByteCount available_for_operators;
};
```

约束：

- `minimum_reservation_bytes` 是软保障，不是硬承诺。
- 不能满足 minimum 时，返回 `should_externalize=true` 或 `should_wait=true`，不直接 OOM。
- 多 query 并发时先 query 间公平，再 query 内 operator 分配。
- `TemporaryMemoryState` 析构自动 unregister。
- BufferManager 析构时必须 invalidate 外发 state；invalidated 后 `Reservation()` 返回 0，`Decision()` 返回保守 externalize 决策，析构不得访问已销毁的 manager。

---

## 13. BufferManager 对外接口

```cpp
struct BufferManagerConfig {
    ByteCount memory_limit_bytes = 0;
    ByteCount pinned_limit_bytes = 0;
    std::chrono::milliseconds reserve_wait_timeout =
        std::chrono::milliseconds(1000);

    double emergency_scratch_fraction = 0.005;
    ByteCount emergency_scratch_floor = 64ULL << 20;

    // Spill 存储层为进程级单例 ProcessSpillService。每个 BufferManager
    // 通过 spillClient.enableSpill = true 申请一个 SpillClient 视图。
    // 不再有 spill_dir / spill_worker_threads / max_pending_spill_tasks
    // 等字段——这些都迁移到了 ProcessSpillServiceConfig。
    SpillClientConfig spillClient;
    MetricsRegistry* metrics = nullptr;
};

class BufferManager {
public:
    explicit BufferManager(BufferManagerConfig);
    ~BufferManager();

    BufferHandle Allocate(AllocateOptions);

    std::shared_ptr<BlockHandle> AllocatePersistent(
        AllocateOptions,
        std::function<void(DataPtr, ByteCount)> init);

    BufferHandle Pin(const std::shared_ptr<BlockHandle>&);
    BufferHandle TryPin(const std::shared_ptr<BlockHandle>&,
                        std::chrono::milliseconds timeout);

    std::unique_ptr<AccountedMemory> AllocateMemory(MemoryTag, ByteCount bytes);

    BufferPoolReservation ReserveMemory(
        MemoryTag,
        ByteCount bytes,
        ReservationKind kind = ReservationKind::kNormal);

    TemporaryMemoryManager& TempMemory();

    bool TrySetMemoryLimit(ByteCount, SetLimitMode);
    ByteCount GetMemoryUsage() const;
    ByteCount GetMemoryUsage(MemoryTag) const;
};
```

用户入口限制：

- `ReserveMemory` 只接受 `kNormal` 和 `kPinned`。
- `kScratch` 和 `kScratchEmergency` 只能由内部 allocator 使用。
- `AllocateOptions::policy == kRecompute` 时必须提供 `recovery_fn`。
- `kPinnedForever` block 使用 `ReservationKind::kPinned`，不伪造 pin count。

---

## 14. 构造、后置注入与析构

### 14.1 进程级初始化（先于任何 BufferManager）

`ProcessSpillService` 是进程级单例，必须在第一个 `BufferManager` 构造之前由进程主控逻辑显式初始化一次：

```text
1. 进程启动早期：调用 ProcessSpillService::CleanupStaleDirsAtStartup()
   扫描残留 bolt_spill_<pid>_* 目录，通过 kill(pid, 0) 判活后清理死进程残留。
2. 调用 ProcessSpillService::ConfigureDefault(cfg)：
   - cfg.dirs：spill 落盘根目录列表
   - cfg.processDiskQuotaBytes：进程级磁盘配额
   - cfg.workerThreadCount：GlobalSpillScheduler worker 数 (0 = 同步路径)
   - cfg.maxActiveAttempts、cfg.metrics、cfg.unknownFallbackKind、cfg.cleanupOnDestroy
3. 首次访问 ProcessSpillService::Instance() 时懒构造单例，
   构造时启动 GlobalSpillScheduler::Start()。
```

未调用 `ConfigureDefault` 即访问 `Instance()` 必须直接抛错，不允许使用隐式默认配置。
进程内只允许存在一个 `ProcessSpillService` 实例，二次 `ConfigureDefault` 仅在尚未懒构造前生效。

### 14.2 BufferManager 构造顺序

```text
1. 选择 MetricsRegistry；为空则使用 NoOpMetricsRegistry。
2. 计算 memory_limit_bytes：显式配置优先；否则根据 cgroup / rlimit / sysconf 推导。
3. 计算 emergency_scratch_bytes：
   - memory_limit_bytes >= 256 MiB：max(memory_limit_bytes * fraction, floor)。
   - memory_limit_bytes < 256 MiB：emergency_scratch_bytes = 0。
4. 构造 BufferPool。
5. 构造 BufferAllocator。
6. 构造 BlockEvictor(*this)。
7. 若 config.spillClient.enableSpill 为 true：
   - spillClient_ = ProcessSpillService::Instance().CreateClient(config.spillClient)
   - 该调用向 GlobalSpillScheduler 注册一个 per-tenant 客户端视图。
8. 后置注入：
   - evictor_.SetSpillRequester(spillClient_.get())（若 spill 关闭则保持 nullptr）
   - pool.SetEvictor(&evictor_)
9. 构造 TemporaryMemoryManager。
```

`SetEvictor` / `SetSpillRequester` 使用 atomic acquire/release，允许析构期置空。
`GlobalSpillScheduler` 在 `ProcessSpillService` 单例构造时已经 `Start()`，BufferManager 自身不再启动后台线程。

### 14.3 小内存配置 gate

如果 `emergency_scratch_bytes == 0`，则 `kSpillToDisk` 和 `kCompressThenSpill` 在 MVP 中默认不可用，`Allocate` / `AllocatePersistent` 应直接抛 `InvalidArgumentError`，避免用户误以为系统仍能稳定 spill。

### 14.4 BufferManager 析构顺序

```text
1. 设置 shutting_down，禁止新 Allocate / Pin / Register / Reserve / Resize(grow)。
2. 遍历 BlockHandle weak registry，调用 InvalidateForManagerDestruction()。
   - 设置 state=kInvalid。
   - 释放 buffer_、compressed_、spill_loc_ 等资源；spill_loc_ 通过
     SpillClient::Release 归还配额，spill_loc 释放是幂等的。
   - notify_all 唤醒 kLoading/kSpilling waiter。
3. 调用 TemporaryMemoryManager::InvalidateAllStatesForManagerDestruction()。
4. 切断裸指针：pool.SetEvictor(nullptr)，evictor_.SetSpillRequester(nullptr)。
5. spillClient_.reset()：触发 SpillClient 析构，从 GlobalSpillScheduler
   注销该 client，清空该 client 的 ready 队列与残留 spill 文件。
6. 清空 BufferManager 内部 block registry。
7. 销毁 BufferAllocator。
8. 最后销毁 BufferPool。
```

析构期允许的最小操作集只有资源释放，不允许新的增长型 reserve。外部对象不应超过 BufferManager 生命周期；但如果残留，生产实现也必须避免 use-after-free。

### 14.5 进程级关停

进程退出前由主控逻辑显式调用 `ProcessSpillService::Shutdown()`，按顺序：
停接收新 Submit → `GlobalSpillScheduler::Stop()` join worker → 等待所有 SpillClient 已 reset →
清理本进程的 `bolt_spill_<pid>_*` 目录（若 `cleanupOnDestroy=true`）。
不依赖 `atexit`：单例销毁时若仍有 SpillClient 存活属于错误使用，应记录日志并跳过文件清理以避免 UAF。

---

## 15. 锁顺序与并发规则

推荐锁顺序：

```text
TemporaryMemoryManager.lock
  -> BufferPool accounting / queue lock
  -> BlockHandle.lock
  -> SpillStore internal lock
```

硬规则：

- Reserve 快路径尽量使用 atomic 和短锁。
- 不得在持有 `BlockHandle.lock` 时调用 `BufferPool::Reserve`。
- 不得在持有 `BlockHandle.lock` 时执行磁盘 I/O、压缩、recovery。
- 对 victim 加 `BlockHandle.lock` 前必须释放 queue lock。
- `WaitForProgress` 前必须释放 BufferPool 锁。
- `GlobalSpillScheduler` worker 不得持锁等待 I/O。
- `SpillStore::Write` 的 per-dir token 必须 RAII 释放，异常路径也一样。

---

## 16. 错误传播规则

| 场景 | 行为 |
| --- | --- |
| Reserve 超限且无法回收 | 抛 `OutOfMemoryError` |
| pinned_limit_bytes 失败 | 直接抛 `OutOfMemoryError`，不 eviction |
| emergency scratch 不足 | 直接抛 `OutOfMemoryError`，不 eviction |
| Pin `kDiscarded` | 返回 invalid `BufferHandle`，不抛 |
| Pin `kInvalid` | 返回 invalid `BufferHandle` 或 cancellation error，不访问底层资源 |
| SpillStore 短读 / 解压失败 | 抛 `SpillIOError` |
| SpillStore dst 容量不足 | 抛 `InvalidArgumentError` |
| recovery_fn 抛异常 | 状态恢复为 `kEvictedRecomputable`，允许后续重试 |
| async spill transient scratch OOM | 恢复 resident 状态，backoff 后重新入队，不杀 worker |
| scheduler shutdown | `SubmitSpill` 返回 `kFailed` 或 `WaitForProgress` 返回 false |

---

## 17. Metrics

所有组件通过构造函数注入 `MetricsRegistry*`，为空时使用 NoOp。

```cpp
class Counter   { public: virtual void Add(uint64_t = 1) = 0; };
class Gauge     { public: virtual void Set(int64_t) = 0; virtual void Add(int64_t) = 0; };
class Histogram { public: virtual void Observe(double) = 0; };

class MetricsRegistry {
public:
    virtual ~MetricsRegistry() = default;
    virtual Counter& GetCounter(std::string_view name,
                                std::string_view labels) = 0;
    virtual Gauge& GetGauge(std::string_view name,
                            std::string_view labels) = 0;
    virtual Histogram& GetHistogram(std::string_view name,
                                    std::string_view labels) = 0;
};
```

建议指标：

- `buffer_pool.used_bytes{tag,kind}`
- `buffer_pool.reserve_wait_total{outcome}`
- `buffer_pool.eviction_candidate_total{cost,priority,result}`
- `block.pin_wait_total{state,outcome}`
- `block.evict_total{policy,result}`
- `spill.write.bytes_total{medium}`
- `spill.write.errors_total{medium,reason}`
- `spill.write.queue_depth{path,medium}`
- `spill.scheduler.backpressured_total{reason}`
- `spill.scheduler.inflight_bytes`
- `spill.scheduler.progress_epoch`
- `spill.probe.fallback_total{reason}`
- `tmm.active_operators{query}`
- `tmm.advisory_budget_bytes{tag}`

---

## 18. 推荐实现顺序

虽然本文不再按 Agent 拆分，但为了降低集成风险，建议按依赖闭环推进：

1. **基础与测试骨架**：common types、errors、metrics noop/mock、mock quota sink。
2. **BufferPool + Reservation**：完成配额、RAII、snapshot、eviction queue、mock evictor 测试。
3. **BufferAllocator / AccountedMemory**：接入 BufferPool，验证所有 `ReservationKind` 计费。
4. **BlockHandle 基础状态机**：先支持 kLoaded、Pin、Unpin、sealed immutable、kDiscard、kRecompute。
5. **SpillStore MVP**：单目录、普通文件、Write/Read/Release、cleanup、mock scratch。
6. **同步 spill/reload**：打通 BlockHandle 与 SpillStore，先不引入后台线程。
7. **进程级 spill 服务**：`ProcessSpillService` 单例 + `GlobalSpillScheduler` worker pool + `SpillClient` per-tenant 视图，实现 progress_epoch、DRF-lite 公平调度与 backpressure。
8. **压缩与介质感知**：加入 compression、DiskProbe、MediumProfile、per-dir token。
9. **TemporaryMemoryManager**：基于 snapshot 做 advisory budget。
10. **BufferManager 生命周期集成**：构造顺序、后置注入、析构 invalidate、端到端测试。
11. **压力与故障注入**：多线程、OOM、磁盘错误、Stop/析构竞态、4× memory workload。

每一步都应保留可运行系统，不建议长时间维护大量未集成分支。

---

## 19. 必测用例

### 19.1 Reservation / BufferPool

- `kNormal`、`kPinned`、`kScratch`、`kScratchEmergency` 计费正确。
- pinned limit 失败直接 OOM。
- pinned 维度通过但 total 超限时，可 eviction 其他 candidate。
- emergency scratch 不触发 eviction。
- `Resize` 增大失败时原 reservation 不变。
- move assignment 覆盖已有 reservation 时先释放旧 reservation。
- Reserve 慢路径只把 `kFreed` 当作释放内存。
- `kScheduled` / `kBackpressured` 触发等待，不改变 used_total_bytes。
- `SetEvictor(nullptr)` 后 Release 仍可执行，需要 eviction 的 Reserve 直接 OOM。

### 19.2 BlockHandle

- 初始 handle 可写，第一次 unpin 后 sealed。
- sealed 后普通 Pin 只读，`MutableData()` 失败。
- Async spill 期间 Pin 等待。
- `kSpilling` 成功后释放内存并转 `kSpilled`。
- `kSpilling` 失败后恢复 resident 状态并重新入队。
- `kLoading` 失败传播给等待线程，不 thundering herd 重试。
- `kPinnedForever` 计入 used_pinned_bytes，unpin 到 0 不入 eviction queue。
- BufferManager 析构时等待中的 Pin/TryPin 被唤醒并返回 invalid。

### 19.3 SpillStore

- Write/Read/Release 正常闭环。
- double Release 幂等并记录 metric。
- invalid Release 抛错或 debug DCHECK。
- short read、corrupt compressed data 抛 `SpillIOError`。
- chroot / 无 `/sys` 时 DiskProbe 返回 kUnknown 并 fallback。
- forced_kind 覆盖 probe。
- kHdd profile 下 sequential sticky 生效。
- NVMe profile 下 per-dir io_depth 限流生效。
- 多目录混合介质选择符合负载和错误率。
- cleanup 不误删其他实例 session。

### 19.4 ProcessSpillService / GlobalSpillScheduler / SpillClient

- 未调用 `ConfigureDefault` 直接访问 `Instance()` 抛 `BoltUserError`。
- 重复 `ConfigureDefault` 在懒构造前透明覆盖，懒构造之后抛错。
- `ResetForTesting` 在仍有活 SpillClient 时抛错。
- `SubmitSpill` 返回 `kBackpressured` 时 node 已进入对应 client 的 pending/ready 队列。
- Reserve 端不重新 enqueue backpressured node。
- 全局 `progress_epoch` 覆盖成功、失败、token 释放、Stop 等事件。
- active spill attempts 受 emergency scratch 与 `maxActiveAttempts` 共同约束。
- `workerThreadCount=0` 时所有 spill 走同步路径，行为确定。
- DRF-lite 公平：高 weight client 占用更多并发，低 weight client 不被饿死。
- 进程级 disk quota 超限时 `SpillClient::Write` 抛 `BoltUserError`，不回退 compress-only。
- `Shutdown()` 幂等，析构时无后台线程访问已销毁对象；启动时 `CleanupStaleDirsAtStartup` 仅清死进程残留。

### 19.5 TemporaryMemoryManager

- 多 query、多 operator 公平分配。
- sum minimum 大于 available 时返回 externalize/wait，不 OOM。
- unregister 后预算重算。
- BufferManager 析构 invalidate 外发 state，残留 shared_ptr 析构不访问已销毁 manager。

### 19.6 集成与压力

- 4× memory hash/sort workload 无 OOM/abort。
- kHdd-only、NVMe-only、混合介质下 metrics label 正确。
- 多线程 Pin/Unpin/Evict/Spill 无数据竞争。
- 磁盘 ENOSPC、I/O error、scratch OOM 均能恢复或清晰失败。
- BufferManager 析构与并发 Pin/Reserve/Spill 竞态安全。

---

## 20. 接口冻结清单

实现前必须确认以下语义不再随意变更：

1. `ReservationKind` 四类计费语义。
2. `kScratchEmergency` 不触发 eviction，只服务 spill/compress 关键路径。
3. `kSpilling` 表示写出 in-flight，buffer 仍在内存，新 Pin 等待。
4. `kScheduled` / `kBackpressured` 不等价于 freed。
5. `kBackpressured` 必须表示 scheduler 已接收 candidate。
6. `eviction_sequence` 用于 stale candidate 判断。
7. `spill_sequence` 仅用于 BlockHandle 内部防止异步完成乱序，不进入 `EvictionNode`。
8. block 初始写窗口结束后 sealed immutable。
9. `kPinnedForever` 使用 hard pinned charge，不伪造 pin_count。
10. `BlockBuffer::memory_` 是 kLoaded 主体内存唯一 owner。
11. `kCompressed` 状态由 `compressed_` 持有唯一 reservation。
12. `BufferPoolSnapshot` 是 TMM 的唯一预算输入。
13. `WaitForProgress` 必须有 timeout，基于 `progress_epoch` 防 lost wakeup。
14. `BufferPool` 等待 progress 前必须释放内部锁。
15. 介质探测只在 SpillStore 构造期执行。
16. M3/M5/M6 不直接做 HDD/NVMe 策略分支。
17. BufferManager 析构必须 invalidate 外发 BlockHandle / TemporaryMemoryState。
18. `MetricsRegistry* == nullptr` 时使用 NoOp。
19. 用户入口不得申请 `kScratch` / `kScratchEmergency`。
20. 外部 handle/state/accounted memory 不支持超过 BufferManager 生命周期；生产实现仍要防 UAF。

---

## 21. 一句话总结

这套设计的核心是：用 `BufferPool` 统一配额和 eviction candidate，用 `AccountedMemory` 保证 reservation RAII，用 `BlockHandle` 承担严格状态机和 sealed immutable 语义，用 `SpillStore` 封装全部磁盘与介质差异，用进程级单例 `ProcessSpillService` 和 `GlobalSpillScheduler` 处理跨 BufferManager 的异步 spill / backpressure / progress / DRF-lite 公平调度（每个 BufferManager 持 `SpillClient` 视图），用 `TemporaryMemoryManager` 提供 advisory operator budget，最后由 `BufferManager` 串起构造、后置注入和析构安全线。只要上述状态机、计费和生命周期约束被严格实现，即使不按多 Agent 明确拆分，也可以完成一套可靠的新 OLAP BufferManager。
