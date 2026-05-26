# DiskIoScheduler Optimization Plan

本文档细化 `DiskIoScheduler::SubmitAndWait()` 持锁执行 `engine_->Execute(req)` 的优化方案。目标是先把设计边界、阶段拆分、风险和验证方式说清楚，后续再按阶段改代码。

## 背景

当前 BufferManager spill write 的核心路径是：

```text
BufferManager::Reclaim()
    |
    v
SpillCoordinator::SubmitSpill()
    |
    v
DiskIoTaskExecutor::SubmitTask()
    |
    v
SpillFileStore::WriteToLocation()
    |
    v
DiskIoScheduler::SubmitAndWait()
    |
    v
UringDiskIoEngine::Execute()
```

`DiskIoScheduler::SubmitAndWait()` 当前实现会在持有 `DiskIoScheduler::mutex_` 的情况下执行所有 request：

```text
lock scheduler mutex
    pick next request
    engine_->Execute(req)
    queueDepth_.Observe(completion)
    ChargePriorityLocked(...)
unlock scheduler mutex
```

而 `UringDiskIoEngine::Execute()` 当前对每个 request 的语义是：

```text
get one SQE
io_uring_submit()
io_uring_wait_cqe()
cqe_seen()
return completion
```

这导致两个问题叠加：

```text
1. scheduler mutex 把多个 DiskIoTaskExecutor workers 串行化。
2. io_uring 层每次 submit 一个 request 后立即 wait，一个 ring 上没有真实 in-flight window。
```

因此，即使 `disk_io_workers=4`，当前实现也更接近：

```text
many workers
    -> one scheduler mutex
        -> one request submitted
        -> wait one completion
```

而不是：

```text
many workers
    -> central admission control
        -> multiple in-flight disk requests
        -> batched completions
```

## 设计目标

优化目标：

```text
1. 保留中心化 disk 控制点，避免每个 worker 自己无限制打 disk。
2. scheduler mutex 只保护 pending / in-flight / priority / metrics 状态，不包住真实 I/O 等待。
3. `initialQueueDepth` / `maxQueueDepth` 变成真实 in-flight 上限，而不是只被 Observe 使用。
4. io_uring 使用批量 submit / 批量 reap，尤其改善 4 KiB 小 I/O。
5. BufferManager 对外语义不变：Reclaim() 只有在 spill 完成并 commit 后才返回 reclaimed bytes。
```

非目标：

```text
1. 不在第一阶段改变 SpillFileStore 的文件布局。
2. 不在第一阶段引入 direct I/O 作为默认 runtime 语义。
3. 不把 disk 控制分散到每个 owner thread 或每个 spill worker。
4. 不用隐藏同步 fallback 掩盖异步路径错误。
```

## 关键判断

### 不能只把 `engine_->Execute()` 移到锁外

最小改法看起来是：

```text
lock scheduler
    pick request
unlock scheduler

engine_->Execute(req)

lock scheduler
    observe completion
unlock scheduler
```

但这不是完整方案，原因是 `UringDiskIoEngine` 当前只有一个 ring。多个 worker 如果同时调用同一个 engine 的 `Execute()`，会并发访问同一个 `io_uring` SQ/CQ 状态。除非给 engine 再加一把锁，否则会有 ring 并发安全风险；如果给 engine 加锁，又会把串行点从 scheduler mutex 挪到 engine mutex。

所以“把 Execute 移出 scheduler 锁”最多适合作为 profiling 对照，不应作为最终并发方案。

### 推荐用单 ring owner 线程

更稳妥的方向是让一个 disk I/O owner 线程独占 `UringDiskIoEngine`，其他 worker 只提交 request 到 scheduler 队列：

```text
DiskIoTaskExecutor workers
    |
    | SubmitAndWait / SubmitAsync
    v
DiskIoScheduler pending queues
    |
    | dispatch up to queue depth
    v
DiskIoService thread owns io_uring ring
    |
    | batch submit / batch reap
    v
completion records
    |
    v
waiter wakeup / SpillCoordinator completion
```

这样可以同时满足：

```text
1. ring 只有一个 owner，不需要多线程直接碰 SQ/CQ。
2. scheduler 仍然是唯一 admission-control 点。
3. queue depth 可以直接对应 ring in-flight 数量。
4. 后续可以自然扩展成每块盘一个 ring owner。
```

## 分阶段方案

### Stage 0: 加观测，不改变行为

先补指标，确认瓶颈占比，避免误判。

建议指标：

```text
bm_disk_scheduler_submit_total
bm_disk_scheduler_request_bytes
bm_disk_scheduler_lock_wait_us
bm_disk_scheduler_locked_us
bm_disk_scheduler_engine_execute_us
bm_disk_scheduler_queue_depth_limit

bm_uring_submit_count
bm_uring_wait_count
bm_uring_inflight_max
bm_uring_request_bytes
bm_uring_execute_us

bm_disk_task_queue_wait_us
bm_disk_task_run_us
bm_disk_task_payload_bytes
```

预期如果当前判断正确，4 KiB write 会看到：

```text
uring_submit_count ~= block_count
uring_wait_count ~= block_count
uring_inflight_max ~= 1
scheduler_engine_execute_us 占 scheduler locked_us 的大头
```

### Stage 1: 拆开 scheduler 状态锁和 I/O 执行模型

这一阶段不追求最终性能，只把职责边界改清楚。

接口草案：

```cpp
struct DiskIoHandle {
  uint64_t id;
  DiskIoCompletion Wait();
};

class DiskIoScheduler {
 public:
  DiskIoCompletion SubmitAndWait(const DiskIoRequest& request);
  std::vector<DiskIoCompletion> SubmitAndWait(
      const std::vector<DiskIoRequest>& requests);

 private:
  DiskIoHandle SubmitAsync(DiskIoRequest request);
  void EnqueueLocked(DiskIoRequest request, uint64_t id);
  void DispatchReadyLocked();
};
```

`SubmitAndWait()` 可以先兼容旧调用方：

```text
SubmitAndWait(req)
    handle = SubmitAsync(req)
    return handle.Wait()
```

这样上层 API 暂时不变，后续 `SpillCoordinator` 再逐步改成 batch 提交。

这一阶段的数据结构：

```text
pending_by_priority[low/medium/high]
in_flight map request_id -> completion_state
completed state per request_id
queue_depth_limit
next_request_id
```

关键点：

```text
1. scheduler mutex 只保护 pending / in_flight / completed / metrics。
2. waiter 等 condition_variable 时必须释放 scheduler mutex。
3. completion 写入时只短时间持锁，然后 notify。
4. queueDepth_.Observe() 在 completion 到达后执行。
```

### Stage 2: 引入 ring owner 线程和真实 in-flight window

这一阶段是核心优化。

线程模型：

```text
DiskIoScheduler
    owns pending queues
    owns completion states
    owns queue-depth policy

DiskIoService thread
    owns UringDiskIoEngine
    drains pending queues
    submits SQEs up to QueueDepthLimit()
    waits/reaps CQEs
    writes completions back to scheduler
```

ring owner 主循环：

```text
while running:
    lock scheduler
        pick requests while in_flight < queue_depth_limit
        move picked requests to local dispatch batch
        mark them in_flight
    unlock scheduler

    submit batch to io_uring

    reap available completions

    lock scheduler
        complete request states
        queueDepth_.Observe(each completion)
        notify waiters
    unlock scheduler
```

`UringDiskIoEngine` 接口从同步 `Execute()` 拆成：

```cpp
class DiskIoEngine {
 public:
  virtual size_t SubmitBatch(
      std::span<const DiskIoRequest> requests,
      std::span<const uint64_t> requestIds) = 0;

  virtual std::vector<DiskIoCompletion> ReapCompletions(
      size_t maxCompletions,
      std::chrono::milliseconds timeout) = 0;
};
```

`io_uring` 的 `user_data` 应该放 `request_id`：

```text
SQE user_data = request_id
CQE user_data -> scheduler completion state
```

### Stage 3: SpillCoordinator 批量提交小块 spill

Stage 2 解决了 disk scheduler 串行化，但 4 KiB case 仍然有每 block 一个 task 的固定成本。因此第三阶段再优化 spill 上层粒度。

当前：

```text
one 4 KiB block
    -> one DiskIoTask
    -> one SpillFileStore::Write()
    -> one DiskIoScheduler request
    -> one completion
```

优化方向：

```text
BufferManager::Reclaim()
    collects N spill candidates
    submits batch to SpillCoordinator

SpillCoordinator worker
    prepares N write requests
    submits N DiskIoRequests through SubmitBatch
    waits for N completions
    emits N block-level completions
```

对 BufferManager 的语义保持不变：

```text
每个 block 仍然单独 CommitAsyncSpillSuccess / Failure
Reclaim() 仍然只返回真实完成并释放的 resident bytes
```

## future / promise 的位置

可以使用 future/promise，但不建议作为底层核心模型。

推荐边界：

```text
DiskIoHandle:
    lightweight request handle
    provides Wait()

optional adapter:
    DiskIoHandle -> std::future<DiskIoCompletion>
```

原因：

```text
1. 4 KiB case 可能产生几十万个 request，std::promise/std::future 的分配和同步成本偏高。
2. std::future 不擅长 wait-many，不适合 batch completion。
3. io_uring 天然是 user_data -> CQE completion，轻量 request_id 更贴近底层。
```

所以 future/promise 可以用于 API 适配，不应该决定 scheduler 内部结构。

## Backpressure 语义

Backpressure 不是取消 I/O，而是限制进入 pending / in-flight 的 request 数。

建议分两层：

```text
in_flight_limit:
    来自 AdaptiveQueueDepth::Limit()
    控制已经 submit 到 io_uring 但未完成的数量

pending_limit:
    防止上层无限提交导致内存和 completion state 暴涨
```

当 pending 满时：

```text
SubmitAsync high priority:
    可以等待空间或返回 backpressured

SubmitAsync low priority prefetch:
    优先返回 backpressured

spill write:
    不做同步 fallback
    保持 failure/backpressure 可见
```

## 优先级调度

现有 `PickNextLocked()` 只在一次传入的 vector 中做 priority/deficit 选择。优化后应改成全局 pending queues：

```text
pending_high
pending_medium
pending_low
deficit[priority]
priorityWeights[priority]
```

每次 dispatch batch 时：

```text
while dispatch_batch not full and in_flight < limit:
    add weight to each priority deficit
    pick queue with best positive deficit and non-empty pending
    pop one request
    charge by max(1, request.size)
```

这样 prefetch 这类 low priority request 不会饿死，但 spill write 可以优先。

## 错误处理

必须保留清晰错误传播：

```text
submit SQE failed:
    complete request with negative errno
    notify waiter

CQE res < 0:
    complete request with res

short write / short read:
    初期返回 short completion，由 SpillFileStore 保持现有 CHECK
    后续可以在 engine 层支持 requeue remainder

ring owner thread fatal error:
    fail all pending and in-flight requests
    wake all waiters
    不 fallback 到同步 spill
```

短读短写重试可以作为后续增强，避免第一阶段扩大变更面。

## 文件和模块影响

预计涉及：

```text
bolt/common/memory/bm/DiskIo.h
    DiskIoHandle / completion state / async scheduler API
    DiskIoEngine batch submit/reap API

bolt/common/memory/bm/DiskIo.cpp
    scheduler pending queues
    in-flight accounting
    ring owner loop
    UringDiskIoEngine batch implementation

bolt/common/memory/bm/SpillFileStore.cpp
    初期继续 SubmitAndWait
    后续接 SubmitBatch

bolt/common/memory/bm/SpillCoordinator.cpp
    Stage 3 批量 spill submit

bolt/common/memory/bm/tests/DiskIoTest.cpp
    scheduler concurrency/backpressure/priority/completion tests

bolt/common/memory/bm/benchmarks/BufferManagerSpillIoBenchmark.cpp
    输出 scheduler / uring 观测字段
```

## 测试计划

### Unit tests

建议先用 fake engine 测 scheduler，不依赖真实 io_uring：

```text
1. SubmitAndWait returns completion and releases waiter.
2. Multiple requests can be in-flight up to queueDepth limit.
3. Queue depth limit prevents over-dispatch.
4. High priority request is picked before low priority when both pending.
5. Failed completion wakes waiter and propagates error.
6. Stop fails pending and in-flight requests without hanging.
```

io_uring 相关测试保持 skip-friendly：

```text
if io_uring_queue_init returns EPERM:
    skip uring integration test
```

### Benchmark matrix

最小验证矩阵：

```text
threads=4,8
disk_io_workers=1,2,4,8
block_size=4KiB
total_mb_per_thread=small enough for iteration, then full size
baseline=buffered default
```

关注指标：

```text
bm_write_iops
write_ratio
scheduler_lock_wait_us
scheduler_inflight_max
uring_inflight_max
uring_submit_batch_size_avg
uring_reap_batch_size_avg
disk_task_queue_wait_us
```

预期：

```text
Stage 0:
    uring_inflight_max ~= 1

Stage 2:
    uring_inflight_max approaches queue depth under 4 KiB pressure
    bm_write_iops improves with disk_io_workers until disk or scheduler saturates

Stage 3:
    task count per GiB drops
    4 KiB write IOPS improves further
```

## 推荐落地顺序

```text
1. 先做 Stage 0 指标，确认当前瓶颈占比。
2. 做 Stage 1 API 兼容重构，但不要声称性能优化完成。
3. 做 Stage 2 ring owner + real in-flight window，这是核心优化。
4. 跑 4 KiB write 矩阵，确认 queue depth 生效。
5. 再做 Stage 3 SpillCoordinator batch，降低小块 task 成本。
```

如果只选一个最值得做的优化，应优先做 Stage 2。它解决的是当前 `DiskIoScheduler` 串行化和 `UringDiskIoEngine` 单 in-flight 的核心结构问题。
