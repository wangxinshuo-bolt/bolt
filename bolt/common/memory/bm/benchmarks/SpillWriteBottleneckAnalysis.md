# BufferManager Spill I/O Bottleneck Analysis

本文档基于 `bolt_buffer_manager_spill_io_benchmark` 的完整运行日志，分析 BufferManager spill write / read / prefetch 路径暴露出来的问题。

日志来源：

```text
/data00/home/wangxinshuo.db/bolt/log.txt
```

本文重点服务后续优化设计。它不直接给出最终重构方案，而是把现象、口径差异、可能瓶颈、证据强弱和建议观测点拆开。

更新说明：

```text
2026-05-26:
    benchmark baseline 默认改为 buffered I/O，不再默认使用 O_DIRECT，
    也不再默认在 write 后 fdatasync。历史日志仍来自旧 direct baseline 口径，
    因此本文中基于 /data00/home/wangxinshuo.db/bolt/log.txt 的数值解释保留为历史分析。
```

## 结论摘要

完整 benchmark 给出几个明确结论：

```text
1. 4 KiB spill write 很慢，而且线程数从 4 增到 8 后更慢。
2. 4 KiB prefetch read 也很慢，和 write 一样暴露出小块异步任务路径的固定成本。
3. 在历史日志中，8 MiB BM write 明显快于 direct disk baseline，但这个结果不能解释成 BM 磁盘写更快，因为当时 BM 是 buffered write，baseline 是 O_DIRECT + fdatasync。
4. 在历史日志中，BM Pin() read 明显快于 direct disk baseline，主要说明它读到了 page cache，不能拿来衡量真实磁盘 read 能力。
5. 8 MiB prefetch read 在 4 线程和 8 线程之间差异很大，说明 prefetch 路径除了磁盘 I/O，还可能受内存分配、page cache、completion drain 或中心队列调度影响。
```

最需要优先确认的三个问题：

```text
1. DiskIoScheduler 是否把多个 disk_io_workers 串行化成了单路 I/O？
2. UringDiskIoEngine 是否始终只有 1 个 in-flight request？
3. 每 4 KiB block 一个 DiskIoTask 的调度成本到底占多少？
```

## 完整 Benchmark 结果

### Summary 指标

```text
+------------+---------+------------+----------------+----------------+-------------+-------------+-------------+
| case       | threads | total      | BM write       | disk write     | write ratio | BM prefetch | prefetch rt |
+------------+---------+------------+----------------+----------------+-------------+-------------+-------------+
| 4 KiB      | 4       | 1 GiB      | 830 IOPS       | 4933 IOPS      | 0.168       | 2356 IOPS   | 0.228       |
| 4 KiB      | 8       | 2 GiB      | 409 IOPS       | 4994 IOPS      | 0.082       | 1385 IOPS   | 0.136       |
| 8 MiB      | 4       | 4 GiB      | 1775 MiB/s     | 183 MiB/s      | 9.718       | 25574 MiB/s | 142.259     |
| 8 MiB      | 8       | 8 GiB      | 2015 MiB/s     | 181 MiB/s      | 11.158      | 136 MiB/s   | 0.754       |
+------------+---------+------------+----------------+----------------+-------------+-------------+-------------+
```

普通 `Pin()` read 指标：

```text
+------------+---------+-------------+-------------+----------+
| case       | threads | BM pin read | disk read   | ratio    |
+------------+---------+-------------+-------------+----------+
| 4 KiB      | 4       | 147856 IOPS | 10310 IOPS  | 14.341   |
| 4 KiB      | 8       | 139612 IOPS | 10174 IOPS  | 13.722   |
| 8 MiB      | 4       | 3281 MiB/s  | 180 MiB/s   | 18.251   |
| 8 MiB      | 8       | 2748 MiB/s  | 180 MiB/s   | 15.232   |
+------------+---------+-------------+-------------+----------+
```

### Progress 耗时和测量窗口

progress 的阶段耗时包含准备工作；summary 中的 `bm_*_elapsed_ms` 只统计真正测量窗口。因此 progress 耗时和 summary 耗时不是一个口径。

```text
+------------+---------+-------------------+--------------------+-------------------+
| case       | threads | phase             | progress elapsed   | measured elapsed  |
+------------+---------+-------------------+--------------------+-------------------+
| 4 KiB      | 4       | bm_write          | 316.583 s          | 315.820 s         |
| 4 KiB      | 4       | bm_pin_read       | 348.731 s          | 1.773 s           |
| 4 KiB      | 4       | bm_prefetch_read  | 465.026 s          | 111.280 s         |
| 4 KiB      | 8       | bm_write          | 1282.870 s         | 1282.111 s        |
| 4 KiB      | 8       | bm_pin_read       | 1284.542 s         | 3.755 s           |
| 4 KiB      | 8       | bm_prefetch_read  | 1685.383 s         | 378.557 s         |
| 8 MiB      | 4       | bm_write          | 4.860 s            | 2.308 s           |
| 8 MiB      | 4       | bm_pin_read       | 5.812 s            | 1.248 s           |
| 8 MiB      | 4       | bm_prefetch_read  | 4.315 s            | 0.160 s           |
| 8 MiB      | 8       | bm_write          | 6.606 s            | 4.065 s           |
| 8 MiB      | 8       | bm_pin_read       | 9.094 s            | 2.982 s           |
| 8 MiB      | 8       | bm_prefetch_read  | 66.361 s           | 60.202 s          |
+------------+---------+-------------------+--------------------+-------------------+
```

对 write case，progress 和 measured 很接近，说明 write 阶段的准备工作很少，summary 中的 `bm_write_elapsed_ms` 可以代表端到端 `Reclaim()` spill write 耗时。

对 read / prefetch case，progress 还包含重新构造 blocks 并 spill 的准备阶段。比如 4 KiB 8 线程 `bm_pin_read` progress 是 `1284.542 s`，但 measured read 只有 `3.755 s`。这不是矛盾，而是说明该阶段大部分时间花在“准备一批已 spilled blocks”。

## Benchmark 口径差异

分析历史结果前必须先明确：旧日志里 BM 路径和 disk baseline 的 I/O 语义不一样。

### 历史日志中的 Disk baseline 是 direct I/O

历史日志对应的 benchmark baseline 使用：

```text
open(..., O_DIRECT)
pwrite / pread
fdatasync after write
```

相关代码在 `BufferManagerSpillIoBenchmark.cpp`：

```text
direct write:
    open(path, O_RDWR | O_DIRECT)
    pwrite(...)
    fdatasync(...)

direct read:
    open(path, O_RDONLY | O_DIRECT)
    pread(...)
```

所以旧 baseline 尽量绕过 page cache，写入后还会做 `fdatasync()`。

当前 benchmark 默认 baseline 已改成：

```text
open(..., buffered I/O)
pwrite / pread
no fdatasync after write
no posix_fadvise(DONTNEED) before read
```

如果后续需要设备级或冷读对比，可以显式打开：

```text
--bm_spill_io_benchmark_disk_baseline_direct_io=true
--bm_spill_io_benchmark_disk_baseline_fsync_after_write=true
--bm_spill_io_benchmark_disk_baseline_drop_cache_before_read=true
```

### BufferManager spill 当前是 buffered I/O

`SpillFileStore::WriteToLocation()` 当前使用：

```text
open(location.path, O_CREAT | O_TRUNC | O_WRONLY)
DiskIoScheduler::SubmitAndWait(write)
```

它没有 `O_DIRECT`，也没有在每次 spill write 后 `fdatasync()`。

这意味着：

```text
BM write 完成
    = 数据已写入 kernel page cache 或被内核接受
    != 数据已按 O_DIRECT 语义落到设备
```

对历史日志而言：

- 8 MiB BM write 比 direct baseline 快 10 倍，不代表 BM 磁盘写入能力强于磁盘。
- BM Pin read 比 direct baseline 快 14 到 18 倍，主要说明刚 spill 的数据仍然在 page cache。
- 4 KiB BM write 在 buffered I/O 口径下仍然只有 direct baseline 的 8% 到 17%，这反而强化了“小块框架固定成本很重”的判断。

### 当前 benchmark 更适合回答的问题

当前默认 benchmark 更适合回答：

```text
BufferManager 端到端 spill/reload 语义在不同 block size 下的框架吞吐是多少？
4 KiB 小块路径会不会被 task / scheduler / metadata 固定成本压垮？
owner threads 增加后，BM 框架是否能扩展？
```

当前 benchmark 不适合直接回答：

```text
BM write 是否真正达到了设备写带宽？
BM read 是否真正达到了设备读带宽？
BM 和 O_DIRECT baseline 的 read bandwidth 是否公平可比？
```

如果后续要做设备级对比，需要显式打开 direct / fsync / cache-drop baseline 选项，或者让 BM spill file 也支持对应 I/O 语义。

## 现象 1: 4 KiB write 随线程增加明显恶化

4 KiB write 结果：

```text
+---------+-----------+------------+-------------+
| threads | BM IOPS   | disk IOPS  | write ratio |
+---------+-----------+------------+-------------+
| 4       | 830.043   | 4933.007   | 0.168       |
| 8       | 408.926   | 4993.911   | 0.082       |
+---------+-----------+------------+-------------+
```

disk baseline 从 4 线程到 8 线程基本稳定：

```text
4933 IOPS -> 4994 IOPS
```

BM write 从 4 线程到 8 线程明显下降：

```text
830 IOPS -> 409 IOPS
```

这说明问题不是磁盘在 8 线程下更慢。更可能是 BM 框架里的共享资源竞争变重了，例如：

- `DiskIoTaskExecutor` task queue。
- `DiskIoScheduler::mutex_`。
- `SpillFileStore::mutex_`。
- owner completion list / active owner tracking。
- small spill allocator 的内部状态。

4 KiB 8 线程 case 有 `524288` 个 block。如果每个 block 都对应一个 task、一个 store write、一个 scheduler submit、一个 completion，那么共享锁和调度开销会比 4 线程 case 翻倍以上。

## 现象 2: 8 MiB write 很快，但不能证明 I/O path 没问题

8 MiB write 结果：

```text
+---------+--------------+----------------+-------------+
| threads | BM bandwidth | disk bandwidth | write ratio |
+---------+--------------+----------------+-------------+
| 4       | 1774 MiB/s   | 183 MiB/s      | 9.718       |
| 8       | 2015 MiB/s   | 181 MiB/s      | 11.158      |
+---------+--------------+----------------+-------------+
```

这个结果有两层含义。

第一，8 MiB case 的 block 数很少：

```text
4 threads: 512 blocks
8 threads: 1024 blocks
```

相比 4 KiB case 的 `262144 / 524288` 个 block，per-block task、metadata、completion 固定成本被大幅摊薄。

第二，BM 使用 buffered write，而 baseline 使用 `O_DIRECT + fdatasync`。8 MiB BM write 的 1.7 到 2.0 GiB/s 更像是 page cache / memory copy / kernel buffered write 接收速度，不是设备真实写带宽。

所以 8 MiB write 的正确解读是：

```text
大块下，BM per-block 框架成本不明显；
但当前 benchmark 不能用 8 MiB write ratio 证明 BM 磁盘路径优于 baseline。
```

## 现象 3: Pin read 主要测到了 page cache

`Pin()` read 全部显著快于 direct read baseline：

```text
4 KiB 4 threads: 147856 IOPS vs 10310 IOPS
4 KiB 8 threads: 139612 IOPS vs 10174 IOPS
8 MiB 4 threads: 3281 MiB/s vs 180 MiB/s
8 MiB 8 threads: 2748 MiB/s vs 180 MiB/s
```

这些 ratio 都大于 13。这个数量级不符合真实设备 read 对比，更像是：

```text
BM spill write
    -> data remains in page cache
Pin read
    -> buffered read hits page cache
```

因此，`bm_pin_read` 当前只能说明：

```text
BufferManager Pin reload 状态机 + page-cache read 很快。
```

它不能说明：

```text
BufferManager 真正 disk read path 比 O_DIRECT disk baseline 快。
```

后续如果要优化真实 read path，需要单独设计冷读 benchmark 或 direct I/O BM read。

## 现象 4: Prefetch read 对小块仍然慢

4 KiB prefetch read：

```text
+---------+--------------+-----------+---------------+
| threads | BM prefetch  | disk read | prefetch ratio|
+---------+--------------+-----------+---------------+
| 4       | 2356 IOPS    | 10310 IOPS| 0.228         |
| 8       | 1385 IOPS    | 10174 IOPS| 0.136         |
+---------+--------------+-----------+---------------+
```

`Prefetch()` 读的是 buffered file，按理说也可能受 page cache 帮助。但 4 KiB prefetch 仍然只有 direct disk read 的 13% 到 23%。这说明小块 prefetch 的主要瓶颈很可能不是设备 read，而是：

- 每 block 一个 prefetch task。
- 低优先级 task 进中心队列。
- worker 执行 `SpillFileStore::Read()`。
- `DiskIoScheduler::SubmitAndWait()` 串行化。
- owner thread 需要反复 `Prefetch({})` drain completion。
- 每个 block 都要重新 commit resident memory 状态。

这个现象和 4 KiB write 互相支持：小块异步 I/O 框架路径有明显固定成本。

## 现象 5: 8 MiB prefetch 在 4 线程和 8 线程之间差异异常大

8 MiB prefetch read：

```text
+---------+--------------+-----------+---------------+
| threads | BM prefetch  | disk read | prefetch ratio|
+---------+--------------+-----------+---------------+
| 4       | 25574 MiB/s  | 180 MiB/s | 142.259       |
| 8       | 136 MiB/s    | 180 MiB/s | 0.754         |
+---------+--------------+-----------+---------------+
```

4 线程 case 的 `25574 MiB/s` 基本可以判断是 page cache / memory copy 效果，不是设备 read。

8 线程 case 下降到 `136 MiB/s`，反而低于 direct read baseline。这一项需要单独调查，可能原因包括：

- 8 GiB prefetch 触发内存压力或 page cache eviction。
- 8 个 owner threads 同时 drain completion，中心 completion list 或 owner commit 有竞争。
- `DiskIoTaskExecutor` 只有 4 个 workers，8 个 owner 提交的大块 prefetch 在队列里等待。
- 8 MiB request 在 `UringDiskIoEngine::Execute()` 中仍然是 submit/wait 模型，大块下每个 request 占用 scheduler 锁时间更长。
- benchmark 的 prefetch drain loop 用 `Snapshot().usedLoadedBytes >= expected` 判断完成，如果 owner commit 或 memory allocation 变慢，会被计入 prefetch elapsed。

这个现象说明：不能只优化 4 KiB task 粒度。大块 prefetch 在高并发下也需要看中心队列、scheduler 锁和 owner completion commit。

## 当前写路径

4 KiB write case 的 BufferManager 测量路径：

```text
worker thread
    |
    | Reclaim(total_thread_bytes)
    v
BufferManager
    |
    | TryScheduleEvict(one block)
    v
SpillCoordinator
    |
    | SubmitTask(one block)
    v
DiskIoTaskExecutor
    |
    | worker executes one spill task
    v
SpillFileStore
    |
    | Write(one block)
    v
DiskIoScheduler
    |
    | SubmitAndWait(one request)
    v
UringDiskIoEngine
    |
    | submit one SQE and wait one CQE
    v
kernel / filesystem / page cache / disk
```

计时从各线程进入 `Reclaim()` 后开始，到所有线程完成 spill write、owner thread drain completion 并 commit 后结束。因此 BM write 结果包含：

- block eviction candidate 扫描和调度。
- spill task 提交和 worker 调度。
- `SpillFileStore` location 分配、元数据更新、写入。
- `DiskIoScheduler` 调度和 `UringDiskIoEngine` 执行。
- completion 回传、owner thread drain、block 状态提交和 resident memory 释放。

direct disk baseline 的测量路径：

```text
worker thread
    |
    | O_DIRECT pwrite(fd, aligned_buffer, block_bytes, offset)
    v
shared baseline file
    |
    | fdatasync(fd)
    v
disk
```

baseline 多线程共享同一个文件，每个线程写不同 offset range。baseline 不包含 BufferManager 的 block 状态机、spill metadata、task 队列和 completion commit 成本。

## 问题 1: DiskIoScheduler 串行化了 I/O 执行

现状：

```text
DiskIoTaskExecutor worker 0 --+
DiskIoTaskExecutor worker 1 --+--> DiskIoScheduler::mutex_ --> engine_->Execute()
DiskIoTaskExecutor worker 2 --+
DiskIoTaskExecutor worker 3 --+
```

`DiskIoScheduler::SubmitAndWait()` 在持有 `mutex_` 的情况下调用 `engine_->Execute(req)`。这意味着即使 `disk_io_workers=4`，最终进入 `UringDiskIoEngine` 的请求仍然被 `DiskIoScheduler` 单锁串行化。

为什么完整日志支持这个怀疑：

- 4 KiB disk baseline 从 4 线程到 8 线程保持在约 5K IOPS。
- 4 KiB BM write 从 830 IOPS 降到 409 IOPS。
- 4 KiB BM prefetch 从 2356 IOPS 降到 1385 IOPS。
- 如果下游 I/O 能随 owner 并发扩展，线程数增加不应该导致 BM 小块吞吐直接腰斩。

影响：

- benchmark baseline 是多线程并发 `pwrite()`。
- BM write 近似变成多个 worker 竞争一个中心锁，然后单路执行 I/O。
- 对 4 KiB IOPS，锁等待和单路执行会直接压低吞吐。

验证建议：

```text
metric: scheduler_submit_count
metric: scheduler_lock_wait_us
metric: scheduler_execute_under_lock_us
metric: scheduler_request_bytes
metric: scheduler_requests_while_locked
```

如果 `scheduler_lock_wait_us` 很高，说明 worker 被中心锁阻塞。如果 `scheduler_execute_under_lock_us` 接近整个 BM write 耗时，说明 I/O 层基本单路执行。

建议单独跑：

```text
disk_io_workers=1,2,4,8
threads=4,8
case=4k
```

如果 worker 增加后 IOPS 不升反降，`DiskIoScheduler::mutex_` 就是第一优先级瓶颈。

## 问题 2: UringDiskIoEngine 一次只 submit 一个请求并等待一个 completion

现状：

```text
for each DiskIoRequest:
    io_uring_get_sqe()
    io_uring_prep_write()
    io_uring_submit()
    io_uring_wait_cqe()
    io_uring_cqe_seen()
```

`UringDiskIoEngine::Execute()` 对单个 request 采用同步等待模型。它没有保持多个 in-flight SQE，也没有批量 submit 多个 4 KiB write。

影响：

- io_uring 的核心收益来自批量提交和异步 completion。
- 当前用法更像“用 io_uring 包了一层同步 write/read”。
- 对 4 KiB 小 I/O，submit/wait syscall 成本和 CQE 处理成本会被放大。
- `DiskIoConfig::initialQueueDepth/maxQueueDepth` 当前没有真正转化为 in-flight I/O 数量。

完整日志里的间接证据：

- 4 KiB write 的 block 数是 `262144 / 524288`，BM 每个 block 都走一次 request。
- 如果每个 request 都 submit/wait，4 KiB case 会产生几十万次 submit/wait。
- 8 MiB write 的 block 数只有 `512 / 1024`，固定 submit/wait 成本被摊薄，因此没有表现为低吞吐。

验证建议：

```text
metric: uring_submit_count
metric: uring_wait_count
metric: uring_request_bytes
metric: uring_inflight_max
metric: uring_execute_us
```

预期现象：

```text
4 KiB case:
    uring_submit_count ~= block_count
    uring_wait_count ~= block_count
    uring_inflight_max ~= 1
```

如果验证成立，说明当前 queue depth 配置对 4 KiB write 没有发挥作用。

## 问题 3: 每 4 KiB block 一个 DiskIoTask，任务粒度过细

4 KiB case 中：

```text
4 threads: 1 GiB / 4 KiB = 262144 blocks
8 threads: 2 GiB / 4 KiB = 524288 blocks
```

当前每个 block 都会提交一个 spill task：

```text
one 4 KiB block
    -> one SpillCoordinator::SubmitSpill()
    -> one DiskIoTaskExecutor::SubmitTask()
    -> one worker wakeup / dequeue
    -> one SpillFileStore::Write()
    -> one DiskIoScheduler::SubmitAndWait()
```

影响：

- 262144 / 524288 次 task 入队。
- 262144 / 524288 次 task 出队。
- 大量 mutex lock/unlock、condition_variable notify/wakeup、deque erase。
- 每个 task 只携带 4 KiB payload，调度成本相对 payload 太重。

这个问题和问题 1、问题 2 会叠加：

```text
task 粒度过细
    -> worker 频繁竞争 scheduler 锁
    -> scheduler 每次只执行一个 request
    -> uring 每次只 submit/wait 一个 4 KiB I/O
```

完整日志里的支持点：

- 8 MiB write block 数少两个数量级，BM write measured 只有 2.3 到 4.1 秒。
- 4 KiB write block 数巨大，BM write measured 是 315.8 到 1282.1 秒。
- 4 KiB 8 线程的总数据量只是 4 线程的 2 倍，但 BM write 耗时约 4.1 倍，说明固定成本和共享竞争随并发恶化。

验证建议：

```text
metric: disk_task_submit_count
metric: disk_task_queue_wait_us
metric: disk_task_run_us
metric: disk_task_payload_bytes
metric: disk_task_bytes_per_task
```

预期现象：

- `disk_task_submit_count` 接近 block 数。
- `disk_task_bytes_per_task` 接近 4096。
- `disk_task_queue_wait_us + task dispatch overhead` 在 4 KiB case 中占比明显。

## 问题 4: SpillFileStore 每个小块都有元数据和锁成本

`SpillFileStore::Write()` 每次写入会做：

```text
PrepareSpillPayload()
smallAllocator_.Allocate()
RegisterLiveFile()
liveLocations_.insert()
WriteToLocation()
metrics update
```

4 KiB block 会命中 small spill allocator，但仍然需要 location 分配和 live location 记录。`RegisterLiveFile()` 和 `liveLocations_` 更新都涉及 `SpillFileStore` 内部 mutex。

影响：

- 每个 4 KiB block 都要维护一条 `SpillLocation`。
- small slot 写入减少了小文件数量，但没有消除 per-block metadata 成本。
- 多 worker 并发写时，`SpillFileStore::mutex_` 可能成为次级热点。

完整日志里的支持点：

- 4 KiB case 的 block 数远大于 8 MiB case。
- 4 KiB 8 线程相对 4 线程吞吐下降，符合共享 metadata 锁竞争恶化的模式。
- 但当前日志无法区分 store metadata 和 scheduler 串行化哪个占主导，需要补 metrics。

验证建议：

```text
metric: spill_store_prepare_payload_us
metric: spill_store_allocate_location_us
metric: spill_store_register_location_us
metric: spill_store_write_io_us
metric: spill_store_write_total_us
metric: spill_store_mutex_wait_us
metric: small_allocator_wait_us
```

需要特别区分：

```text
store write total time
    = metadata time
    + scheduler / I/O time
```

如果 metadata time 在 4 KiB 下占比高，说明需要考虑批量 location 分配、批量 metadata commit，或者更粗粒度的 spill unit。

## 问题 5: Reclaim 计入 owner thread commit 成本

benchmark 的 BM write 结果不是“单纯磁盘写入耗时”，而是 `Reclaim()` 完整语义耗时。

`BufferManager::Reclaim()` 会：

```text
DrainPrefetchCompletions()
DrainSpillCompletions()
scan live blocks
EnqueueEvictionCandidate()
TryScheduleEvict()
WaitForProgress()
DrainSpillCompletions()
commit block state
update metrics
return reclaimed bytes
```

这个口径是合理的，因为 BufferManager 对外承诺的是“内存真实被释放”。只有 spill write 完成并由 owner thread commit 后，resident memory 才算 reclaimed。

但它和 direct disk baseline 的口径不同：

```text
direct baseline:
    pwrite finished
    fdatasync finished

BufferManager:
    buffered write accepted by kernel
    + completion delivery
    + owner drain
    + block state commit
    + memory release
```

影响：

- BM write ratio 低不完全等价于 disk I/O 慢。
- 其中一部分是 BufferManager 正确语义需要付出的状态机成本。
- 4 KiB block 数量极大时，commit 成本会被放大。

验证建议：

```text
metric: reclaim_scan_us
metric: reclaim_submit_us
metric: reclaim_wait_us
metric: reclaim_drain_completion_us
metric: reclaim_commit_us
metric: reclaimed_blocks
metric: reclaimed_bytes
```

如果 `reclaim_wait_us` 占大头，说明瓶颈主要在下游 I/O/task。  
如果 `reclaim_drain_completion_us` 或 `reclaim_commit_us` 占比高，说明 owner commit 本身也需要优化。

## 问题 6: BM write 和 disk baseline 的文件访问模型不同

baseline：

```text
one shared file
thread 0 writes offset range 0
thread 1 writes offset range 1
thread 2 writes offset range 2
thread 3 writes offset range 3
O_DIRECT
fdatasync
```

BM spill：

```text
small spill allocator decides location
SpillFileStore writes location.path + location.offset
buffered write
no per-spill fdatasync
live file/location metadata is maintained per block
```

这不一定是“不公平”，因为这些元数据是 BufferManager 功能需要的一部分。但分析 ratio 时要注意：

- baseline 代表较低层的 direct disk write/read 能力。
- BM write 代表 BufferManager 当前 buffered spill 框架的端到端释放内存能力。
- 如果要比较真实设备 I/O 能力，需要让两边的 cache / durability 语义一致。

## 问题 7: Prefetch 完成条件和 owner drain 可能放大高并发成本

benchmark 的 prefetch read 测量逻辑是：

```text
result = manager.Prefetch(blocks)
loop:
    manager.Prefetch({})     # drain completion
    snapshot = manager.Snapshot()
    break when snapshot.usedLoadedBytes >= expected bytes
```

这意味着 prefetch measured elapsed 包含：

- task submit。
- worker read。
- completion push。
- owner thread drain completion。
- block state commit。
- resident memory allocation。
- snapshot polling。

4 KiB prefetch 慢，说明小块 completion/drain/commit 成本很可能很高。

8 MiB 8 线程 prefetch 异常慢，说明大块高并发下可能还有：

- resident memory allocation 压力。
- page cache 命中率下降。
- completion drain 被 owner thread 调度影响。
- 中心队列中大 request 长时间占用 scheduler。

验证建议：

```text
metric: prefetch_submit_us
metric: prefetch_task_queue_wait_us
metric: prefetch_read_io_us
metric: prefetch_completion_queue_us
metric: prefetch_owner_drain_us
metric: prefetch_commit_us
metric: prefetch_alloc_us
```

## 当前优先级判断

按“最可能解释 4 KiB write 低吞吐”的优先级排序：

```text
1. DiskIoScheduler 持锁执行 engine_->Execute()，导致 I/O 执行被中心锁串行化。
2. UringDiskIoEngine 每 request submit/wait，没有真实 queue depth。
3. 每 4 KiB 一个 DiskIoTask，调度粒度过细。
4. SpillFileStore per-block metadata 和 mutex 成本。
5. owner thread drain / commit 成本。
6. BM 和 baseline 的 cache / durability 语义不同。
```

注意：第 6 点不是导致 4 KiB write 慢的直接原因。相反，BM 使用 buffered write 本应让 write 看起来更快；在这种口径下 4 KiB BM write 仍然很慢，说明前 1 到 5 点更值得优先调查。

按“最可能解释 benchmark 数字看起来反直觉”的优先级排序：

```text
1. BM buffered I/O vs baseline O_DIRECT + fdatasync。
2. BM read / Pin /部分 prefetch 可能命中 page cache。
3. progress elapsed 和 measured elapsed 口径不同。
4. 8 MiB block 数太少，per-block overhead 被摊薄。
5. 8 MiB 8 线程 prefetch 可能受内存/page cache/owner drain 影响。
```

## 建议先补的观测点

下一步不建议直接大改实现。先补一层 profiling counters，让瓶颈可见。

建议最小观测集：

```text
BufferManager / Reclaim:
    reclaim_scan_us
    reclaim_submit_us
    reclaim_wait_us
    reclaim_drain_completion_us
    reclaim_commit_us
    reclaimed_blocks
    reclaimed_bytes

DiskIoTaskExecutor:
    disk_task_submit_count
    disk_task_queue_wait_us
    disk_task_run_us
    disk_task_payload_bytes
    disk_task_max_queue_depth

SpillFileStore:
    spill_store_prepare_payload_us
    spill_store_allocate_location_us
    spill_store_register_location_us
    spill_store_write_io_us
    spill_store_write_total_us
    spill_store_read_io_us
    spill_store_mutex_wait_us

DiskIoScheduler:
    scheduler_submit_count
    scheduler_lock_wait_us
    scheduler_execute_under_lock_us
    scheduler_request_bytes

UringDiskIoEngine:
    uring_submit_count
    uring_wait_count
    uring_request_bytes
    uring_inflight_max
    uring_execute_us

Prefetch:
    prefetch_submit_us
    prefetch_owner_drain_us
    prefetch_commit_us
    prefetch_alloc_us
```

建议 benchmark 输出追加以下派生指标：

```text
avg_bytes_per_task
avg_task_queue_wait_us
avg_scheduler_lock_wait_us
avg_uring_execute_us
avg_store_metadata_us
avg_owner_commit_us
avg_prefetch_owner_drain_us
```

建议 benchmark 额外打印或配置：

```text
bm_io_direct=false/true
bm_write_fsync=false/true
disk_baseline_direct_io=false/true
disk_baseline_fsync_after_write=false/true
disk_baseline_drop_cache_before_read=false/true
page_cache_control=none/drop_before_read/drop_between_cases
```

这些字段用于避免后续比较时混淆 buffered I/O 和 direct I/O。

## 后续可讨论的优化方向

这里只列方向，不作为最终方案。

### 方向 A: 让 DiskIoScheduler 真正支持并发和 queue depth

目标：

```text
central admission control remains
but engine execution is not serialized by one mutex
```

可能做法：

- mutex 只保护 admission / priority / accounting 状态。
- I/O execute 不在 scheduler mutex 内执行。
- 用 queue depth permit 控制 in-flight 数量。
- completion 后归还 permit 并更新 adaptive queue depth。

优点：

- 保留“中心管控磁盘”的设计目标。
- 直接解决 worker 增加但 I/O 不扩展的问题。

风险：

- 需要重新梳理 `DiskIoScheduler` 与 `UringDiskIoEngine` 的线程安全边界。
- 如果一个 ring 不能被多个线程安全共享，需要改成单 I/O loop 或 per-worker engine。

### 方向 B: 改造 UringDiskIoEngine 为批量 submit / completion 驱动

目标：

```text
one io_uring ring
many in-flight requests
batch submit
batch reap completions
```

优点：

- 更符合 io_uring 的使用方式。
- 对 4 KiB IOPS 提升最直接。

风险：

- 需要异步 completion 路由。
- `SubmitAndWait()` 接口可能要退到兼容层，核心路径改成 async submit。

### 方向 C: 合并 4 KiB spill task 粒度

目标：

```text
many small blocks
    -> one batch spill task
    -> one or few batched disk requests
    -> one batch completion to owner
```

优点：

- 降低 task queue、worker wakeup、scheduler 调用、metadata 操作次数。
- 4 KiB case 收益明显。

风险：

- batch 内部分失败处理更复杂。
- owner commit 需要支持 batch completion。
- 需要避免 batch 太大导致单 owner 或低优先级 block 被拖延。

### 方向 D: 优化 SpillFileStore small-block metadata

目标：

```text
small block location allocation and registration become cheaper
```

可能做法：

- batch allocate slots。
- per-slab metadata 分片锁。
- live location registry 分片。
- 对小块减少不必要的 live file 重复注册。

优点：

- 对小块场景直接有效。

风险：

- 如果主瓶颈在 scheduler / uring，这部分先做收益有限。

### 方向 E: 统一 benchmark I/O 语义

目标：

```text
BM path and disk baseline use comparable cache / durability semantics
```

可能做法：

- 给 BM spill file 增加可选 `O_DIRECT`。
- 给 BM write 增加可选 fsync/fdatasync 策略。
- disk baseline 默认使用 buffered mode，与 BM 当前模式做框架开销对比。
- read benchmark 增加 cold-read 模式，避免刚写完马上读 page cache。

优点：

- 能把“框架成本”和“设备 I/O 能力”拆开。
- 后续优化结果更容易解释。

风险：

- direct I/O 要处理 alignment、buffer lifecycle、small spill slot alignment。
- fsync 策略会改变 BufferManager 的语义和性能，需要作为 benchmark option，而不是直接改变默认 runtime 行为。

## 建议的下一步顺序

建议按下面顺序推进：

```text
step 1:
    给 benchmark 和核心路径补 profiling counters。
    目标是确认 scheduler lock、task queue、uring submit/wait、store metadata、owner commit 的占比。

step 2:
    增加 benchmark I/O 语义标记。
    至少明确输出 BM 是 buffered write/read，以及 baseline 是否使用 direct I/O、fdatasync、drop-cache。

step 3:
    跑 disk_io_workers=1,2,4,8 矩阵。
    如果 worker 数增加不能改善 4 KiB write，优先改 scheduler / uring。

step 4:
    跑 block size 梯度。
    例如 4 KiB, 16 KiB, 64 KiB, 256 KiB, 1 MiB, 8 MiB。
    找出 per-block 固定成本从主导到不主导的拐点。

step 5:
    根据 counters 决定优化路线。
    如果 scheduler lock/uring wait 占大头，先改方向 A/B。
    如果 task queue/metadata 占大头，先改方向 C/D。
    如果 read/prefetch 数字继续反直觉，先做方向 E。
```

## 最终判断

当前完整日志支持这个判断：

```text
4 KiB spill write 的主要问题不是磁盘不够快，而是 BufferManager 小块 spill 框架路径的固定成本和共享串行点过重。
```

更具体地说：

```text
4 KiB write:
    buffered BM write 仍远慢于 O_DIRECT baseline
    -> 框架固定成本非常重

8 MiB write:
    BM 快于 O_DIRECT baseline
    -> 口径受 page cache 影响，不能证明设备级 I/O 没问题
    -> 但说明 per-block 框架成本在大块下被摊薄

4 KiB prefetch:
    buffered read 仍慢于 O_DIRECT baseline
    -> 小块异步 task/completion/commit 路径也很重

Pin read:
    明显快于 baseline
    -> 主要是 page cache，不应用来证明真实 disk read 性能
```

因此，优化前应先补充 profiling counters 和 benchmark 口径标记。随后优先调查 `DiskIoScheduler` 串行化、`UringDiskIoEngine` 单 request submit/wait、以及 4 KiB one-block-one-task 的调度粒度。
