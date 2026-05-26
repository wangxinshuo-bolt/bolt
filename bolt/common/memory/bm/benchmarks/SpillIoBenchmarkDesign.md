# BufferManager Spill I/O Benchmark Design

本文档描述一个独立 benchmark，用于量化 BufferManager 框架下 spill write、普通 read reload、Prefetch read 的吞吐，并和 direct disk I/O baseline 对比。

目标可执行文件：

```text
bolt_buffer_manager_spill_io_benchmark
```

## 测量目标

Benchmark 需要回答两个问题：

1. 当 block 都是 4 KiB 时，BufferManager 框架下的 write/read IOPS 是多少，和 disk direct I/O IOPS 相比损耗多少。
2. 当 block 都是大块，例如 8 MiB 时，BufferManager 框架下的 write/read bandwidth 是多少，和 disk direct I/O bandwidth 相比损耗多少。

读路径需要同时覆盖：

- `Pin()` 触发的普通 reload read。
- `Prefetch()` 触发的异步预取 read。

## 测试矩阵

```text
+-------------+---------+-------------------------+-----------------------------+
| block size  | threads | BufferManager cases     | disk baseline cases         |
+-------------+---------+-------------------------+-----------------------------+
| 4 KiB       | 4       | spill write IOPS        | direct write IOPS           |
| 4 KiB       | 4       | pin reload read IOPS    | direct read IOPS            |
| 4 KiB       | 4       | prefetch read IOPS      | direct read IOPS            |
| 4 KiB       | 8       | spill write IOPS        | direct write IOPS           |
| 4 KiB       | 8       | pin reload read IOPS    | direct read IOPS            |
| 4 KiB       | 8       | prefetch read IOPS      | direct read IOPS            |
| 8 MiB       | 4       | spill write MiB/s       | direct write MiB/s          |
| 8 MiB       | 4       | pin reload read MiB/s   | direct read MiB/s           |
| 8 MiB       | 4       | prefetch read MiB/s     | direct read MiB/s           |
| 8 MiB       | 8       | spill write MiB/s       | direct write MiB/s          |
| 8 MiB       | 8       | pin reload read MiB/s   | direct read MiB/s           |
| 8 MiB       | 8       | prefetch read MiB/s     | direct read MiB/s           |
+-------------+---------+-------------------------+-----------------------------+
```

默认线程数为 `4,8`。这里的线程数是 BufferManager owner/client threads 数；`DiskIoTaskExecutor` worker 数由独立 flag 控制。

## 组件路径

```text
BM write / read cases:

worker thread
    |
    v
BufferManager
    |
    v
SpillCoordinator
    |
    v
DiskIoTaskExecutor
    |
    v
SpillFileStore
    |
    v
DiskIoScheduler / UringDiskIoEngine

Disk baseline cases:

worker thread
    |
    v
O_DIRECT pwrite / pread
    |
    v
dedicated benchmark file
```

## BufferManager Write 测法

每个 worker thread 持有自己的 `BufferManager`。所有 `BufferManager` 共享进程级 `SpillCoordinator`、`DiskIoTaskExecutor` 和 `SpillFileStore`。

流程：

```text
for each thread:
    create BufferManager
    AllocatePersistent N blocks
      block size = 4 KiB or 8 MiB
      policy = kSpillToDisk
      payload = deterministic bytes
    keep shared_ptr<BlockHandle> list

barrier
start timer

for each thread:
    Reclaim(total_thread_bytes)

stop timer after all threads return
```

`Reclaim()` 返回实际释放的 resident bytes。用它计时可以确保 benchmark 统计的是完成 spill write 并由 owner thread commit 后的吞吐，而不是只统计 task submit 速度。

输出指标：

```text
bm_write_blocks
bm_write_bytes
bm_write_elapsed_ms
bm_write_iops
bm_write_mib_per_sec
```

4 KiB case 主要看 `bm_write_iops`。8 MiB case 主要看 `bm_write_mib_per_sec`。

## 普通 Read 测法

普通 read 使用 `BufferManager::Pin()` 触发 reload。该路径代表用户真正访问一个 spilled block 时看到的同步读成本。

准备阶段：

```text
for each thread:
    create and fill blocks
    Reclaim(total_thread_bytes)
    verify Snapshot().usedSpilledBytes reaches expected bytes
```

测量阶段：

```text
barrier
start timer

for each thread:
    for block in blocks:
        handle = manager.Pin(block)
        touch one byte or checksum payload
        drop handle

stop timer after all threads return
```

输出指标：

```text
bm_pin_read_blocks
bm_pin_read_bytes
bm_pin_read_elapsed_ms
bm_pin_read_iops
bm_pin_read_mib_per_sec
```

注意：普通 read 会把 block 重新变成 resident memory。如果还要在同一轮继续测 Prefetch read，需要重新 spill，或者为 Prefetch read 准备另一批 block。

## Prefetch Read 测法

Prefetch read 使用 `BufferManager::Prefetch(blocks)` 提交异步 reload，然后通过 `Prefetch({})` drain completion。该路径代表预取接口在 BufferManager 框架下的读吞吐上限。

准备阶段：

```text
for each thread:
    create and fill blocks
    Reclaim(total_thread_bytes)
    verify Snapshot().usedSpilledBytes reaches expected bytes
```

测量阶段：

```text
barrier
start timer

for each thread:
    result = manager.Prefetch(blocks)
    loop:
        manager.Prefetch({})     # drain-only
        snapshot = manager.Snapshot()
        break when snapshot.usedLoadedBytes >= expected bytes

stop timer after all threads return
```

输出指标：

```text
bm_prefetch_submitted_blocks
bm_prefetch_skipped_blocks
bm_prefetch_bytes
bm_prefetch_elapsed_ms
bm_prefetch_iops
bm_prefetch_mib_per_sec
```

`Prefetch()` 不 pin block，因此测得的是异步读入 resident memory 的能力，不包含用户持有 pin 的时间。

## Disk Baseline 测法

Disk baseline 使用 direct I/O。所有 benchmark worker threads 共享同一个 baseline 文件，每个线程负责该文件中的一个 offset range。

这里不使用“每线程独立文件”。共享文件上的 offset 分配、inode 状态、文件系统调度和多线程提交竞争，也是实际磁盘访问路径的损耗之一，应计入 baseline。这样 BM 结果和 disk baseline 都是在多线程共享同一磁盘资源的场景下比较。

写 baseline：

```text
for each thread:
    open the same file with O_DIRECT | O_RDWR
    thread_range = [thread_id * total_thread_bytes,
                    (thread_id + 1) * total_thread_bytes)
    allocate aligned buffer

main thread:
    create/truncate the shared file
    posix_fallocate(total_bytes)

barrier
start timer

for each thread:
    for offset in thread_range:
        pwrite(fd, aligned_buffer, block_bytes, offset)
    fdatasync(fd)

stop timer after all threads return
```

读 baseline：

```text
for each thread:
    shared file has been filled by write baseline
    fdatasync(fd)
    posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED)
    thread_range = [thread_id * total_thread_bytes,
                    (thread_id + 1) * total_thread_bytes)

barrier
start timer

for each thread:
    for offset in thread_range:
        pread(fd, aligned_buffer, block_bytes, offset)

stop timer after all threads return
```

输出指标：

```text
disk_write_iops
disk_write_mib_per_sec
disk_read_iops
disk_read_mib_per_sec
direct_io_used
```

direct I/O baseline 不是设备裸测，但它可以避开大部分 page cache 影响，适合作为 BufferManager 框架损耗的对照。

## 输出格式

每个 block size 和线程数组合输出一行 summary：

```text
BMSpillIoBenchmarkResult \
case=4k \
threads=4 \
block_bytes=4096 \
total_bytes=1073741824 \
blocks=262144 \
bm_write_iops=... \
bm_pin_read_iops=... \
bm_prefetch_iops=... \
disk_write_iops=... \
disk_read_iops=... \
write_ratio=... \
pin_read_ratio=... \
prefetch_read_ratio=...
```

```text
BMSpillIoBenchmarkResult \
case=8mb \
threads=8 \
block_bytes=8388608 \
total_bytes=8589934592 \
blocks=1024 \
bm_write_mib_per_sec=... \
bm_pin_read_mib_per_sec=... \
bm_prefetch_mib_per_sec=... \
disk_write_mib_per_sec=... \
disk_read_mib_per_sec=... \
write_ratio=... \
pin_read_ratio=... \
prefetch_read_ratio=...
```

Ratio 定义：

```text
write_ratio = bm_write / disk_write
pin_read_ratio = bm_pin_read / disk_read
prefetch_read_ratio = bm_prefetch_read / disk_read
```

## 建议 Flags

```text
--bm_spill_io_benchmark_spill_dir=/tmp/bolt_bm_spill_io_benchmark
--bm_spill_io_benchmark_threads=4,8
--bm_spill_io_benchmark_small_block_bytes=4096
--bm_spill_io_benchmark_large_block_bytes=8388608
--bm_spill_io_benchmark_small_total_mb_per_thread=256
--bm_spill_io_benchmark_large_total_mb_per_thread=1024
--bm_spill_io_benchmark_disk_io_workers=4
--bm_spill_io_benchmark_disable_compression=true
--bm_spill_io_benchmark_cleanup=true
--bm_spill_io_benchmark_run_disk_baseline=true
--bm_spill_io_benchmark_run_pin_read=true
--bm_spill_io_benchmark_run_prefetch_read=true
```

## 默认配置建议

1. 默认关闭 compression。

   这个 benchmark 的目标是测 BufferManager I/O 框架损耗。压缩会引入 CPU 成本和数据缩小收益，使结果难以解释。

2. 默认同时测普通 read 和 Prefetch read。

   普通 read 代表用户同步访问 spilled block 的成本；Prefetch read 代表预取接口和中心 I/O 队列的异步吞吐能力。两者都需要保留。

3. 默认使用 4 和 8 个 owner threads。

   这能观察多 BufferManager 并发提交时，中心 `DiskIoTaskExecutor` 和 `SpillFileStore` 的扩展性。

4. `DiskIoTaskExecutor` worker 数独立配置，默认值为 4。

   owner threads 和 disk task workers 是两个维度。第一版默认 `disk_io_workers=4`，后续如果要找瓶颈，可以扩展成 worker 数矩阵。

5. 小块和大块使用不同默认总量。

   4 KiB case 需要足够多 block 才能稳定 IOPS；8 MiB case 需要足够大总量才有稳定 bandwidth，但不宜默认占用过多内存和磁盘。

## 注意事项

- BM write case 的计时应从所有线程开始 `Reclaim()` 后算起，到所有线程 `Reclaim()` 返回后结束。
- BM read case 必须先确认 block 已经处于 spilled 状态。
- 普通 read 和 Prefetch read 不能复用同一批已经被 read 回 resident 的 block，除非中间重新 spill。
- Disk baseline 应使用 direct I/O 和 aligned buffer；所有线程共享同一个 baseline 文件，但写入和读取不同 offset range。若 direct I/O 打不开，应显式打印 `direct_io_used=false`，不要静默退化。
- 输出中需要打印实际 `total_bytes`、`block_bytes`、`threads`、`disk_io_workers`、`compression_enabled`，避免后续比较结果时丢失上下文。
