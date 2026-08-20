# Gluten + Bolt Serial HashJoin 使用 BmRowContainer 的调查结论与实施路线

## 文档状态

- 调查基线：`8d1135d8af448e0c55e77790814f5b020443492f`
- 文档日期：2026-08-19
- 调查范围：`BufferManager`、`BmRowContainer`、`HashBuild`、`HashTable`、`HashProbe`、`HashJoinBridge`、Gluten Bolt执行入口、BM压缩层、相关测试及历史 BM Window 集成
- 运行时范围：只支持Gluten Bolt backend的默认serial路径：`spark.gluten.parallel.enabled=false`、`Task::ExecutionMode::kSerial`、QueryCtx无driver executor、ungrouped、non-morsel
- 算子范围：一个 `HashBuild` pipeline driver、一个 `HashProbe` pipeline driver；首版每个Gluten native Task只允许一个BM-enabled HashJoin
- 串行契约：Gluten `ResultIterator` 的普通 `hasNext()/next()/spillFixedSize()` 竞争同一个独占active-call token；token可跨blocking wait，确保等待期间也不能进入另一个普通JNI调用。实际Task/operator/BM step再经可重入execution guard串行进入；`nativeClose` 是唯一允许与active call重叠的控制调用，它在短持execution guard时发出cancel、随后在guard外drain再teardown。不要求永久绑定同一OS thread
- Legacy对照范围：只比较 `row_based_spill_mode=raw|compression` 的RowContainer row-based路径，不讨论其他build spill格式
- 压缩目标：新增OpenZL backend，固定官方release `v0.2.0`；通过正确性和性能门槛后，把BM默认从当前 `kLz4Block` 切到OpenZL
- 文档性质：源码调查、架构结论和实施路线，不代表实现已经完成或测试已经通过

## 1. 结论摘要

### 1.1 核心判断

现有 RowContainer 与 BmRowContainer 的 spill/restore 生命周期可以对齐：

```text
现有 HashJoin

RowContainer resident char*
  -> row-based Spiller 按partition写serialized-row stream
  -> table_->clear()
  -> 旧 char* 失效
  -> RowBasedSpillReadFile返回reader-buffer中的serialized rows
  -> copySerializedRow()写入新的 RowContainer
  -> 取得新的 char*
  -> prepareJoinTable() 重建索引

BmRowContainer 目标路径

BmRowContainer resident char*
  -> 销毁当前 HashTable/index
  -> finalize + unpin + BM spill
  -> 旧 char* 失效
  -> BatchPin 当前 partition blocks
  -> StringView rebase
  -> 取得新的 char*
  -> 重置/重建 join runtime metadata
  -> prepareJoinTable() 重建索引
```

两条路径都只要求 pointer 在一个 resident/probe epoch 内稳定，不要求 pointer 跨 spill 永久稳定。

### 1.2 真正的实现差异

差异主要集中在四个方面：

1. BmRowContainer 当前 row layout只有用户列，没有 duplicate link、probed flag、normalized-key prefix等 HashJoin metadata。
2. BmRowContainer 当前缺少 row-vs-`DecodedVector` equality、row hash、HashJoin iterator和probed flag API。
3. 现有 concrete `HashTable` 直接依赖 `RowContainer` 布局与方法，尚没有 row-store abstraction。
4. 当前 `BmRowContainer` 已通过每个 `BlockRef.handle` 持续pin resident blocks；HashJoin不需要再pin一份，只需round-level逻辑 `RoundLease` 禁止任何会清handle或销毁segment/chunk metadata的API。

### 1.3 推荐主路线

主路线改为让 BmRowContainer 直接承载完整 build rows：

1. 为 BmRowContainer 增加 join layout/runtime metadata。
2. 抽取 HashTable 所需的 join-row storage contract。
3. 先实现 resident-only 的 BM HashJoin。
4. 再实现 spill/unpin/reload/重建 HashTable。
5. 然后由唯一的HashBuild和唯一的HashProbe对串行partition rounds和其他join types逐项扩展。
6. BM spill压缩先以opt-in方式接入OpenZL serial/generic graph；等真实fixed-width HashJoin row spill路径落地后，再增加layout descriptor和split-by-struct profile；所有真实benchmark完成并分别通过LZ4门槛后，才在独立阶段切换BM默认codec。

现有 HybridContainer-first 路线保留为可选的低风险性能实验，不再作为主架构。

### 1.4 Gluten serial 下与 RowContainer row-based spill 的性能对比

本文唯一Legacy性能基线是当前Gluten serial HashJoin可启用的RowContainer row-based spill。源码中的两条容器级路径是：

```text
Legacy row-based
  RowContainer rows
    -> list/hash/partition row pointers
    -> ContainerRow2RowSerde::serialize()
    -> row-based spill file
    -> RowBasedSpillReadFile::nextBatch(vector<char*>)
    -> reader buffer中的serialized rows
    -> 新RowContainer::copySerializedRow()
    -> 新char*列表

BM full-row
  BmRowContainer row/heap blocks
    -> seal partition segments
    -> unpin并直接spill blocks
    -> BatchPin原blocks
    -> 刷新block地址、rebase StringView
    -> 新char*列表
```

对应的性能判断如下：

| 阶段 | BmRowContainer的潜在优势 | 必须计入的代价与边界 |
|---|---|---|
| build append | 当前BM实现先批量reserve rows，再按列和连续row ranges写fixed-width cells | HashBuild还需实现partitioned selected batch append；join metadata和hash成本不在现有容器benchmark中 |
| spill write | 直接spill row/heap blocks，省去Legacy逐row `ContainerRow2RowSerde::serialize()` | block slack、heap tail和BM metadata可能增加物理写出；fixed/raw历史结果中BM写出量确实更大 |
| spill read/restore | 原blocks读回后直接恢复row pointers；不再为每行调用 `copySerializedRow()`，变长payload也不必重新深拷贝到新的HashStringAllocator | StringView仍需rebase；HashTable、duplicate chain和join runtime metadata仍必须重建 |
| resident Probe | 没有天然优势；两者都可向bucket暴露resident `char*` | accessor若引入逐row虚调用或sidecar随机访问，BM可能更慢 |
| resident内存 | 可减少restore时reader buffer到新RowContainer的分配和复制 | 两条路径都只让当前build partition resident；BM还要求当前partition整轮pin住，不能宣称必然降低peak memory |
| 再次回收 | 有spill backing、unpinned且保持clean的block可以direct discard | StringView rebase和row内join metadata mutation都会把row block标dirty；这一点不是普遍优势 |

因此，最明确的预期收益是省掉RowContainer row-based restore中的“reader serialized rows -> `copySerializedRow()` -> 新RowContainer”逐row重建，以及省掉spill写侧逐row serde。HashTable index在两条路径中都必须重建，BM不会消除这部分成本。

commit `8f1be66ecf`归档了一次2026-06-16的single-caller synthetic row-storage pipeline运行。Old侧明确使用 `Spiller::Type::kHashJoinBuild` 的row-based模式，restore明确创建新RowContainer并调用 `copySerializedRow()`；所以spill格式和restore机制与本文要求的Legacy基线一致。但该程序没有创建Driver、HashBuild、HashTable或HashProbe，不能称为Gluten HashJoin实测。

每个case处理25 GiB逻辑输入，但只物化并循环复用128 MiB input batches；同一进程另有128 MiB不计时warmup。每个case在独立进程中只测一次（`iterations=1`），case前执行 `sync` 和 `drop_caches`。这是single-caller口径，BM内部仍可通过DiskIoScheduler并发提交block I/O。计时边界仅为 `store -> row-based spill write -> row-based spill read/restore -> extract`，表中的total是四段计时之和，不是进程或算子wall time：

| 数据 | 压缩 | Old阶段和 | BM阶段和 | 加速比 |
|---|---|---:|---:|---:|
| fixed | raw | 219.36s | 40.55s | 5.41x |
| fixed | lz4 | 232.67s | 92.29s | 2.52x |
| fixed | zstd | 364.73s | 243.21s | 1.50x |
| variable | raw | 74.85s | 30.46s | 2.46x |
| variable | lz4 | 39.58s | 24.07s | 1.64x |
| variable | zstd | 45.61s | 34.93s | 1.31x |

fixed/raw的阶段数据：

| 阶段 | Old | BM | 加速比 |
|---|---:|---:|---:|
| batch store | 26.50s | 6.72s | 3.95x |
| spill write | 42.40s | 9.99s | 4.24x |
| spill read/restore | 143.48s | 18.99s | 7.56x |
| resident extract | 6.98s | 4.86s | 1.44x |

variable/raw中store和resident extract优势很小，主要收益来自spill链路：

| 阶段 | Old | BM | 加速比 |
|---|---:|---:|---:|
| batch store | 5.60s | 5.08s | 1.10x |
| spill write | 14.89s | 8.95s | 1.66x |
| spill read/restore | 51.36s | 13.65s | 3.76x |
| resident extract | 3.00s | 2.78s | 1.08x |

其中fixed数据是 `BIGINT + INTEGER + DOUBLE`；当时的variable数据额外包含固定1024-byte string，相当于当前profile中的large-string合成场景，不能泛化到任意字符串长度、null或skew分布。Old使用简化RowContainer layout，BM也尚无join metadata；二者都不等同于最终HashJoin row layout。结果归档没有完整记录运行binary的精确Git hash、硬件和build flags，也没有重复运行或置信区间。

这些单次观察值说明BM row-storage pipeline值得接入验证，但不能作为HashJoin端到端加速比。它们不包含partition hash、bucket build、duplicate-chain rebuild、join runtime metadata、join filter、Probe和output。

对本文Gluten serial HashJoin的合理预期是：

- 无spill、fixed-width：selected batch append可能更快，但HashTable/Probe占比高时端到端收益会被稀释。
- 无spill、变长列：可能接近持平，BM heap metadata甚至可能略慢。
- 有spill：最有希望获益，尤其restore省去serialized rows到新RowContainer的逐row复制和变长payload深拷贝。
- 压缩：历史数据只覆盖raw/lz4/zstd；OpenZL尚无Bolt实测，不能把官方其他数据集结果直接外推。当前BM默认是LZ4而非Zstd。
- Probe热路径：尚无性能证据；必须通过HashJoin benchmark判断是否持平或退化。

### 1.5 总体置信度

- spill/restore生命周期可对齐：0.99
- BmRowContainer可以直接承载完整build rows：0.95
- 需要HashTable row-store适配而非机械替换：0.99
- Gluten serial、generic-hash、fixed-width MVP可实施：0.95
- Gluten serial recursive repartition可实施：0.86
- 相对RowContainer row-based spill的容器pipeline优势：有一次历史single-caller synthetic实测
- HashJoin端到端性能收益：必须新增benchmark验证
- OpenZL可作为BM block backend接入：0.95；能否成为默认取决于Bolt/Gluten benchmark

## 2. 关键术语与不变量

### 2.1 Resident epoch 与 RoundLease

Resident epoch 指一组 row blocks保持 pinned、对应 `char*` 可以被安全解引用的时间段。

现有 `BlockRef.handle` 就是唯一的physical pin owner。`RoundLease` 是不新增handle的逻辑capability：它覆盖current partition，并拒绝finalize/spill、release segment/chunk、`popFrontRows()`、window release/evict、consuming merge release和container销毁。

一个 epoch 的边界是：

```text
开始：所有本轮 blocks 已 pin，所有 pointers 和 runtime metadata 已建立
结束：HashTable索引已销毁，所有可能缓存旧地址的Probe状态已清空或变为不可达，随后才能unpin
```

### 2.2 Probe round

Probe round 指唯一的 HashBuild发布一张 HashTable，到唯一的 HashProbe完成该表处理的阶段。发生 spill 时，一个 query可以有多个串行Probe rounds，每轮对应一个内存中的build partition；这不引入额外driver。

### 2.3 Persisted row data 与 runtime metadata

需要跨 spill保存的内容：

- key values；
- dependent/payload values；
- null bits；
- partition identity和row count。

只在当前 resident epoch有效、restore后需要重置或重建的内容：

- HashTable buckets；
- duplicate `next` pointers；
- probed flags；
- normalized keys（可以持久化，但首版建议重算）；
- cached `char*`；
- HashLookup hits和iterators。

### 2.4 必须始终成立的不变量

1. 任何 `char*` 的有效期不超过其 resident epoch。
2. spill/unpin 前必须先销毁引用该 epoch pointers 的 HashTable和iterators。
3. restore后不得使用spill前的bucket、next pointer或cached row pointer。
4. HashTable发布给Probe前，当前partition全部row/heap blocks必须保持pinned。
5. 当前HashProbe结束并释放table前，RoundLease覆盖的任何 `BlockRef.handle` 都不能被清空。
6. 当前BM backend拥有的build rows不能同时交给旧build Spiller再次管理。
7. 中间round销毁当前table只能撤销current RoundLease，不能销毁仍保存其他partitions的join-lifetime storage。
8. 每次resident epoch切换必须递增该partition自己的generation，并显式清空 `HashLookup::hits`、`JoinResultIterator::nextHit`、`outputTableRows_` 和last-probe iterator等旧地址缓存。
9. `BmRestoreToken` 必须在Gluten execution guard内以 `{SpillPartitionId, partitionGeneration}` validate-and-consume；旧generation或重复consume必须失败，不需要并发原子协议。
10. 在Gluten serial支持范围内，普通BmRowContainer/coordinator/Bridge-facing操作和storage teardown必须由同一个iterator-lifetime control block与execution guard串行化。允许的off-thread路径只有三类：纯BM candidate scan/physical reclaim只进入BM state同步；composite `reclaimableBytes()` 在Task pause前只读取atomic logical-victim snapshot；composite `reclaim()` 只在Task已pause后进入storage state同步。三者都不得取得Gluten execution guard。
11. storage mutex是BmHashJoinStorage/coordinator/RoundLease状态的权威互斥域。普通路径按“active-call lease -> execution guard -> 短持storage mutex”进入；composite mutation路径按“Task pause -> 短持storage mutex”进入。每次影响victim资格的状态迁移都在该mutex内重新计算并release-store聚合的 `sealableLogicalBytes`；pause前的candidate scan只acquire-load该atomic，不读取partition catalog。storage mutex内只做validate、状态迁移、generation更新、snapshot发布和move handles，不执行allocation、Pin/Unpin、压缩、I/O或arbitration，也不与Bridge mutex或BM state mutex嵌套。
12. 初始化capability gate在第一批build input前失败时可以稳定选择Legacy；BM backend一旦commit并写入任意build row，后来检测到parallel执行、普通 `nativeHasNext/nativeNext/nativeSpill` token重叠或lifetime协议违规必须失败query并完成teardown，不能把同一份build状态静默切回Legacy。与active call重叠的 `nativeClose` 不是违规，而是专门设计的cancel/drain路径。
13. 独占active-call token覆盖完整普通JNI调用，用于阻止另一普通调用和last-owner析构；execution guard只覆盖实际 `task_->next()`、spill、operator close和BM mutation。`nextInternal()` 的 `ContinueFuture::wait()` 前必须释放guard但保留token；close的cancel-future wait也不持guard。closing先阻止新token，再取得execution guard调用幂等 `requestCancel()`，使其同步触发的off-thread driver/Bridge close也受保护；取得cancel future后立即释放guard。随后在guard外等待现有token和cancel future，最后重新取得guard执行唯一last-owner teardown。

## 3. 现有 RowContainer HashJoin 的真实生命周期

### 3.1 Build 阶段

在本文限定的执行形态中，唯一的 HashBuild driver拥有一张 `BaseHashTable` 和一个 `RowContainer`。输入经decode后：

- keys和dependents写入RowContainer；
- 非去重路径通常先积累rows，结束时建立最终HashTable；
- semi/anti去重路径可能在addInput期间执行group probe；
- row pointer在当前RowContainer未clear时稳定。

### 3.2 Spill 阶段

单build/probe pipeline下现有HashJoin可以启用RowContainer row-based spill。它不是让RowContainer自己管理spill，而是让 `Spiller` 读取RowContainer rows：

1. 按hash partition收集row pointers；
2. `ContainerRow2RowSerde::serialize()` 把rows编码成row-based spill stream；
3. `table_->clear()`；
4. RowContainer allocations被释放；
5. 所有旧 `char*` 失效。

关键路径：

- `HashBuild::spillRowBasedInput()`：`bolt/exec/HashBuild.cpp:852-864`
- `HashBuild::runSpill()`：`bolt/exec/HashBuild.cpp:1013-1040`
- `Spiller::writeSpill()` row-container分支：`bolt/exec/Spiller.cpp:652-700`
- `SpillWriter::write(rows, info)`：`bolt/exec/SpillFile.cpp:470-518`
- `RowContainer::clear()`：`bolt/exec/RowContainer.cpp:1047-1080`

这说明现有 HashTable本身已经建立在“spill会结束当前pointer epoch”的假设上。

### 3.3 Restore 阶段

Bridge选择一个spilled partition并交给唯一的HashBuild driver。HashBuild：

1. reset旧table、spiller和reader；
2. `RowBasedSpillReadFile::nextBatch()` 从reader buffer返回serialized row pointers；
3. `addSpilledRowInput()` 对每一行调用 `RowContainer::copySerializedRow()`；
4. 在新的RowContainer中得到新的 `char*`；
5. `prepareJoinTable()` 重建buckets和duplicate chains；
6. 发布新表给Probe。

关键路径：

- `HashBuild::setupSpillInput()`：`bolt/exec/HashBuild.cpp:1429-1464`
- `HashBuild::processSpillInput()` row-based分支：`bolt/exec/HashBuild.cpp:1488-1499`
- `HashBuild::addSpilledRowInput()`：`bolt/exec/HashBuild.cpp:511-532`
- `RowBasedSpillReadFile::nextBatch()`：`bolt/exec/SpillFile.cpp:650-700`
- `RowContainer::copySerializedRow()`：`bolt/exec/RowContainer.cpp:762-795`
- `HashBuild::finishHashBuild()`：`bolt/exec/HashBuild.cpp:1087-1285`

### 3.4 Probe 阶段的 pointer 要求

当前HashTable发布后，以下对象会保存或传播build row pointers：

- bucket slots；
- row内duplicate `next`；
- `HashLookup::hits`；
- `JoinResultIterator::nextHit`；
- `HashProbe::outputTableRows_`；
- join filter输入；
- right/full/semi last-probe iterator。

这些对象要求pointer在当前Probe round内稳定，但不要求跨下一次spill/restore稳定。

## 4. BmRowContainer 已有能力

### 4.1 物理存储

```text
BmRowContainer
└── BmSegmentCollection
    └── SegmentData
        └── ChunkData
            ├── rowBlock
            ├── heapBlocks
            └── heapBases
```

一个chunk对应一个row block，并拥有该chunk的所有变长payload heap blocks。`BlockRef` 同时保存：

- 稳定的 `BlockHandle`；
- 当前pin对应的 `BufferHandle`；
- 仅在pin有效时可解引用的 `ptr`。

### 4.2 Resident pointer

active segment的row/heap blocks由 `BufferHandle` 持续pin住。因此：

- `appendRow()` 可以返回 `char*`；
- 新chunk创建不会移动旧block；
- 继续append不会使旧resident pointer失效；
- 在release当前handles前，pointer可以直接给HashTable使用。

reload后的finalized segments也是同一模型：`BmRowBlockLoader` 把 `BatchPin()` 返回的handles移动到各自 `BlockRef.handle`，而 `BulkReadSession` 本身不拥有handle、析构也不会unpin。所以“不主动清segment/chunk handles就保持pinned”已经是现有语义。BM reclaim只选择 `pinCount == 0` 的blocks，current partition无需额外pin。

### 4.3 Spill 与 reload

`spillActivePartitionSegment()` 已完成：

1. finalize active segment；
2. 释放row/heap handles；
3. 清空cached pointers；
4. 调用BufferManager spill；
5. 保存稳定的segment/block metadata。

需要注意，当前 `finalizeAndFlushSegment()` 会立即释放handles并同步spill所有blocks，`kFinalizedResident` 只是函数内部过渡状态。HashJoin为了“seal后由reclaimer择机spill”和“有clean backing时direct discard”，需要把 `seal`、`unpin`、`spill/writeback` 拆成独立操作；不能把当前 `spillActivePartitionSegment()` 直接当作完整coordinator API。

`BulkReadSession` 和 `BmRowBlockLoader` 已完成：

1. 收集当前segments的blocks；
2. `BatchPin()`；
3. 刷新每个BlockRef的pointer；
4. 修复non-inline `StringView`；
5. 返回本轮新的row pointers。

所以 BmRowContainer 已经具备实现“旧pointer失效、restore后重建pointer”的底层能力，也具备round期间持续pin的底层能力。缺少的是把所有会清handle的API统一gate在RoundLease之后。

### 4.4 当前 row layout

当前布局是：

```text
[nullable bits][column 0 cell][column 1 cell]...[column N cell]
```

它支持fixed-width primitive、`VARCHAR`、`VARBINARY`，但尚未包含HashJoin runtime metadata，也不支持复杂类型。

### 4.5 当前需要加强的能力

- append transaction/rollback；
- selected/partitioned batch append；
- join-specific row layout；
- row-vs-decoded-vector equality；
- row hash和normalized-key访问；
- HashJoin rows/probed/null-key iteration；
- round-level logical `RoundLease`，不新增physical pin；
- RoundLease对finalize/release/evict/pop-front/consuming-merge/container-destroy的统一gate；
- sparse partition metadata，消除固定256 partitions限制。

## 5. RowContainer 与 BmRowContainer 的准确对比

| 维度 | 现有 RowContainer | BmRowContainer | 结论 |
|---|---|---|---|
| resident阶段返回 `char*` | 是 | 是 | 等价 |
| append后pointer稳定 | 是，直到clear | 是，直到unpin/release | 可对齐 |
| spill后旧pointer | 失效 | 失效 | 等价 |
| restore后pointer | 重写rows得到新pointer | repin blocks得到新pointer | 都需重建HashTable |
| key/payload持久化 | row-based Spiller逐row serde | BM block backing | BM可避免serialized-row编码和restore复制 |
| duplicate link | row内字段 | 当前没有 | 需增加或sidecar |
| probed flag | row内bit | 当前没有 | 需增加或sidecar |
| normalized key | row前缀 | 当前没有 | 需增加/重算 |
| row-vs-vector equality/hash | 有 | 当前没有 | 需增加 |
| strings reload | 新RowContainer重新复制 | 原blocks读回后rebase | 已有底层能力 |
| partition管理 | Spiller | 原生segmentsForPartition | BM具备基础 |
| complex types | 支持 | 当前不足 | 先fallback |
| 内存回收 | 整partition serialize+clear | block/segment spill | BM粒度更细但需epoch约束 |

准确结论是：

> 生命周期差异不大；实现差异主要在join layout、HashTable contract和pin ownership。

## 6. 目标架构

### 6.1 总体结构

```text
single HashBuild driver
├── BmHashJoinStorage（join lifetime）
│   ├── BmJoinRowContainer
│   │   ├── persisted key/payload cells
│   │   ├── join runtime metadata
│   │   └── partition -> segments
│   ├── shared Task-level BufferManager（首版Task内只有本HashJoin使用）
│   └── BmHashBuildSpillCoordinator
├── BmHashTable / HashTable<RowStore>
└── publish current table

single HashProbe driver
└── current HashTable

HashTable::RoundStorageGuard（round lifetime）
├── shared_ptr<BmHashJoinStorage>
├── RoundLease（logical，uses existing BlockRef.handle pins）
├── current partition/per-partition generation
└── 与table共生命周期的销毁协议

HashJoinBridge pending BM restore token
├── shared_ptr<BmHashJoinStorage>
├── SpillPartitionId
└── partition locator/per-partition generation
```

`BmHashJoinStorage` 必须比任意一轮table存活更久：中间round销毁 `RoundStorageGuard` 只撤销current RoundLease；Bridge中的pending restore token继续强持有storage和剩余partitions。最终round完成或Task teardown后，最后一个storage owner才销毁BmJoinRowContainer和BufferManager。

### 6.2 Join-capable Bm row layout

建议把BmRowContainer扩展成可选join layout，而不是复制一套独立存储系统。逻辑上需要以下字段，但不要求复制Legacy `row[-1]` 的具体负偏移ABI：

```text
[null bits]
[key cells]
[dependent cells]
[optional normalized-key metadata]
[runtime flags: free/probed]
[optional duplicate next metadata]
```

`normalizedKey()/next()` 必须通过row-store accessor访问，HashTable不再假定 `row[-1]` 或固定正偏移。这样Bm layout可以把normalized key放在row内正偏移或sidecar，并保持RowId/row stride简单。

首版明确选择把runtime metadata和用户cells放在同一个row block，以保持duplicate-chain和probed访问的局部性并避免逐row虚调用。每次restore后执行：

```text
next = nullptr
probed = false
free = false
normalizedKey = recompute or reset
```

如果这些字段位于spill-backed row block中，必须在reload-clean block的第一次重建或Probe写入之前调用 `BufferManager::MarkDirty()`；不能先修改再标dirty，否则中途异常可能让已修改block仍被当作clean discard。正确性优先于write amplification。

transient sidecar只作为后续性能优化：sidecar不参与BM持久化，每次HashTable rebuild重新创建，可减少row-block dirty writeback，但会增加row到metadata的寻址和随机访问。只有benchmark证明收益后才替换row-inline方案。

### 6.3 Runtime metadata 的 restore 规则

| 字段 | spill backing中的值 | restore动作 |
|---|---|---|
| keys/payload | 有效 | 保留 |
| null bits | 有效 | 保留 |
| StringView pointer | 地址失效 | rebase |
| duplicate next | 地址失效 | row内方案清零后重建；sidecar方案重建sidecar |
| probed flag | 上一round状态 | row内方案清零；sidecar方案重建sidecar |
| free flag | 不允许跨round复用 | 清零/校验 |
| normalized key | 可能有效 | 首版通过storage accessor重算 |
| bucket table | 不持久化 | 完整重建 |

### 6.4 HashTable row-store contract

当前HashTable静态依赖 `RowContainer`。建议抽取HashJoin实际需要的窄contract：

```text
allocateRow / appendRow
storeKey / storeDependent
equalsDecoded
compareRows
hashRows
normalizedKey get/set
next get/set/reset
setProbed / extractProbed
listAll / listProbed / listNotProbed / listNullKey
extractColumn / extractNulls
numRows / allocatedBytes / sizeIncrement
beginRoundLease / endRoundLease
```

性能上不建议在逐key比较热路径引入通用虚调用。两个可行实现方向：

1. 把现有 `HashTable<ignoreNullKeys>` 扩展成 `HashTable<ignoreNullKeys, RowStore>`，Legacy RowContainer作为默认storage policy，BmJoinRowContainer作为第二个实例。
2. 保留 `BaseHashTable` batch级虚接口，新增 `BmHashTable<ignoreNullKeys>`，并把bucket/probe算法中的可复用部分抽成模板helper。

推荐先做一个小型compile-time spike比较两种方向。若模板化不会大幅放大编译时间和改动面，优先storage policy；否则使用独立BmHashTable backend，但不得复制整个HashTable实现。

### 6.5 消除 HashProbe 对具体 RowContainer 的依赖

HashProbe当前会直接调用：

- `table_->rows()->extractColumn()`；
- `table_->rows()->setProbedFlag()`；
- `table_->rows()->extractProbedFlags()`。

这些操作应移动到 `BaseHashTable` 的storage-neutral batch接口。HashProbe只处理 `BaseHashTable` 返回的row handles，不判断底层是Legacy还是BM。

### 6.6 RoundLease：复用现有 pins

为BM backend定义显式RAII对象，但它不拥有第二套 `BufferHandle`：

```text
RoundLease
├── covered partition/segments
├── per-partition generation
├── owner execution token
├── authoritative current row pointer view
├── forbidden release-operation gate
└── release()
```

创建lease时：

1. 用admission guard覆盖reserve、BatchPin、rebase和HashTable build；预算包含partition reload bytes、HashTable和output headroom。
2. resident path直接检查所有covered row/heap `BlockRef.handle.valid()`；restore path由现有 `BatchPin()` 把pins安装到 `BlockRef.handle`。
3. lease不调用额外 `Pin()`；现有handles就是唯一physical pin authority。
4. StringView rebase只执行一次，并建立唯一的authoritative row pointer view。
5. 在首次修改reload-clean row block前MarkDirty，随后reset runtime metadata。
6. list current row pointers并rebuild HashTable。
7. 所有最终会清 `BlockRef.handle` 或销毁metadata的primitive都必须在同一coordinator state lock下检查covered partition没有active lease；不能只gate上层wrapper。
8. 在lease存活期间明确拒绝：finalize/seal-to-unpin、`unpinPartition()`、`spillOrDiscardPartition()`、release segment(s)/chunk、`popFrontRows()`、`ReadOnlyWindowReadSession::releaseLoadedChunks()/evictLoadedChunks()`、consuming merge release和container teardown。

销毁lease时：

1. 唯一的HashProbe完成本轮输出，显式invalidate所有缓存build `char*` 的round state。
2. Probe和Bridge依次reset当前table，最后一份table引用析构HashTable buckets和RoundStorageGuard。
3. RoundStorageGuard先撤销逻辑gate并清空authoritative pointer view；随后coordinator根据状态显式unpin/spill/direct-discard/release。
4. 只有coordinator明确选择unpin时才清该partition的 `BlockRef.handle/ptr`；无spillfinal round可直接随storage析构RAII释放。
5. storage递增current partition自己的generation；此后该partition才可release或进入下一状态，其他pending token不受影响。

额外双pin既没有必要，也不能防止 `releaseSegment()` 直接销毁metadata；真正的正确性边界是existing pin + logical exclusive gate。

## 7. 端到端状态机

### 7.1 无 spill 路径

```text
HashBuild addInput
  -> append to active BM segments, blocks remain pinned
  -> noMoreInput（single build pipeline无需合并其他table）
  -> collect resident row pointers
  -> build HashTable
  -> publish table（table强持有RoundStorageGuard和RoundLease）
  -> Probe
  -> single HashProbe finish
  -> invalidate round pointer state
  -> resetHashTable依次释放Probe和Bridge的table引用
  -> table与RoundStorageGuard共同析构
  -> revoke RoundLease；storage析构或coordinator释放existing pins
```

这条路径不需要任何pointer rebuild，是最适合首个MVP的验证路径。

### 7.2 Spill 路径

```text
Build pressure/reclaim
  -> stop or discard any transient index referencing target rows
  -> finalize partition segment
  -> unpin + BM spill
  -> old pointers invalid

Restore current partition
  -> reserve
  -> BatchPin all segment blocks
  -> rebase strings
  -> reset runtime metadata
  -> collect new pointers
  -> rebuild HashTable and duplicate chains
  -> publish resident RoundLease
```

### 7.3 多 partition Probe rounds

```text
Round N table + RoundLease
  -> Probe current inputs
  -> spill probe rows whose build partition is not resident
  -> single HashProbe finish round N
  -> prepareForSpillRestore清空round pointer state并reset probe table
  -> Bridge::probeFinished reset build result
  -> table析构并撤销round N lease；pending token继续持有join storage
  -> HashBuild restore partition N+1
  -> rebuild and publish round N+1
```

中间round走 `prepareForSpillRestore() -> probeFinished()`；最终round不调用 `probeFinished()`，而是走 `kFinish -> resetHashTable()`。两条路径都必须在最后一份table引用消失前先invalidate本轮缓存的build row地址。

## 8. P0 Blocker

生命周期模型可以对齐，但以下工程问题仍需在接入前解决。

### 8.1 Gluten serial 执行契约与 arbitration hook

Gluten默认路径已经提供本文需要的串行调度：

- `WholeStageResultIterator` 在 `spark.gluten.parallel.enabled=false` 时创建无driver executor的QueryCtx；
- Task使用 `ExecutionMode::kSerial` 和ungrouped PlanFragment；
- `Task::next()` 在调用线程逐个运行所有drivers，Probe可以先被轮询但只会等待Bridge，真正Probe一定发生在Build发布table之后；
- spill rounds按 `build -> probe -> probeFinished -> build next partition` 严格交替。

因此本文不要求把BufferManager全面改造成并发容器，但不能只给 `WholeStageResultIterator` 的普通方法加一把锁：该类没有 `close()`；JNI `nativeClose()` 只是从 `ObjectStore` 删除handle，而 `nativeHasNext/nativeNext/nativeSpill` 会先各自取得 `shared_ptr<ResultIterator>`，最后一次析构可能发生在任一调用退出的线程。Gluten/Bolt需要为 `ResultIterator` 和底层 `WholeStageResultIterator` 增加共同的 `IteratorLifetimeGate`：

1. `hasNext()/next()/spillFixedSize()` 先取得active-call token；gate处于closing后拒绝新token。token覆盖完整JNI调用并保证iterator存活，但不等于一直持有execution guard。
2. 同一可重入execution guard串行每次 `task_->next()`、spill和operator-facing BM step；允许同一个Spark task在不同时间由不同OS thread调用，但任一时刻只有一个step owner。`nextInternal()` 遇到Bolt `ContinueFuture` 时必须先释放guard再 `wait()`，future ready后重新取得guard并重验closing/task状态；任何future或I/O等待都不能持guard。
3. `nativeClose()` 使用新增的 `ObjectStore::takeForClose<ResultIterator>(handle)`，在同一个ObjectStore锁区间从map移除并返回唯一强引用；首次调用得到owner并成为唯一closer。同一live ObjectStore内resource ID已不存在时一律按幂等no-op返回，包括重复close；invalid store ID仍报错，不为区分“曾经close”和任意missing resource维护tombstone。唯一closer把gate迁移到closing，取得execution guard调用幂等 `requestCancel()`；取得cancel future后立即释放guard，在guard外等待已有active-call token归零和cancel future完成，最后重新取得guard执行唯一last-owner析构。不能先等active call归零再cancel，否则阻塞在future上的next可能永远无法退出。
4. concurrent JNI call即使在takeForClose前已retrieve到shared_ptr，也必须先从同一gate取得active token；closing后的token申请失败并不得触碰iterator内部状态。debug build分别记录active-call ownership和step owner token；BM backend commit后发现普通调用重叠、析构绕过gate或close超时都失败query。

`spark.gluten.parallel.enabled=true`、QueryCtx有executor、`Task::start()`并行路径在初始化gate阶段直接fallback Legacy。这里是“BM backend不选择并行配置”；Gluten当前parallel分支自身是否能正确运行是独立问题，不是本计划的前置假设。

即使是serial路径，BM leaf作为requestor在driver内触发memory arbitration时，当前reclaimer仍没有override：

```text
enterArbitration()
leaveArbitration()
```

当前driver可能仍被计为on-thread，而Task reclaim同步等待全Task pause，形成self-wait。serial execution并不会自动消除这个重入死锁。

修复原则：

- common BM层接收抽象arbitration delegate；
- exec adapter负责driver suspend/resume；
- 从Gluten serial `Task::next()`内的真实HashBuild driver触发arbitration做端到端测试。

### 8.2 CMake 分层

当前：

```text
bolt_exec_bm -> bolt_exec
```

当前并没有已经形成的链接环：`bolt_exec_bm` 依赖 `bolt_exec`，而 `bolt_exec` 尚未链接BM。HashBuild若开始引用BmRowContainer，就需要让 `bolt_exec` 反向链接当前target，从而形成环。接入前应把纯storage代码拆成不依赖 `bolt_exec` 的lower-level target：

```text
bolt_exec_bm_storage
  -> bolt_memory_bm + vector + type + common

bolt_exec
  -> bolt_exec_bm_storage

bm tests/benchmarks/adapters
  -> bolt_exec + bolt_exec_bm_storage
```

### 8.3 Gluten serial 下的阶段所有权

首版直接复用 `Task::maybeCreateBufferManager()` 创建的Task-level BufferManager，并限制每个Gluten native Task只有一个BM-enabled HashJoin。`BmHashJoinStorage` 持有该manager的shared_ptr，在execution guard内采用阶段所有权：

1. build阶段只有HashBuild可append、seal、spill和restore；普通operator路径持active-call token和execution guard，并只短持storage mutex做coordinator状态迁移；
2. publish后，Bridge和HashProbe通过shared ownership承接storage，HashProbe只读current RoundLease覆盖的rows并修改受控runtime metadata；
3. HashBuild只有在 `probeFinished()` 撤销上一RoundLease后才能restore下一partition；
4. operator-facing Allocate/Pin/Unpin、BlockRef handle析构和container mutation在同一个execution guard中串行执行；需要访问coordinator时锁序只能是 `active-call token -> execution guard -> storage mutex`；
5. Gluten `spillFixedSize()` 必须取得同一guard后才能触发shrink/reclaim，不能和 `nativeNext()` 并发；
6. Bridge只在mutex内移动current `buildResult_`/table引用并完成状态转换，table、RoundStorageGuard和BufferHandle的析构在解锁后进入execution guard，随后才notify waiters。final `resetHashTable()` 采用相同规则。

Gluten execution guard只能串行operator/container入口，不能替代BM内部最小同步。Shared/global arbitrator可能在任意仲裁线程读取 `reclaimableBytes()`，并在memory-reclaim executor上执行victim reclaim；这些调用不经过 `WholeStageResultIterator`。Phase 0必须完成：

1. 把 `unpinnedResidentBytes` 做成atomic reclaimable snapshot，使候选扫描不读普通字段产生data race。
2. 给BufferManager的 `nextBlockId_`、BlockMemory state/pinCount/dirty/generation、stats和eviction queue增加独立state mutex；状态选择/转换和queue操作在锁内，压缩及I/O等待在锁外，完成时重新校验block generation。
3. BM reclaimer绝不获取Gluten execution guard；它只处理已经由coordinator解除handles的unpinned blocks。这样 `spillFixedSize()` 即使持execution guard同步等待shrink，也不会和off-thread BM reclaim互相等待。
4. composite candidate estimate是pause前的只读off-thread例外：coordinator在storage mutex内维护并release-store `sealableLogicalBytes` atomic snapshot；`reclaimableBytes()` 只acquire-load该值，不取storage mutex、不遍历partition catalog、不改变handles。snapshot允许保守过期，真正reclaim必须重验。
5. 外部composite HashBuild reclaim的mutation只有确认Task已pause、没有普通operator代码运行后，才可不取execution guard而短持storage mutex执行victim revalidate、seal状态迁移、generation更新、snapshot刷新并把待清handles move到局部变量；解锁后才析构handles、执行Unpin、BM reclaim、pool release或I/O。
6. storage mutex不与Bridge mutex或BM state mutex嵌套，不得在持有它时allocation、Pin/Unpin、压缩、等待I/O或发起arbitration。纯BM candidate scan/physical reclaim只使用BM state mutex，不接触container/coordinator metadata；普通路径始终先execution guard后storage mutex，off-thread路径永不反取execution guard。

这不是multi-driver支持，而是Gluten内存仲裁本身不可避免的跨线程边界。

Gluten目前没有把 `buffer-manager-enabled` 映射进Bolt QueryConfig；Phase 0需要补上映射，使Task在driver创建前生成唯一manager。HashBuild从 `DriverCtx::bufferManager()` 获取一次并放入 `BmHashJoinStorage`，HashProbe只通过published table/storage访问它。Task析构顺序已经先清drivers/split-group state、后reset manager，可作为最终兜底。

### 8.4 Probe round ownership 与释放

Gluten serial、non-morsel路径已经提供两个所需释放点：

- spill round：`prepareForSpillRestore()` 先reset `HashProbe::table_`，随后 `HashJoinBridge::probeFinished()` reset bridge中的build result；
- final round：不调用 `probeFinished()`；`HashProbe::resetHashTable()` reset probe和bridge持有的table。

正确性要求是：Bridge中的BM restore tokens跨round强持有 `BmHashJoinStorage`，当前table只强持有 `RoundStorageGuard(storage + RoundLease)`。中间round最后一份table引用析构只撤销current lease，不能释放剩余partitions。

现有 `prepareForSpillRestore()` 只显式reset table和last-probe iterator，并未清空 `HashLookup::hits`、`JoinResultIterator::nextHit` 和 `outputTableRows_`。BM路径应增加统一的 `resetRoundPointerState()`，在table引用消失前清空这些地址缓存；正确不变量是“旧pointer不再可达和不可解引用”，而不是假设现有代码已经销毁了所有缓存。

`JoinBridge::cancel()` 只标记cancelled并唤醒waiters，不会立即reset `buildResult_`。abort正确性必须由operator `close()`、Bridge/split-group teardown和storage RAII共同保证，并通过故障注入验证manager晚于所有BufferHandles析构。

现有 `HashJoinBridge::probeFinished()` 在持有bridge mutex时直接 `buildResult_.reset()`，可能让最后一份table引用及其pins在锁内析构。BM接入必须改成“锁内move、锁外destroy、再notify”；否则Unpin/coordinator或未来回调与Bridge锁形成锁序风险。

### 8.5 Append 异常安全

BmRowContainer batch append当前先推进row counters，再逐列store。生产HashBuild需要transaction/commit语义，或者明确把发生异常的container标记poisoned并立即失败query，确保永远不会继续读取半写rows。

首版建议先实现rollback：

- 保存segment/chunk/block cursor snapshot；
- store成功后commit；
- 失败恢复counters、used bytes、heap metadata和输出row pointers。

### 8.6 BM write failure

当前BM spill write失败路径没有把payload回滚到BlockMemory，block可能停在 `kSpilling`，不能安全Pin重试。HashJoin必须直接失败query并完成完整Task teardown，不能尝试在同一storage上继续。

### 8.7 OpenZL 压缩后端与默认切换

当前BM默认不是Zstd，而是 `CompressionConfig::kind = kLz4Block`。Zstd只在显式配置和历史benchmark case中使用。目标可以是让OpenZL成为新默认，但必须分成generic opt-in、format-aware opt-in和独立默认切换三个交付阶段。

第一步是codec接入：

1. 固定官方release `facebook/openzl@v0.2.0`（BSD license），不跟随无兼容保证的dev分支。
2. 增加 `CompressionKind::kOpenZl = 4`、`OpenZlOptions`、独立的 `ZL_CCtx`/`ZL_DCtx` context pools，并链接安装后的 `OpenZL::openzl`；关闭OpenZL tests/tools/CLI/examples/Python/introspection。
3. 首先使用standard serial/generic graph，适配现有 `std::span<const char>` block API；OpenZL是同步one-shot block API，适合BM block spill，但不是Zstd drop-in replacement。
4. 写入的Bolt outer header仍记录actual `compressionKind`。OpenZL frame自描述format/graph，standard graph不需要修改outer v1；后续若加入layout/dictionary ID，再引入可向后读取v1的outer header v2/TLV。
5. 写端允许 `OpenZL -> LZ4/raw` fallback并按 `minSavingsBytes/minSavingsRatio` 选择actual stored kind；读端严格按header解码，失败不能猜测其他codec。

第二步是真正format-aware：

1. 给BM block增加稳定的 `SpillEncodingDescriptor`：至少区分opaque heap block、fixed-width row block，并为row block携带row stride、field offsets/widths、null/runtime metadata范围及layout version。
2. opaque/heap blocks使用OpenZL serial graph。fixed-width row block的首个format-aware实现把整个block作为serial input交给 `ZL_Compressor_registerSplitByStructGraph(fieldSizes, successors)`，由它按每条record中背靠背的field widths拆成streams；不要先用 `ZL_TypedRef_createStruct()` 包装后再直接传给该graph，因为split-by-struct入口接受serial而不是Struct。
3. `ZL_TypedRef_createStruct(start, rowStride, rowCount)` 只声明等宽record的Struct typed input，并不会拆列；只有选择明确接受Struct类型的transpose/split graph时才使用它。split-by-struct输出若接numeric-only的bitpack/FieldLZ successor，还必须显式加入serial/struct-to-numeric转换。
4. `split-by-struct` 只接受背靠背field widths；Bm row layout若存在padding、非连续offset或不希望持久化runtime pointer metadata，必须先定义无歧义的spill layout/重排步骤，不能只传offset表假装兼容。
5. graph由代码和versioned layout registry构建，不把OpenZL目前不稳定的serialized compressor当作持久配置；首版禁用external dictionary和custom codec。
6. OpenZL typed/format-aware路径必须有保守output bound或destination-too-small重试；官方 `ZL_compressBound()` 只保证default single-serial pipeline。

只有以下门槛全部满足才把BM默认从LZ4切到OpenZL：round-trip/fuzz/ASAN/UBSAN通过；固定release的跨版本golden corpus可读；在Gluten真实HashJoin/Sort/Agg代表性数据上，physical spill bytes相对当前默认LZ4至少降低10%，query wall相对LZ4回退不超过2%，reclaim/pin p99相对LZ4回退不超过5%，peak scratch和process RSS相对LZ4均在预先登记的预算内；x86_64和ARM必须分别满足全部门槛。Zstd-3保留为附加对照，并可另设“physical bytes相对Zstd-3至少降低5%”的更强目标，但任何Zstd结果都不能替代LZ4 gate。未达标前OpenZL保持opt-in。

## 9. 分阶段实施计划

### Phase 0：Gluten serial gate、构建与仲裁

工作项：

1. 在Gluten增加 `spark.gluten.sql.columnar.backend.bolt.bmHashJoin.enabled`（同时补 `BoltConfig.h` 和Scala config registry），并映射到Bolt QueryConfig `bm-hash-join-enabled`；默认false。
2. gate必须同时验证 `spark.gluten.parallel.enabled=false`、Task `kSerial`、QueryCtx无executor、ungrouped、non-morsel、`multi_driver=false`。
3. 在Gluten `ResultIterator` 与 `WholeStageResultIterator` 之间增加共享 `IteratorLifetimeGate`：普通JNI调用竞争覆盖完整调用的独占active-call token；每个Task/operator/BM step另行取得同一可重入execution guard，blocking wait前释放。新增ObjectStore原子 `takeForClose()` 在移除handle时把强引用交给唯一closer；closer短持guard发出幂等 `requestCancel()`，再在guard外drain active call/cancel future，最后重新进guard销毁唯一owner。同一live store中的missing resource按幂等close返回，不等待或共享首个close result；invalid store ID仍报错。
4. 拆出 `bolt_exec_bm_storage` target，并增加独立build/link smoke target证明其不依赖 `bolt_exec`。
5. 为BM reclaimer增加exec-aware arbitration hooks，确保serial driver内发起arbitration时正确enter/leave suspended。
6. 把Gluten feature flag同时映射到 `buffer-manager-enabled=true`，复用driver创建前已有的Task-level BufferManager；限制每个native Task只有一个BM-enabled HashJoin。
7. 实现BM最小线程安全边界：atomic reclaimable snapshot、独立state mutex、带generation的锁外I/O completion；BM reclaimer不得获取Gluten guard。
8. 在读取第一批build input前完成capability判定；此时失败可选Legacy。记录BM backend committed状态，首个BM row写入后任何并发/lifetime guard违规都直接失败query。
9. 增加Gluten JNI/iterator真实路径的arbitration、spill callback、普通hasNext/next/spill token重叠拒绝、`nativeClose` 与blocked-next/spill race、重复close幂等no-op、ObjectStore take/retrieve竞争、requestCancel、abort和last-owner teardown测试。

退出条件：

- BM leaf触发arbitration时driver正确suspend/resume；
- 不发生same-task self-wait；
- `IteratorLifetimeGate` 证明 `nativeHasNext/nativeNext/nativeSpill/nativeClose/requestCancel/last-owner destructor` 生命周期安全：普通调用只有一个独占token，blocking wait不持guard；`takeForClose()` 与retrieve/重复close竞争只选出一个closer，同一live store的后续missing resource幂等返回；唯一closer短持guard发cancel、释放guard后drain现有token、拒绝新token且恰好析构一次；
- global arbitration线程并发扫描reclaimable snapshot和执行BM reclaim时TSAN无data race、无guard自锁；
- 一个native Task出现第二个BM-enabled HashJoin时稳定fallback；
- manager晚于所有handles销毁；
- default flag关闭时legacy HashJoin无行为变化。

### Phase 0.5：OpenZL generic codec opt-in

工作项：

1. 以独立package固定 `facebook/openzl@v0.2.0` 和commit `3dceb64867840201fb8f57a29d179995f700c9b8`，纳入BSD notice；不直接 `add_subdirectory`，避免它内置的Zstd/LZ4 targets与Bolt依赖冲突。
2. 增加 `CompressionKind::kOpenZl = 4`、OpenZL options、encoder/decoder context pools和CompressionManager dispatch。
3. 实现standard serial/generic graph adapter、actual-kind header、OpenZL->LZ4/raw写端fallback、strict read dispatch。
4. 增加v1 none/LZ4/Zstd/Snappy/OpenZL golden records、corruption/unknown-kind、release-upgrade corpus及并发context-pool tests。
5. 增加独立package consumption smoke test；若v0.2.0导出的 `OpenZL::openzl` 无法解析其public `libzstd/lz4` targets，则维护dependency-provider patch映射到Bolt已有targets。
6. 以config opt-in跑block micro和SpillStore benchmark；本阶段不依赖BmJoin layout、不切默认codec。

退出条件：

- OpenZL v0.2.0 round-trip、fallback、旧codec读取和跨release corpus全部通过；
- installed-package configure/link smoke通过且不重复引入Zstd/LZ4；
- generic OpenZL数据有可复现实测；OpenZL仍保持opt-in。

### Phase 1：Join-capable BmRowContainer

工作项：

1. 增加 `BmJoinRowLayoutOptions`：keys、dependent columns、hasNext、hasProbedFlag、hasNormalizedKey。
2. 计算join metadata offsets。
3. 首版实现row-inline runtime metadata，并保证reload后在首次mutation前MarkDirty；sidecar不作为本阶段前置。
4. 实现 `next`、probed、normalized-key get/set/reset accessor，不暴露具体偏移ABI。
5. 实现row-vs-decoded equality和row hash。
6. 实现HashJoin所需list/extract接口。
7. 实现transactional selected append。
8. 把segment生命周期拆为seal、unpin、spill/discard，支持resident table publish和压力spill分别调用。
9. 实现logical `RoundLease`，复用现有 `BlockRef.handle` pins；`unpinPartition()`、`spillOrDiscardPartition()` 和所有旧release wrappers都必须调用同一lease-aware primitive。
10. 把partition metadata从固定256改为sparse结构，或首版明确限制。
11. 实现可回滚的BatchPin/admission guard，任何中途失败都释放部分pins并清空pointer view。

退出条件：

- resident fixed-width rows与Legacy RowContainer逐项行为对照一致；
- duplicate/probed/normalized metadata可reset和重建；
- selected append故障注入后状态完整回滚；
- sealed-resident segment可在不spill的情况下发布table，且不再接受append；
- 同一partition不能同时创建两个RoundLease；lease期间所有release API失败；lease结束后由coordinator显式决定继续resident、unpin/spill或release。

### Phase 2：HashTable row-store 适配

工作项：

1. 确定storage policy或BmHashTable backend方向。
2. 保持Legacy backend作为默认，不改变aggregation/sort调用者。
3. 抽象 `BaseHashTable::RowsIterator`，不再内嵌只能遍历Legacy的 `RowContainerIterator`。
4. 把HashProbe直接访问 `table_->rows()` 的操作移到BaseHashTable batch API。
5. 新增 `HashProbe::resetRoundPointerState()`，清空hits、join-result iterator、outputTableRows和last-probe iterator。
6. 把Bridge的current table release改为锁内move/状态转换、锁外destroy/notify。
7. 把HashBuild的append、row count、size estimation和backend构造移动到row-store边界。
8. 让BM backend显式提供bucket allocator使用的MemoryPool、column metadata、row listing/hash和allocated-bytes统计。
9. 保留 `prepareJoinTable()` 对同一concrete backend的约束，并让唯一的build table直接完成构建。
10. 为BM backend禁用JIT和非generic hash，直到对应支持完成。
11. 增加Legacy/BM HashTable对照测试。

退出条件：

- Legacy所有HashTable/HashJoin测试通过；
- Bm backend支持new/store/equals/hash/list/extract；
- 热路径没有意外的逐row虚调用；
- compile time和binary size变化有记录。

### Phase 3：Resident-only MVP

范围：

- Gluten Bolt `ExecutionMode::kSerial`；
- `spark.gluten.parallel.enabled=false`、QueryCtx无driver executor；
- single build/probe pipeline driver；
- ungrouped、non-morsel、`multi_driver=false`；
- inner join；
- fixed-width keys和payload；
- duplicate build keys和跨batch join output；
- generic hash；
- 不触发spill；
- 默认关闭。

数据流：

1. HashBuild直接写BmJoinRowContainer。
2. active blocks始终pinned。
3. noMoreInput时收集resident pointers。
4. 基于这些pointers构建HashTable。
5. Bridge发布table和RoundLease ownership。
6. Probe完成后先resetRoundPointerState，再destroy table和撤销RoundLease。

退出条件：

- 与Legacy inner join结果完全一致；
- duplicate chains和多output batches与Legacy一致；
- bucket、hits和iterator pointer在整轮稳定；
- final-round release恰好执行一次；
- ASAN/TSAN无问题，Gluten execution guard无并发违规。

### Phase 4：Single-partition spill/reload

本阶段继续只覆盖fixed-width rows；StringView rebase在Phase 6启用VARCHAR/VARBINARY时接入。

增加强制spill测试路径：

1. 销毁当前transient HashTable。
2. finalize/unpin/spill BM segment。
3. 验证旧pointers不再使用。
4. BatchPin全部segments。
5. 刷新row-block pointers。
6. 首次写前MarkDirty并reset next/probed/normalized metadata。
7. 取得新pointers并重建HashTable。
8. Probe round保持epoch pinned。

退出条件：

- restore前后row地址允许不同；
- 新HashTable不包含任何旧地址；
- duplicate chains只引用新epoch pointers；
- second spill/reload仍正确；
- read/write failure安全失败query。

### Phase 5：Gluten serial partitioned spill 与串行多轮 Probe

本阶段仍限定fixed-width rows、generic hash和inner join。

工作项：

1. 使用现有HashBitRange计算build partition。
2. 实现partitioned selected append。
3. 一个partition可以产生多个segments。
4. 增加serial `BmHashBuildSpillCoordinator`，统一替代BM backend上的旧build `Spiller` 控制面。
5. 拆分现有 `finalizeAndFlushSegment()`，提供 `sealPartition()`、`unpinPartition()` 和 `spillOrDiscardPartition()`；当前实现把三步绑定在一次同步spill中。
6. coordinator接入 `ensureInputFits()`：选择victim partition、seal其active segment并释放pins。
7. build-running阶段为HashBuild pool安装专用 `BmHashBuildCompositeReclaimer`，而不是继续使用只观察operator leaf的通用reclaimer。它override `reclaimableBytes()`：在Task pause前只以acquire语义读取coordinator维护的atomic `sealableLogicalBytes`，保守公布“当前可seal且未被RoundLease覆盖的victim logical bytes”，使HashBuild pool在通用aggregate candidate快照时就能进入候选集；estimate不取storage mutex、不遍历catalog，也不得通过临时unpin制造候选。
8. composite `reclaim()` 在Task暂停后短持storage mutex选择并把victim迁移到sealed/unpinning generation，解锁后清handles；随后调用一个从 `BufferManagerReclaimer` 抽出的共享BM leaf原语 `reclaimAndReleaseReservation(target, statsOwner)`。该原语在BM leaf上执行 `manager->Reclaim(target)` 和 `bmPool->release()`，用BM leaf `reservedBytes` 前后差作为唯一框架返回值；manager报告的physical payload bytes只做诊断指标。
9. 不能依赖通用aggregate reclaimer在HashBuild返回后再选择BM leaf：它在调用child前已快照候选，当时BM blocks仍pinned、BM leaf为0，不会在unpin后重扫。composite调用共享原语时必须抑制/协调普通BM leaf对同一victim的统计所有权，确保一次reclaim只由composite或普通BM reclaimer中的一个路径调用 `MemoryReclaimer::run()`、累加stats和返回reservation delta；root capacity仍由外层 `ArbitrationParticipant::reclaim()` 在pool reclaim后按实际可释放reservation执行shrink。
10. published/`kWaitForProbe`阶段，HashBuild本身是non-reclaimable；此前已经unpinned的inactive partitions可由Task-level BM leaf独立spill/discard，RoundLease覆盖的current partition始终不可选。
11. 不能只在现有 `void HashBuild::reclaim()` callback内调用manager：普通 `Operator::MemoryReclaimer` 只用 `ScopedReclaimedBytesRecorder` 观察HashBuild leaf，sibling BM leaf下降会被记成0。新增composite reclaimer必须直接返回共享BM leaf原语测得的reservation delta。
12. coordinator用victim state/generation防止composite reclaim与off-thread BM leaf重复选择；分别记录logical candidate bytes、physical payload reclaimed bytes、BM leaf reservation delta和最终root capacity shrink，四者不能混称或重复累计。
13. coordinator接入 `finishHashBuild()`：确定首个resident partition，向Bridge发布其他BM restore tokens。
14. 唯一的HashBuild按partition ID逐个restore；当前partition全量pin，其他partitions保持unpinned BM backing。
15. HashProbe继续使用现有probe-side Spiller保存对应probe rows。
16. Bridge使用tagged restore source：`NoMoreInput | DiskSpillShard | BmRestoreToken`；token包含logical `SpillPartitionId`、storage强引用、partition locator和per-partition generation。
17. Bridge在serial guard内validate-and-consume token；stale generation和重复领取均报错。
18. 唯一的HashProbe结束本轮后，先destroy table，再撤销RoundLease；随后同一个HashBuild restore下一partition。

partition identity必须与现有 `SpillPartitionId {bitOffset, partitionNumber}` 完全一致。

首版关闭：

- range skew/subrange；
- adaptive bit range变化；
- recursive repartition；
- grouped/morsel；
- reusable table（本文始终fallback到Legacy）。

### Phase 6：Join 语义扩展

本阶段始终保持一个HashBuild driver和一个HashProbe driver，不增加driver并行度。

建议顺序：

1. VARCHAR/VARBINARY与StringView rebase；
2. build projections；
3. join filter；
4. left join；
5. right/full及probed flag；
6. semi/anti；
7. null-aware filter和null-key full scan；
8. array/normalized hash mode；
9. dynamic filter。

每扩展一种语义都要增加spill前、spill后restore和二次restore三组对照。

### Phase 7：Gluten serial recursive repartition 与优化

若当前partition无法整体resident：

1. 由唯一的HashBuild用更深HashBitRange流式重分区；
2. 写入新的BM child partition segments；
3. 更新 `offsetToJoinBits`；
4. Probe使用相同bit range；
5. commit后release父partition。

后续性能优化：

- 优化runtime metadata sidecar，避免固定宽row blocks因next/probed reset反复dirty；
- prefetch和BatchPin深度控制；
- current-partition headroom estimation；
- normalized-key持久化复用；
- clean backing direct discard。

### Phase 8：OpenZL fixed-row format-aware opt-in

本阶段依赖Phase 4/5已经提供真实fixed-width BM row blocks、spill/reload和可重复的Gluten serial HashJoin benchmark；不能在只有synthetic block输入时提前决定row profile。

工作项：

1. 为BM spill block增加versioned `SpillEncodingDescriptor`，明确opaque/heap与fixed-row，并记录row stride、连续field widths、null区域、runtime metadata区域和layout ID。
2. 先让fixed row block作为serial input进入split-by-struct；对不连续layout先生成明确的canonical spill layout，不把TypedRef Struct直接接到serial-only graph。
3. 为每个field选择保守successor，并在numeric-only successor前加入显式类型转换；unknown descriptor/profile一律fallback到OpenZL generic、LZ4或raw，并把actual kind/profile写入header。
4. 引入outer header v2/TLV保存layout/profile ID，同时保持v1旧records可读；graph由versioned registry重建，不持久化不稳定serialized compressor对象。
5. 增加destination-too-small重试、descriptor fuzz/golden corpus、跨release读取及unknown-layout fallback测试。
6. 在HashJoin、Sort、Agg真实spill数据上同时跑OpenZL generic和format-aware；本阶段仍只允许显式opt-in，不修改BM默认。

退出条件：

- format-aware profile对所有支持layout可round-trip，未知layout稳定fallback；
- v1和v2 records跨release读取通过；
- x86_64与ARM都有可复现的bytes、CPU、p99和RSS结果；
- 无论结果好坏，默认仍保持LZ4，进入Phase 9才作切换决策。

### Phase 9：OpenZL 默认切换 gate

只有Phase 0.5、Phase 4/5、Phase 6涉及的代表性字符串路径、Phase 8以及§14的真实端到端benchmark全部完成后，才执行本阶段：

1. 冻结候选OpenZL release、graph registry、fallback策略和benchmark corpus；禁止用新的调参结果替换未复测的平台数据。
2. 在x86_64和ARM上分别对当前默认LZ4执行§8.7全部强制门槛；Zstd-3只作附加对照或更强目标。
3. 通过后才把 `CompressionConfig::kind` 的默认值改为 `kOpenZl`，保留显式LZ4回滚配置、旧record读取和actual-kind metrics。
4. 任一workload/platform门槛未通过，则结束本阶段并保持OpenZL opt-in；不得以部分case或synthetic历史数据切默认。
5. 默认修改单独提交，运行全量BM/Gluten兼容性、升级/回滚和canary验证。

退出条件：

- 两个平台相对LZ4的bytes、wall、p99和RSS强制门槛全部通过；
- 默认配置、显式LZ4回滚、v1/v2旧record读取均通过；
- 若门槛未通过，交付结果是“保持LZ4默认并保留OpenZL opt-in”，不是失败地强行切换。

## 10. Bridge 与所有权协议

### 10.1 Join-lifetime owner 与 round guard

需要明确分离两种生命周期：

```text
BmHashJoinStorage                 // join lifetime
├── shared_ptr<BmJoinRowContainer> store
├── shared_ptr<BufferManager> manager
├── BmHashBuildSpillCoordinator coordinator
├── partition catalog / per-partition generations
└── release state

RoundStorageGuard                 // one Probe round
├── shared_ptr<BmHashJoinStorage> storage
├── RoundLease lease
├── SpillPartitionId currentPartition
└── per-partition generation

BmRestoreToken                    // pending round
├── shared_ptr<BmHashJoinStorage> storage
├── SpillPartitionId logicalId
├── partition locator
└── per-partition generation
```

- HashProbe和Bridge持有current table时，table强持有RoundStorageGuard。
- Bridge的pending restore collection持有BmRestoreTokens，从而让完整storage跨中间round存活。
- 中间round切换时，Probe先invalidate pointer state并reset table，Bridge再reset最后一份current table；RoundStorageGuard析构撤销current RoundLease，coordinator随后决定unpin/spill/release。
- final round由现有serial `resetHashTable()` resetprobe和bridge两份table引用；此时若无pending token，最后一个storage owner随table/bridge teardown释放。
- final build可能先于final probe结束，因此storage不能只由HashBuild operator字段持有。

现有Bridge的round rendezvous、promises、cancelled、build result、restored partition identity仍需保留。BM只把pending source和restore input扩展成backend-neutral tagged token；single builder领取整个BmRestoreToken，不执行 `SpillPartition::split()`。

token消费协议在Gluten execution guard内串行执行：只有catalog中 `pending` 且generation相等的partition可从 `pending -> restoring`；publish成功后变为 `resident/published`，round结束后变为 `consumed` 并递增该partition generation。任何旧token或重复consume都失败。

### 10.2 Release 顺序

正常路径：

```text
single HashProbe finish or switch round
  -> resetRoundPointerState
  -> reset HashProbe::table_
  -> Bridge锁内move current table并更新round状态，然后解锁
  -> 锁外destroy HashTable buckets
  -> destroy RoundStorageGuard and revoke RoundLease
  -> coordinator显式unpin/spill/release consumed partition
  -> pending token继续持有storage
  -> intermediate round锁外notify唯一HashBuild
```

异常路径：

```text
Task cancel/error
  -> Bridge mark cancelled and wake waiters
  -> Task停止driver并调用HashBuild/HashProbe close
  -> operator、bridge和split-group teardown释放table与restore tokens
  -> RoundStorageGuard撤销RoundLease
  -> 最后一个BmHashJoinStorage owner销毁store
  -> 所有BufferHandles销毁后再销毁manager
```

`cancel()` 本身不销毁build result；上述完整teardown顺序必须由故障注入验证。所有 `release()` 必须幂等。

## 11. 内存与 Reclaim 策略

### 11.1 Build 阶段

- active segments保持pinned，不可被BM自动reclaim；
- sealed/unpinned partitions是BM reclaim候选；
- 若存在transient HashTable引用某partition，spill前必须先销毁该index；
- append和metadata mutation期间进入non-reclaimable section。
- build-running阶段由专用composite reclaimer的 `reclaimableBytes()` 先公布coordinator维护的可seal logical-byte snapshot；实际reclaim再做seal/unpin并调用共享BM leaf `Reclaim + pool release` 原语，向框架返回BM leaf reservation delta，而不是physical payload bytes或HashBuild leaf recorder差值。
- 普通container/coordinator状态变更发生在Gluten execution guard内；Task-pause后的composite状态迁移是只取storage mutex的明文例外；纯BM leaf candidate scan/reclaim依赖§8.3的atomic snapshot和state mutex。

### 11.2 Probe 阶段

- current partition所有row/heap blocks保持pinned；
- current partition不应报告为reclaimable；
- 其他未驻留partitions可以被BM spill/discard；
- 若内存不足以同时容纳current partition、HashTable和output headroom，应在publish前失败或继续repartition，不能在Probe中途unpin。
- RoundLease期间现有 `BlockRef.handle` 持续pin current partition；BM reclaim自然跳过它。
- 中间round结束先撤销RoundLease，再由coordinator显式清handles或release partition；RoundLease本身不拥有/释放第二套pins。
- published/`kWaitForProbe`阶段HashBuild不参与reclaim；BM leaf只处理inactive unpinned partitions。

### 11.3 Restore admission budget

预算至少包含：

- current partition全部unloaded BM bytes；
- HashTable bucket allocation；
- duplicate/runtime metadata；
- StringView rebase可能产生的dirty writeback；
- output/filter batch headroom；
- 一个read/decompression buffer window；
- allocator和metadata余量。

`MaybeReserve()` 只是瞬时探测。实现需要一个覆盖“reserve -> partial BatchPin -> rebase/dirty -> HashTable build -> publish”的admission guard，并定义partial failure rollback；不能在load完成后立即释放全部reservation而假定后续HashTable allocation必然成功。

### 11.4 Gluten serial pool 与 reclaim 路由

首版保持当前pool拓扑，不新建per-join manager：

```text
Task aggregate
├── task-bm-* leaf                    // existing Task-level BufferManager
└── HashJoin node aggregate
    ├── HashBuild operator leaf
    └── HashProbe operator leaf
```

这一选择依赖“每个native Task只允许一个BM-enabled HashJoin”。Build-running时，专用composite reclaimer先在 `reclaimableBytes()` 公布可seal logical victims，使HashBuild pool进入aggregate候选；Task pause后的 `reclaim()` 请求coordinator seal并清除victim handles，然后在同一次调用中执行共享BM leaf `manager->Reclaim(target) + bmPool->release()` 原语。不能等通用aggregate reclaimer重扫BM leaf，因为candidate list已在unpin前快照，也不能用HashBuild leaf的currentBytes下降推断sibling BM释放量。published/`kWaitForProbe`时HashBuild不可回收，但Task-level BM leaf仍可回收早已unpinned的inactive partitions，RoundLease覆盖的current partition因pinCount非零自然跳过。

logical unpin不计作框架reclaimed bytes；BM `Reclaim()` 返回的physical payload bytes用于诊断spill/discard效果，BM leaf `reservedBytes` delta才是composite reclaimer返回值和 `MemoryReclaimer::Stats::reclaimedBytes` 的口径，外层participant随后根据实际可释放reservation shrink root capacity。共享原语必须指定唯一stats owner，防止composite和普通BM leaf对同一次操作各调用一次 `MemoryReclaimer::run()`。若未来要在同一Task启用多个BM算子，再引入plan-node/tag级victim policy或per-node manager；该并发通用化不在本文范围。

## 12. Capability 与 Fallback

首个MVP只在第一批build input前满足全部条件时启用：

- 来自Gluten Bolt backend；
- `spark.gluten.parallel.enabled == false`；
- Task `ExecutionMode::kSerial` 且 `queryCtx()->isExecutorSupplied() == false`；
- `HashBuild` pipeline和 `HashProbe` pipeline各恰好一个driver；
- `splitGroupId == kUngroupedGroupId`；
- `morselDrivenEnabled() == false`；
- `multi_driver == false`；
- Gluten `nativeHasNext/nativeNext/nativeSpill/nativeClose/requestCancel/last-owner teardown` 受同一个 `IteratorLifetimeGate` 协调生命周期；其中实际Task/operator/BM step由同一个execution guard串行化，blocking wait不持guard；
- inner join；
- fixed-width key/payload；
- generic hash；
- `hybrid_join_enabled=false`、无reusable table；
- 无reuse、range skew和recursive restore。

否则在第一批build input前稳定fallback到Legacy。

本文不支持Gluten parallel分支。当前该分支硬编码 `maxDrivers=2`，且会向QueryCtx提供共享driver executor；即使某条pipeline最终只有一个driver，也不满足本计划的串行BM契约。

reusable/OpaqueHashTable会绕过本次BM build和join-lifetime owner，并可能跨query存活，因此在本文所有phase中都保持Legacy fallback，不列为后续语义扩展。

一旦已经写入BM，不能因为后续admission失败而静默切回Legacy。必须：

- 在本文已经实现的BM-native partition/repartition能力范围内继续BM；或
- `requestCancel()` 并明确失败query。

本文不设计也不允许BM-to-Legacy replay；它会重新引入跨backend数据转换、事务回滚和另一套pointer epoch协议，并超出“只对比Legacy RowContainer row-based spill、BM直接承载完整build rows”的范围。

同理，BM backend commit后若 `IteratorLifetimeGate` 检测到两个普通 `nativeHasNext/nativeNext/nativeSpill` 调用竞争同一独占token、parallel执行或绕过gate的teardown，这是运行时契约违规，必须requestCancel并失败query；不能用Legacy fallback掩盖已产生的BM状态。按§8.1协议执行的 `nativeClose` 与active call重叠是合法的cancel/drain路径，不属于违规。

## 13. 测试计划

### 13.1 BmJoinRowContainer 单测

- join layout offsets；
- fixed/string/null store和extract；
- next/probed/normalized metadata；
- row内metadata mutation正确MarkDirty，或sidecar完全不污染BM backing；
- metadata reset；
- row-vs-decoded equality；
- hash与Legacy结果一致；
- listAll/listProbed/listNotProbed/listNullKey；
- transactional selected append；
- Nth row-block/heap allocation failure rollback；
- StringView reload rebase；
- two consecutive spill/reload cycles；
- old pointer generation检测；
- per-partition generation相互独立；
- stale/replayed restore token拒绝；
- RoundLease期间分别直接调用finalize、`releaseSegment()`、`releaseSegments()`、`releaseChunk()`、`popFrontRows()`、`ReadOnlyWindowReadSession::releaseLoadedChunks()`、`evictLoadedChunks()`、consuming-merge release、新增 `unpinPartition()`、`spillOrDiscardPartition()` 和container teardown；每条路径都必须在统一lease-aware primitive处失败，并逐项断言原 `BlockRef.handle` 仍valid、BM `pinCount`不变、`BlockRef.ptr`与可读row地址不变；
- 撤销RoundLease前container teardown被拒绝；撤销后先invalidate pointer view，再由coordinator清handles，最后销毁segment/container，验证ownership析构顺序和幂等release；
- BulkReadSession析构不unpin，lease撤销后coordinator显式unpin。

### 13.2 HashTable backend 对照

同一input分别建立Legacy和BM backend，比较：

- generic hash hits；
- duplicate chains；
- listJoinResults跨batch；
- null-key handling；
- probed/not-probed scan；
- erase如果BM backend最终需要支持；
- clear/rebuild；
- normalized/array mode后续对照。

### 13.3 HashJoin 端到端

- resident inner join；
- forced spill/reload；
- restore后地址变化；
- second spill/reload；
- empty build/probe；
- duplicate keys；
- filters和projections；
- left/right/full/semi/anti/null-aware；
- final round table/RoundLease共同所有权与及时release；
- intermediate round只撤销current RoundLease、不释放remaining BM partitions；
- round切换前hits/results/outputTableRows全部失效化；
- Bridge锁内move、锁外table/RoundLease析构，析构回调重入不死锁；
- BmRestoreToken validate-and-consume只成功一次，旧generation失败；
- early finish；
- cancellation和spill IO failure；
- exact build/probe partition matching；
- oversize partition early failure。

### 13.4 Reclaim 与生命周期

- 真实HashBuild driver从BM leaf触发local arbitration；
- global shrink选择BM victim；
- current driver suspend/resume；
- same-task self-victim不超时；
- RoundLease覆盖的current Probe blocks不被reclaim；
- inactive partitions仍可reclaim；
- `IteratorLifetimeGate` 覆盖hasNext/next/spill/closing/requestCancel/last-owner teardown；测试普通next-vs-spill token重叠拒绝、close-vs-blocked-next、close-vs-spill、重复close幂等no-op和ObjectStore take-vs-retrieve race，断言next在future wait期间不持execution guard，唯一closer短持guard发cancel、随后释放guard并drain现有token、拒绝新token且只析构一次；同一live store的missing resource幂等返回、invalid store ID报错；BM commit后的违规失败query，commit前capability失败才走Legacy；
- off-thread/global BM candidate scan与reclaim不获取Gluten guard，atomic snapshot/state mutex下无data race；
- 普通next/close、local arbitration和global composite reclaim交错时验证固定锁序：普通路径execution guard后短持storage mutex，Task-pause composite只短持storage mutex，纯BM路径只取BM state mutex；无反向取guard、无Bridge/storage/BM锁嵌套死锁；
- composite `reclaimableBytes()` 在victim仍pinned时公布可seal logical bytes并确保HashBuild pool进入aggregate候选；不可seal或有RoundLease时报告0；
- candidate scan与普通next并发更新victim资格时，atomic `sealableLogicalBytes` acquire/release snapshot在TSAN下无data race、无需Task pause且不获取storage/execution guard；真正reclaim在pause后重验，保守过期estimate只允许少回收或返回0；
- build-running一次composite reclaim完成seal/unpin+共享BM leaf `Reclaim + pool release`；分别断言physical payload bytes、BM leaf reservation delta、root capacity shrink，框架返回值等于reservation delta；published阶段Task BM leaf独立回收；普通BM/composite两个stats owner不重复计数；
- manager由join-lifetime storage持有，晚于所有round guards和BufferHandles析构；
- ASAN；
- TSAN；
- JoinFuzzer Legacy/BM capability矩阵。

### 13.5 OpenZL

- v0.2.0 serial/generic/struct graph round-trip；
- 4 KiB到16 MiB、不可压缩/低基数/递增整数/fixed join rows/string heap blocks；
- output-bound、OpenZL error、savings不足时的actual-kind fallback；
- header指定OpenZL但payload损坏时严格失败，不猜LZ4/raw；
- v1旧codec golden records和OpenZL跨release corpus；
- per-call独占CCtx/DCtx pool、stress/TSAN；
- row layout descriptor version、unknown layout fallback和字段重建一致性。

## 14. Benchmark 计划

### 14.1 对照路径

1. Legacy HashJoin + RowContainer row-based Spiller；
2. BM full-row resident；
3. BM full-row spill/reload；
4. 可选 HybridContainer/BM payload实验。

每条BM路径再固定比较 `raw / LZ4（当前默认）/ Zstd-3 / OpenZL serial / OpenZL format-aware`。OpenZL结果与历史表格分开报告，不补算或推断。

### 14.2 数据集

- key-heavy；
- payload-heavy；
- duplicate-heavy；
- fixed-width；
- nullable；
- short/long VARCHAR；
- Gluten serial、固定一个HashBuild driver和一个HashProbe driver；
- resident；
- forced spill；
- recursive partition pressure。

### 14.3 指标

- append/store CPU；
- Legacy row serde / BM block preparation CPU；
- spill compression CPU；
- spill write bytes；
- restore read/decompression CPU；
- StringView rebase CPU；
- pointer/metadata rebuild CPU；
- prepareJoinTable CPU；
- probe CPU；
- task/BM pool peak；
- process RSS peak；
- restore peak；
- p50/p95/p99；
- spill file count/size；
- repeated-round write amplification；
- codec/profile/layout ID及fallback reason；
- raw/stored/header bytes、compression/decompression p50/p95/p99；
- OpenZL context/graph scratch与峰值RSS。

关键性能问题不是“能否重建pointer”，而是：

1. BM原blocks读回、rebase并重建index，是否比Legacy row-based reader + `copySerializedRow()` + index rebuild更快；
2. current partition全量pin + HashTable的峰值是否可接受；
3. runtime metadata reset是否导致过多dirty writeback；
4. Gluten serial下BM IO调度、compression和HashTable rebuild的组合成本；
5. OpenZL serial是否已经优于LZ4/Zstd，以及format-aware struct graph能否进一步改善fixed join rows；
6. OpenZL收益是否满足§8.7的默认切换门槛。

## 15. 可选架构与不推荐路径

### 15.1 HybridContainer-first

可以先让BM只保存dependent payload，旧RowContainer继续保存keys和join metadata。优点是改动小，适合验证BM arbitration、locator和payload extraction。

但如果最终目标明确是让BmRowContainer承载完整HashJoin rows，这条路线不是必要前置，可能形成两套长期storage path。建议把它作为可选spike或payload-heavy性能实验，而不是主实施计划。

### 15.2 HashTable 随机保存 RowId

未来可以让bucket保存stable `RowId`，按chunk批量load。但这会把Probe改造成可能触发IO的随机访问状态机，并需要异步blocked协议。当前不建议；现阶段应坚持“current partition整体resident，HashTable继续保存char*”。

### 15.3 把 BM pointer 当成跨 spill 永久地址

不推荐。正确做法不是维持旧地址，而是在新resident epoch取得新地址并重建所有含地址的metadata。

## 16. 历史 BM Window 集成经验

仓库历史中曾有完整BM Streaming Window集成：

- `b69944ac33`：引入实现；
- `f2ffeaceb7`、`ccb6b1d1ba`、`27433b8f9e`：优化resident read、缓存和批量release；
- `d091895177`、`8d1135d8af`：删除实现和残余接线。

提交历史没有说明删除原因，不能据此推断性能或正确性失败。可复用经验：

- Task/Driver获取BM的接线；
- 基础设施flag与算子flag双gate；
- append前reserve、mutation期间non-reclaimable；
- stable segment/range metadata；
- row/heap blocks一起pin；
- 已消费数据显式release；
- resident、spill和reclaim分别benchmark。

Window的FIFO消费、单partition ownership和无锁state不能直接照搬到HashJoin的多轮partition状态机。

## 17. 风险与置信度

| 结论 | 置信度 |
|---|---:|
| 两种容器的spill/restore pointer生命周期可对齐 | 0.99 |
| BmRowContainer可直接承载完整build rows | 0.95 |
| 当前代码不能机械替换类型 | 0.99 |
| 需要join row layout与HashTable storage适配 | 0.99 |
| current partition必须在整轮Probe中pinned | 0.99 |
| 现有BlockRef handles足以保持round内pins，额外pin不需要 | 0.99 |
| Gluten serial resident MVP可实施 | 0.96 |
| single-partition spill/reload可实施 | 0.92 |
| Gluten serial多partition串行round可实施 | 0.90 |
| Gluten serial recursive repartition可实施 | 0.86 |
| OpenZL可接入BM standard block codec | 0.95 |
| OpenZL format-aware fixed-row graph有收益 | 0.75，需Bolt数据benchmark |
| OpenZL可取代LZ4成为默认 | 未验证，必须通过门槛 |
| 性能收益 | 需要benchmark |

总体架构判断置信度约0.93。

## 18. 关键证据索引

以下行号以调查基线 `8d1135d8af` 为准：

| 事实 | 代码位置 |
|---|---|
| 旧build spill后clear table | `bolt/exec/HashBuild.cpp:852-864,1013-1040` |
| 单build/probe pipeline才可启用row-based HashJoin spill | `bolt/exec/HashBuild.cpp:335-365` |
| row-based Spiller执行逐row serde | `bolt/exec/Spiller.cpp:652-700; bolt/exec/SpillFile.cpp:470-518` |
| row-based reader返回serialized row pointers | `bolt/exec/SpillFile.cpp:650-700` |
| row-based restore复制到新RowContainer | `bolt/exec/HashBuild.cpp:511-532,1488-1499; bolt/exec/RowContainer.cpp:762-795` |
| 最终prepareJoinTable和publish | `bolt/exec/HashBuild.cpp:1087-1285` |
| RowContainer clear使旧pointer失效 | `bolt/exec/RowContainer.cpp:1047-1080` |
| 旧join row metadata布局 | `bolt/exec/RowContainer.cpp:199-341` |
| bucket存row pointer和normalized key | `bolt/exec/HashTable.cpp:369-381` |
| key compare直接调用RowContainer | `bolt/exec/HashTable.cpp:389-404` |
| duplicate chain写在row内 | `bolt/exec/HashTable.cpp:1257-1279` |
| join iterator跨batch保存nextHit | `bolt/exec/HashTable.cpp:1928-1993` |
| BmRowContainer返回resident row | `bolt/exec/bm/BmRowContainer.cpp:28-33` |
| Bm finalize/unpin/spill | `bolt/exec/bm/BmSegmentCollection.cpp:166-205` |
| Bm reload刷新block pointers | `bolt/exec/bm/BmRowBlockLoader.cpp:41-124` |
| Bm reload后列出新row pointers | `bolt/exec/bm/BmRowContainerReadLoad.cpp:94-103` |
| BlockRef.handle是resident pin owner，ptr依赖handle有效 | `bolt/exec/bm/BmSegmentTypes.h:43-50` |
| window release直接清handle和ptr | `bolt/exec/bm/BmReadOnlyWindowReadSession.cpp:136-174` |
| RowId稳定定位信息 | `bolt/exec/bm/BmRowContainerPublicTypes.h:48-57` |
| 当前Bm layout只有用户列 | `bolt/exec/bm/BmRowLayout.cpp:30-82` |
| BM reclaimer未覆盖enter/leave | `bolt/common/memory/bm/BufferManagerReclaimer.h:11-26` |
| BM leaf安装reclaimer | `bolt/common/memory/bm/BufferManager.cpp:44-52` |
| BufferManager::Reclaim只返回physical payload bytes，不release leaf reservation | `bolt/common/memory/bm/BufferManager.cpp:191-199` |
| BM leaf reclaimer执行Reclaim、pool release并按reservation delta记账 | `bolt/common/memory/bm/BufferManagerReclaimer.cpp:25-49` |
| aggregate reclaimer在调用child前一次性快照非零候选 | `bolt/common/memory/MemoryArbitrator.cpp:270-329` |
| participant在reclaim前查询reclaimable，reclaim后独立shrink root capacity | `bolt/common/memory/ArbitrationParticipant.cpp:179-182,290-320` |
| Task真正reclaim前才等待全Task pause | `bolt/exec/Task.cpp:3249-3277` |
| BM stats依赖reclaimer与API串行 | `bolt/common/memory/bm/BufferManagerStats.cpp:144-147` |
| 当前Task BM挂task pool | `bolt/exec/Task.cpp:461-490` |
| HashJoin node/operator pool拓扑 | `bolt/exec/Task.cpp:570-646` |
| Gluten创建serial ungrouped Task | `gluten/cpp/bolt/compute/WholeStageResultIterator.cc:192-252` |
| Gluten serial调用Task::next | `gluten/cpp/bolt/compute/WholeStageResultIterator.cc:478-509` |
| Task::next逐个串行运行drivers | `bolt/exec/Task.cpp:715-803` |
| Gluten parallel分支硬编码maxDrivers=2 | `gluten/cpp/bolt/compute/WholeStageResultIterator.cc:414-425` |
| Gluten nativeSpill可进入spillFixedSize | `gluten/cpp/core/jni/JniWrapper.cc:750-762` |
| native hasNext/next/spill各自retrieve ResultIterator shared_ptr | `gluten/cpp/core/jni/JniWrapper.cc:576-608,750-762` |
| nativeClose只从ObjectStore release handle | `gluten/cpp/core/jni/JniWrapper.cc:765-771; gluten/cpp/core/utils/ObjectStore.cc:77-80` |
| WholeStage没有close方法，析构时requestCancel | `gluten/cpp/bolt/compute/WholeStageResultIterator.h:49-71` |
| 当前BM默认codec为LZ4 | `bolt/common/memory/bm/compress/CompressionConfig.h:46-52` |
| BM压缩入口当前只有字节span | `bolt/common/memory/bm/compress/CompressionManager.cpp:67-117` |
| BM outer spill record v1和codec kind | `bolt/common/memory/bm/compress/SpillRecordHeader.h:11-22` |
| 单driver非morsel才主动reset table | `bolt/exec/HashProbe.cpp:1909-1920` |
| 中间round先reset Probe table再probeFinished | `bolt/exec/HashProbe.cpp:498-526` |
| Bridge legacy restore round | `bolt/exec/HashJoinBridge.cpp:135-188` |
| cancel只唤醒waiters而不reset build result | `bolt/exec/JoinBridge.cpp:45-52` |
| 当前CMake依赖bolt_exec_bm到bolt_exec | `bolt/exec/bm/CMakeLists.txt:26-31` |

## 19. 最终建议

按照以下顺序推进：

1. 先锁定Gluten serial gate和可重入execution guard，parallel路径一律fallback。
2. 修复BM arbitration hook并打通Gluten `spillFixedSize()` 的串行reclaim验证。
3. 接入OpenZL v0.2.0作为opt-in generic codec，先跑serial/generic baseline，但不在此时建设row-layout-aware graph或切默认。
4. 给BmRowContainer增加join layout、HashTable所需接口和不新增physical pin的RoundLease gate。
5. 做Gluten serial resident-only MVP，先证明完整build rows可以直接由BM承载。
6. 做single-partition spill/reload，验证地址变化、metadata reset和HashTable rebuild。
7. 接串行partition rounds和probe-side spill，并闭合composite reclaimer的候选、BM leaf reservation与root shrink口径。
8. 扩展join语义，最后做Gluten serial recursive repartition和性能优化。
9. 基于真实fixed-row spill建设OpenZL format-aware opt-in profile。
10. 所有真实benchmark完成后单独执行相对LZ4的默认切换gate；未全部达标就保持LZ4默认。

不要以“让旧pointer跨spill保持有效”为目标；应把每次restore视为新resident epoch，并完整重建所有含地址的状态。

也不必先把payload单独迁移到HybridContainer。若目标是完整BM HashJoin，直接建设BmJoinRowContainer和HashTable storage contract更符合最终形态。

Round内pointer稳定不需要重复pin：现有 `BlockRef.handle` 已经完成这件事。新增设计只负责阻止任何主动清handle或销毁metadata的操作。
