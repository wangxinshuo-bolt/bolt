# BmRowContainer 设计

## 目标

新增一个 `BmRowContainer`，用于 StreamingWindowBuild 场景的
BufferManager 化实验，同时不改变现有 `RowContainer` 的行为和使用方。

新容器的目标是把 window 输入的 row-format 中间数据放进 task-level
`BufferManager` 管理的 block 中，让定长 row block 和变长 heap block 都可以
unpin、reclaim 和 spill，并避免 StreamingWindowBuild 长期保存裸 `char*` 行指针。

第一版只面向 StreamingWindowBuild 类使用方式，不把它设计成 hash join、
aggregation、spilling、JIT 等路径里的通用 `RowContainer` 替代品。

## 现有约束

`RowContainer` 的操作模型建立在长期有效的 `char*` row 地址上。

几个关键假设：

- `RowContainer::newRow()` 返回 `char*`。
- Window build 代码把这些指针保存在 `inputRows_`、`sortedRows_` 和 partition
  range 中。
- `WindowPartition` 接收 `folly::Range<char**>`，并在 `extractColumn`、
  `extractNulls`、peer 比较和 frame 边界计算中反复解引用这些指针。
- `RowContainer` 把变长值存成 `StringView` 或 `std::string_view`，这些 view
  可能指向 `HashStringAllocator` 管理的内存。

`BufferManager` 的地址语义不同：

- `BlockHandle` 是稳定的 block 身份。
- `BufferHandle::Ptr()` 只在 block 被 pin 住期间有效。
- block unpin 并被 reclaim 后，同一个 block 再次 pin 回来时虚拟地址可能变化。

因此，BM-backed row container 不能把 `char*` 暴露成稳定行身份。否则只有两种选择：
要么所有 block 永久 pinned，BM 无法发挥 reclaim/spill 价值；要么旧 row 指针在
reclaim 后失效，后续解引用变成未定义行为。

## 非目标

第一版不支持：

- aggregate accumulator 存储；
- hash join `next` 链；
- probed flag；
- normalized key/JIT；
- `RowContainer::listRows(char**)` 兼容；
- row-based spiller 兼容；
- 多 segment combine 语义；
- DuckDB 风格的 `TupleDataChunkPart` 指针 rebasing。

如果 BM-backed row container 后续需要扩展到 StreamingWindowBuild 之外，再逐步补这些能力。

## 推荐 API 形态

`BmRowContainer::newRow()` 返回稳定的逻辑行引用，而不是物理指针。

```cpp
struct BmRowRef {
  uint32_t rowBlockId;
  uint32_t rowOffset;
};
```

这个引用通过 row block 和 block 内 offset 定位 fixed row。它可以安全保存在
`std::vector<BmRowRef>` 中，也可以跨 unpin/reclaim 长期存在。

核心公开操作都基于 `BmRowRef`：

```cpp
class BmRowContainer {
 public:
  BmRowRef newRow();

  void store(
      const DecodedVector& decoded,
      vector_size_t sourceIndex,
      BmRowRef row,
      int32_t column);

  int32_t compare(
      BmRowRef left,
      BmRowRef right,
      int32_t column,
      CompareFlags flags = {});

  void extractColumn(
      folly::Range<const BmRowRef*> rows,
      int32_t column,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize = false);

  void extractNulls(
      folly::Range<const BmRowRef*> rows,
      int32_t column,
      const BufferPtr& result);
};
```

容器内部仍然可以在 pin 住 block 后临时拿到 `char*`，但这个指针不能作为稳定行身份逃逸到外部。

## 存储布局

`BmRowContainer` 复用 `RowContainer` 的 row layout 思路：

- key types 在前；
- dependent types 在 key 后；
- 使用 `RowColumn` 描述字段 offset 和 null bit；
- null flags 放在 fixed row bytes 里；
- fixed-width 值 inline 存储。

物理存储从 `AllocationPool + HashStringAllocator` 改成 BufferManager blocks：

```text
BmRowContainer
  rowBlocks_
    block 0: fixed rows
    block 1: fixed rows
    ...

  heapBlocks_
    block 0: variable-width bytes
    block 1: variable-width bytes
    ...
```

block 元数据：

```cpp
struct BmBlockState {
  std::shared_ptr<memory::bm::BlockHandle> block;
  uint32_t usedBytes;
  uint32_t liveRows;
};
```

第一版可以给 row block 使用固定大小，例如 64KB 或 256KB，然后线性 append。
`BmRowRef::rowOffset` 指向 fixed row 在 row block 内的起始位置。

## 变长字段存储

变长值在 store 时直接转成 offset 引用。

不要在 fixed row 里保存 heap `char*` 指针，而是保存稳定描述：

```cpp
struct BmVarRef {
  uint32_t heapBlockId;
  uint32_t heapOffset;
  uint32_t size;
};
```

对于 `VARCHAR` 和 `VARBINARY`，`RowContainer` 里原来放 `StringView` 的 fixed row
位置，在 `BmRowContainer` 中改为放 `BmVarRef`。对于序列化成 bytes 的 complex
types，也同样放 `BmVarRef`。

变长值的 `store()` 流程：

```text
DecodedVector value
  -> 从当前 heap block 分配 bytes
  -> 把 bytes copy 到 pinned heap block
  -> 在 fixed row 中写入 BmVarRef{heapBlockId, heapOffset, size}
```

读取、比较、提取流程：

```text
BmRowRef
  -> pin row block
  -> 从 fixed row 读 BmVarRef
  -> pin heap block
  -> data = heapHandle.Ptr() + heapOffset
```

这样可以避开 DuckDB 的 `base_heap_ptr` 和 `RecomputeHeapPointers` 机制。fixed row
里没有旧 heap 地址，因此不存在需要修复的 stale pointer。

## 为什么第一版不需要 TupleDataChunkPart

如果 fixed row 里的变长字段已经存成 `BmVarRef`，`TupleDataChunkPart` 对正确性不是必须的。

DuckDB 需要 part，一个重要原因是 row 里保存了指向 heap block 的 pointer。part 记录写入这些
pointer 时使用的 heap base address；scan 时如果 block 被重新 pin 到不同地址，就根据 part
里的 base 信息修 pointer。

`BmRowContainer` 从一开始使用 offset layout，就不需要这层 pointer 修复。

part-like 元数据以后仍然可能有价值：

- 为连续 row run 做批量 pin；
- 记录一次 append batch 的 row bytes 和 heap bytes；
- 释放 StreamingWindowBuild 已完成的 partition prefix；
- 支持 vectorized scan chunk；
- prefetch 即将访问的 row blocks 和 heap blocks。

但第一版不建议把 `part` 作为 row identity 的必要组成。先使用直接的
`BmRowRef{rowBlockId, rowOffset}`，如果 profiling 证明 batch pin 或 prefix release
需要更强的聚合信息，再添加 `BmRowRun` 或 `BmRowChunk`。

## Pinning 模型

`PinnedRows` 应该是内部 RAII helper，而不是主要对外 API。

示例形态：

```cpp
class PinnedRows {
 public:
  const char* row(size_t index) const;

 private:
  std::vector<memory::bm::BufferHandle> rowHandles_;
  std::vector<const char*> rows_;
};
```

它在 `BmRowContainer` 方法内部使用：

- `store()` pin 目标 row block；如果是变长值，再 pin 目标 heap block。
- `compare()` pin 左右 row block，并按被比较列需要 pin heap block。
- `extractColumn()` 为请求的 row range pin row blocks，并为变长值 pin heap blocks。
- `extractNulls()` 只需要 pin row blocks。

append 热路径最好增加 batch appender，避免每一行每一列都 pin/unpin：

```cpp
class BmRowAppender {
 public:
  BmRowRef newRow();
  void store(
      const DecodedVector& decoded,
      vector_size_t sourceIndex,
      BmRowRef row,
      int32_t column);
};
```

`StreamingWindowBuild::addInput()` 可以每个 input batch 创建一个 appender。

## StreamingWindowBuild 接入

新增 BM 专用 window build 路径，不直接改掉所有 `WindowBuild` 的存储模型。

建议改动：

- 保留现有 `RowContainer` 给当前 window builds 使用。
- 只在 BM-enabled StreamingWindowBuild 路径中新增 `std::unique_ptr<BmRowContainer> bmData_`。
- BM 路径里的 row vectors 改成 `std::vector<BmRowRef>`。
- 增加一个 BM partition 实现，持有 `BmRowRef` range，而不是 `char**`。

概念形态：

```cpp
class BmWindowPartition : public WindowPartition {
 public:
  BmWindowPartition(
      BmRowContainer* data,
      folly::Range<const BmRowRef*> rows,
      ...);

  void extractColumn(...) const override;
  void extractNulls(...) const;
  std::pair<vector_size_t, vector_size_t> computePeerBuffers(...) const override;
};
```

当前 `WindowPartition` base class 和 `RowContainer*`、`folly::Range<char**>` 绑定较深。
实现阶段可以二选一：

- 新增 sibling partition base，BM path 使用新的 partition 类型；
- 小心泛化现有 `WindowPartition`，但不影响旧路径。

这个选择留到 implementation plan 阶段根据实际改动面决定。

## BufferManager 来源

使用 commit `7de0785683d8e8a4ddc59149929957edee7f8da9` 新增的 task-level
BufferManager。

operator 只在这些条件满足时 opt in：

- query config 开启 BufferManager；
- task 已创建 BufferManager；
- spill directory 已配置；
- window build type 是目标 StreamingWindowBuild 路径。

如果 BM 不可用，继续走现有 `RowContainer` 路径。

row block 和 heap block 分配使用 `memory::bm::MemoryTag::kWindow`。

## 内存统计和释放

容器暴露近似内存指标：

```cpp
uint64_t allocatedBytes() const;
uint64_t usedBytes() const;
std::optional<int64_t> estimateRowSize() const;
```

StreamingWindowBuild 输出完的 prefix partitions 可以按 row ref range 释放：

```cpp
void releaseRows(folly::Range<const BmRowRef*> rows);
```

第一版可以粗粒度实现 release：

- 每个 row block 维护 `liveRows`；
- row block 的 `liveRows` 归零后 drop 对应 `BlockHandle`；
- heap block 先按 container 生命周期释放；
- 如果之后需要更早释放 heap，再按 row 的 `BmVarRef` 追踪 heap 引用或 heap bytes。

这样能先保证正确性，同时让已经消费完的 row blocks 具备回收机会。

## 测试计划

`BmRowContainer` 单测：

- fixed-width append/extract round trip；
- nullable columns；
- variable-width string append/extract round trip；
- variable-width 数据在 row block 和 heap block unpin/re-pin 后仍可读取；
- fixed-width column compare；
- variable-width column compare；
- 多 row blocks；
- 多 heap blocks；
- `BmRowRef` 在 BM reclaim 和 re-pin 后仍有效；
- completed rows release 后，在可能的情况下 drop row block handles。

Window 测试：

- BM enabled 的 StreamingWindowBuild basic partition output；
- partition boundary compare 跨 row block；
- variable-width sort key 的 peer boundary compare；
- 如果支持 range frame，覆盖 variable-width frame/order columns；
- BM disabled 时 fallback 到现有 RowContainer。

回归测试：

- 现有 `RowContainerTest`；
- 现有 `WindowTest` 和 `RowStreamingWindowTest`；
- task-level BufferManager tests。

## 主要设计决策

1. `newRow()` 返回 `BmRowRef`，不返回 `char*`。
2. fixed row 存在 BM row blocks。
3. variable-width bytes 存在 BM heap blocks。
4. fixed row 存 `BmVarRef`，不存 heap pointer。
5. `PinnedRows` 是容器内部 RAII 状态。
6. 第一版不要求 DuckDB 风格 `part`。
7. StreamingWindowBuild 保存 row refs，不保存 row pointers。
8. 现有 `RowContainer` 对当前使用方保持不变。

