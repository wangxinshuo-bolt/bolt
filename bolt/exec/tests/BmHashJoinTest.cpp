/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/exec/HashBuild.h"
#include "bolt/exec/HashTable.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <gtest/gtest.h>

namespace bytedance::bolt::exec::test {
namespace {

struct HashBuildBmStats {
  int64_t backend{0};
  int64_t fallback{0};
  int64_t spilledRows{0};
  int64_t spilledBytes{0};
  int64_t spilledSegments{0};
  int64_t restoreCount{0};
  int64_t spillWriteCount{0};
  int64_t spillReadCount{0};
  int64_t spillWriteBytes{0};
  int64_t spillReadBytes{0};
  int64_t spillPhysicalWriteBytes{0};
  int64_t spillPhysicalReadBytes{0};
  int64_t legacySpilledRows{0};
  int64_t legacySpilledBytes{0};
  int64_t legacySpilledFiles{0};
};

struct ForcedSpillJoinData {
  RowVectorPtr probe;
  RowVectorPtr build;
  RowVectorPtr expected;
};

using BmFallback = HashBuild::BmHashJoinFallbackReason;

int64_t runtimeStatSum(
    const std::unordered_map<std::string, RuntimeMetric>& stats,
    const std::string& name) {
  auto it = stats.find(name);
  return it == stats.end() ? 0 : it->second.sum;
}

int64_t runtimeStatExactCode(
    const std::unordered_map<std::string, RuntimeMetric>& stats,
    const std::string& name) {
  auto it = stats.find(name);
  if (it == stats.end()) {
    return 0;
  }
  EXPECT_EQ(it->second.min, it->second.max);
  return it->second.max;
}

HashBuildBmStats hashBuildBmStats(const Task& task) {
  HashBuildBmStats result;
  for (const auto& pipeline : task.taskStats().pipelineStats) {
    for (const auto& op : pipeline.operatorStats) {
      if (op.operatorType != "HashBuild") {
        continue;
      }
      result.backend += runtimeStatSum(op.runtimeStats, "bmHashJoinBackend");
      result.fallback +=
          runtimeStatExactCode(op.runtimeStats, "bmHashJoinFallbackReason");
      result.spilledRows +=
          runtimeStatSum(op.runtimeStats, "bmHashJoinSpilledRows");
      result.spilledBytes +=
          runtimeStatSum(op.runtimeStats, "bmHashJoinSpilledBytes");
      result.spilledSegments +=
          runtimeStatSum(op.runtimeStats, "bmHashJoinSpilledSegments");
      result.restoreCount +=
          runtimeStatSum(op.runtimeStats, "bmHashJoinRestoreCount");
      result.spillWriteCount +=
          runtimeStatSum(op.runtimeStats, "bmHashJoinSpillWriteCount");
      result.spillReadCount +=
          runtimeStatSum(op.runtimeStats, "bmHashJoinSpillReadCount");
      result.spillWriteBytes +=
          runtimeStatSum(op.runtimeStats, "bmHashJoinSpillWriteBytes");
      result.spillReadBytes +=
          runtimeStatSum(op.runtimeStats, "bmHashJoinSpillReadBytes");
      result.spillPhysicalWriteBytes += runtimeStatSum(
          op.runtimeStats, "bmHashJoinSpillPhysicalWriteBytes");
      result.spillPhysicalReadBytes += runtimeStatSum(
          op.runtimeStats, "bmHashJoinSpillPhysicalReadBytes");
      result.legacySpilledRows += op.spilledRows;
      result.legacySpilledBytes += op.spilledBytes;
      result.legacySpilledFiles += op.spilledFiles;
    }
  }
  return result;
}

class BmHashJoinTest : public HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    probe_ = makeRowVector(
        {"t_k", "t_payload"},
        {
            makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
            makeFlatVector<int32_t>({10, 20, 30, 40, 50}),
        });
    build_ = makeRowVector(
        {"u_k", "u_payload"},
        {
            makeFlatVector<int32_t>({1, 1, 3, 5, 6}),
            makeFlatVector<int32_t>({100, 101, 300, 500, 600}),
        });
    expectedInner_ = makeRowVector(
        {"t_k", "t_payload", "u_payload"},
        {
            makeFlatVector<int32_t>({1, 1, 3, 5}),
            makeFlatVector<int32_t>({10, 10, 30, 50}),
            makeFlatVector<int32_t>({100, 101, 300, 500}),
        });
  }

  void TearDown() override {
    probe_.reset();
    build_.reset();
    expectedInner_.reset();
    HiveConnectorTestBase::TearDown();
  }

  core::PlanNodePtr makePlan(
      core::JoinType joinType = core::JoinType::kInner,
      std::vector<std::string> outputLayout = {
          "t_k",
          "t_payload",
          "u_payload"}) {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    return PlanBuilder(planNodeIdGenerator)
        .values({probe_})
        .hashJoin(
            {"t_k"},
            {"u_k"},
            PlanBuilder(planNodeIdGenerator).values({build_}).planNode(),
            "",
            outputLayout,
            joinType)
        .planNode();
  }

  std::shared_ptr<Task> runPlan(
      const core::PlanNodePtr& plan,
      const RowVectorPtr& expected,
      bool enableBmHashJoin,
      bool serialExecution = true,
      int32_t maxDrivers = 1,
      bool enableBufferManager = true,
      const std::unordered_map<std::string, std::string>& configs = {},
      std::shared_ptr<core::QueryCtx> queryCtx = nullptr) {
    auto spillDirectory = TempDirectoryPath::create();
    auto builder = AssertQueryBuilder(plan);
    builder.serialExecution(serialExecution)
        .maxDrivers(maxDrivers)
        .spillDirectory(spillDirectory->getPath())
        .config(
            core::QueryConfig::kBmHashJoinEnabled,
            enableBmHashJoin ? "true" : "false")
        .config(
            core::QueryConfig::kBufferManagerEnabled,
            enableBufferManager ? "true" : "false")
        .config(core::QueryConfig::kJitLevel, "0")
        .config(core::QueryConfig::kHybridJoinEnabled, "false");
    for (const auto& [key, value] : configs) {
      builder.config(key, value);
    }
    if (queryCtx != nullptr) {
      builder.queryCtx(std::move(queryCtx));
    }
    return builder.assertResults(expected);
  }

  std::shared_ptr<Task> runBmHashJoin(
      const core::PlanNodePtr& plan,
      const RowVectorPtr& expected,
      bool enableBmHashJoin,
      bool serialExecution = true,
      int32_t maxDrivers = 1) {
    return runPlan(
        plan,
        expected,
        enableBmHashJoin,
        serialExecution,
        maxDrivers,
        enableBmHashJoin);
  }

  core::PlanNodePtr makePlanForVectors(
      const RowVectorPtr& probe,
      const RowVectorPtr& build,
      const std::vector<std::string>& probeKeys,
      const std::vector<std::string>& buildKeys,
      const std::vector<std::string>& outputLayout,
      core::JoinType joinType = core::JoinType::kInner) {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    return PlanBuilder(planNodeIdGenerator)
        .values({probe})
        .hashJoin(
            probeKeys,
            buildKeys,
            PlanBuilder(planNodeIdGenerator).values({build}).planNode(),
            "",
            outputLayout,
            joinType)
        .planNode();
  }

  std::shared_ptr<core::HashJoinNode> makeReusableTablePlan() {
    auto probe = makeRowVector(
        {"t_k"},
        {
            makeFlatVector<int64_t>({1, 2, 3}),
        });
    auto build = makeRowVector(
        {"u_k"},
        {
            makeFlatVector<int64_t>({1, 2, 4}),
        });

    std::vector<std::unique_ptr<VectorHasher>> hashers;
    hashers.push_back(std::make_unique<VectorHasher>(BIGINT(), 0));
    std::shared_ptr<BaseHashTable> table = HashTable<false>::createForJoin(
        std::move(hashers),
        {}, /*dependentTypes*/
        true, /*allowDuplicates*/
        true, /*hasProbedFlag*/
        BaseHashTable::HashMode::kHash,
        1, /*minTableSizeForParallelJoinBuild*/
        pool(),
        false);

    auto* rowContainer = table->rows();
    const auto nextOffset = rowContainer->nextOffset();
    SelectivityVector rows(build->size());
    DecodedVector decoded(*build->childAt(0), rows);
    for (auto i = 0; i < build->size(); ++i) {
      auto* row = rowContainer->newRow();
      if (nextOffset) {
        *reinterpret_cast<char**>(row + nextOffset) = nullptr;
      }
      rowContainer->store(decoded, i, row, 0);
    }
    table->prepareJoinTable(
        {}, nullptr, false, BaseHashTable::kNoSpillInputStartPartitionBit);

    auto opaqueSharedHashTable = std::shared_ptr<core::OpaqueHashTable>(
        table, reinterpret_cast<core::OpaqueHashTable*>(table.get()));
    auto plan = std::dynamic_pointer_cast<const core::HashJoinNode>(
        makePlanForVectors(
            probe, build, {"t_k"}, {"u_k"}, {"t_k", "u_k"}));
    plan->setReusableHashTable(std::move(opaqueSharedHashTable));
    return std::const_pointer_cast<core::HashJoinNode>(plan);
  }

  ForcedSpillJoinData makeForcedSpillJoinData(
      int32_t numBuildRows,
      int32_t blobBytes) {
    std::vector<int32_t> buildKeys;
    std::vector<int32_t> buildPayloads;
    std::vector<std::string> buildBlobs;
    buildKeys.reserve(numBuildRows);
    buildPayloads.reserve(numBuildRows);
    buildBlobs.reserve(numBuildRows);
    for (auto i = 0; i < numBuildRows; ++i) {
      buildKeys.push_back(i);
      buildPayloads.push_back(100000 + i);
      buildBlobs.push_back(
          std::string(blobBytes, static_cast<char>('a' + (i % 26))));
    }

    std::vector<int32_t> probeKeys{0, 17, numBuildRows / 3, numBuildRows - 1, -1};
    std::vector<int32_t> probePayloads{10, 20, 30, 40, 50};

    return ForcedSpillJoinData{
        .probe = makeRowVector(
            {"t_k", "t_payload"},
            {
                makeFlatVector<int32_t>(probeKeys),
                makeFlatVector<int32_t>(probePayloads),
            }),
        .build = makeRowVector(
            {"u_k", "u_payload", "u_blob"},
            {
                makeFlatVector<int32_t>(buildKeys),
                makeFlatVector<int32_t>(buildPayloads),
                makeFlatVector<std::string>(buildBlobs),
            }),
        .expected = makeRowVector(
            {"t_k", "t_payload", "u_payload"},
            {
                makeFlatVector<int32_t>(
                    {probeKeys[0], probeKeys[1], probeKeys[2], probeKeys[3]}),
                makeFlatVector<int32_t>(
                    {probePayloads[0],
                     probePayloads[1],
                     probePayloads[2],
                     probePayloads[3]}),
                makeFlatVector<int32_t>(
                    {100000 + probeKeys[0],
                     100000 + probeKeys[1],
                     100000 + probeKeys[2],
                     100000 + probeKeys[3]}),
            })};
  }

  void expectBmBackend(const Task& task) {
    const auto stats = hashBuildBmStats(task);
    EXPECT_EQ(1, stats.backend);
    EXPECT_EQ(0, stats.fallback);
    EXPECT_EQ(0, stats.spilledRows);
    EXPECT_EQ(0, stats.spilledBytes);
    EXPECT_EQ(0, stats.spilledSegments);
    EXPECT_EQ(0, stats.restoreCount);
    EXPECT_EQ(0, stats.spillWriteCount);
    EXPECT_EQ(0, stats.spillReadCount);
    EXPECT_EQ(0, stats.spillWriteBytes);
    EXPECT_EQ(0, stats.spillReadBytes);
    EXPECT_EQ(0, stats.spillPhysicalWriteBytes);
    EXPECT_EQ(0, stats.spillPhysicalReadBytes);
    EXPECT_EQ(0, stats.legacySpilledRows);
    EXPECT_EQ(0, stats.legacySpilledBytes);
    EXPECT_EQ(0, stats.legacySpilledFiles);
  }

  void expectFallback(const Task& task, BmFallback reason) {
    const auto stats = hashBuildBmStats(task);
    EXPECT_EQ(0, stats.backend);
    EXPECT_EQ(static_cast<int64_t>(reason), stats.fallback);
    EXPECT_EQ(0, stats.spilledRows);
    EXPECT_EQ(0, stats.spilledBytes);
    EXPECT_EQ(0, stats.spilledSegments);
    EXPECT_EQ(0, stats.restoreCount);
    EXPECT_EQ(0, stats.spillWriteCount);
    EXPECT_EQ(0, stats.spillReadCount);
    EXPECT_EQ(0, stats.spillWriteBytes);
    EXPECT_EQ(0, stats.spillReadBytes);
    EXPECT_EQ(0, stats.spillPhysicalWriteBytes);
    EXPECT_EQ(0, stats.spillPhysicalReadBytes);
    EXPECT_EQ(0, stats.legacySpilledRows);
    EXPECT_EQ(0, stats.legacySpilledBytes);
    EXPECT_EQ(0, stats.legacySpilledFiles);
  }

  RowVectorPtr probe_;
  RowVectorPtr build_;
  RowVectorPtr expectedInner_;
};

TEST_F(BmHashJoinTest, disabledFlagUsesLegacyHashJoinBackend) {
  auto task = runBmHashJoin(makePlan(), expectedInner_, false);

  expectFallback(*task, BmFallback::kDisabled);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(BmHashJoinTest, serialInnerJoinUsesResidentBmBackend) {
  auto task = runBmHashJoin(makePlan(), expectedInner_, true);

  expectBmBackend(*task);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(BmHashJoinTest, multiDriverFallsBackToLegacyHashJoinBackend) {
  auto task = runBmHashJoin(
      makePlan(), expectedInner_, true, false, 2);

  expectFallback(*task, BmFallback::kNonSerialExecution);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(BmHashJoinTest, parallelSingleDriverFallsBackToLegacyHashJoinBackend) {
  auto task = runBmHashJoin(
      makePlan(), expectedInner_, true, false, 1);

  expectFallback(*task, BmFallback::kNonSerialExecution);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(BmHashJoinTest, nonInnerJoinFallsBackToLegacyHashJoinBackend) {
  auto expected = makeRowVector(
      {"t_k", "t_payload", "u_payload"},
      {
          makeFlatVector<int32_t>({1, 1, 2, 3, 4, 5}),
          makeFlatVector<int32_t>({10, 10, 20, 30, 40, 50}),
          makeNullableFlatVector<int32_t>(
              {100, 101, std::nullopt, 300, std::nullopt, 500}),
      });

  auto task = runBmHashJoin(
      makePlan(core::JoinType::kLeft), expected, true);

  expectFallback(*task, BmFallback::kUnsupportedJoinType);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(BmHashJoinTest, noBufferManagerFallsBackBeforeBmTableConstruction) {
  auto task = runPlan(
      makePlan(),
      expectedInner_,
      true,
      true,
      1,
      false);

  expectFallback(*task, BmFallback::kNoBufferManager);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(BmHashJoinTest, suppliedExecutorFallsBackToLegacyHashJoinBackend) {
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(1);
  auto queryCtx = core::QueryCtx::create(executor.get());

  auto task = runPlan(
      makePlan(),
      expectedInner_,
      true,
      false,
      1,
      true,
      {},
      queryCtx);

  expectFallback(*task, BmFallback::kNonSerialExecution);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(BmHashJoinTest, jitRowEqualityFallsBackWithExactReason) {
  auto task = runPlan(
      makePlan(),
      expectedInner_,
      true,
      true,
      1,
      true,
      {{core::QueryConfig::kJitLevel, "-1"}});

  expectFallback(*task, BmFallback::kJitRowEq);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(BmHashJoinTest, hybridScatteredFallsBackWithExactReason) {
  auto task = runPlan(
      makePlan(),
      expectedInner_,
      true,
      true,
      1,
      true,
      {{core::QueryConfig::kHybridJoinEnabled, "true"},
       {core::QueryConfig::kHybridJoinScatteredModeEnabled, "true"}});

  expectFallback(*task, BmFallback::kHybridJoin);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(BmHashJoinTest, reusableHashTableFallsBackWithExactReason) {
  auto expected = makeRowVector(
      {"t_k", "u_k"},
      {
          makeFlatVector<int64_t>({1, 2}),
          makeFlatVector<int64_t>({1, 2}),
      });
  auto task = runPlan(makeReusableTablePlan(), expected, true);

  expectFallback(*task, BmFallback::kReusableHashTable);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(BmHashJoinTest, complexKeyFallsBackBeforeBmTableConstruction) {
  auto probe = makeRowVector(
      {"t_k", "t_payload"},
      {
          makeArrayVector<int32_t>({{1, 2}, {3}, {4}, {1, 2}}),
          makeFlatVector<int32_t>({10, 30, 40, 11}),
      });
  auto build = makeRowVector(
      {"u_k", "u_payload"},
      {
          makeArrayVector<int32_t>({{1, 2}, {3}, {5}}),
          makeFlatVector<int32_t>({100, 300, 500}),
      });
  auto expected = makeRowVector(
      {"t_payload", "u_payload"},
      {
          makeFlatVector<int32_t>({10, 11, 30}),
          makeFlatVector<int32_t>({100, 100, 300}),
      });

  auto task = runPlan(
      makePlanForVectors(
          probe, build, {"t_k"}, {"u_k"}, {"t_payload", "u_payload"}),
      expected,
      true);

  expectFallback(*task, BmFallback::kUnsupportedType);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(BmHashJoinTest, complexPayloadFallsBackBeforeBmTableConstruction) {
  auto probe = makeRowVector(
      {"t_k", "t_payload"},
      {
          makeFlatVector<int32_t>({1, 2, 3}),
          makeFlatVector<int32_t>({10, 20, 30}),
      });
  auto build = makeRowVector(
      {"u_k", "u_payload"},
      {
          makeFlatVector<int32_t>({1, 3, 4}),
          makeArrayVector<int32_t>({{100, 101}, {300}, {400}}),
      });
  auto expected = makeRowVector(
      {"t_payload", "u_payload"},
      {
          makeFlatVector<int32_t>({10, 30}),
          makeArrayVector<int32_t>({{100, 101}, {300}}),
      });

  auto task = runPlan(
      makePlanForVectors(
          probe, build, {"t_k"}, {"u_k"}, {"t_payload", "u_payload"}),
      expected,
      true);

  expectFallback(*task, BmFallback::kUnsupportedType);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(BmHashJoinTest, smallIntegerArrayModeCandidateStillUsesBmGenericHash) {
  auto probe = makeRowVector(
      {"t_k", "t_payload"},
      {
          makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
          makeFlatVector<int32_t>({10, 20, 30, 40, 50}),
      });
  auto build = makeRowVector(
      {"u_k", "u_payload"},
      {
          makeFlatVector<int32_t>({1, 3, 5}),
          makeFlatVector<int32_t>({100, 300, 500}),
      });
  auto expected = makeRowVector(
      {"t_k", "t_payload", "u_payload"},
      {
          makeFlatVector<int32_t>({1, 3, 5}),
          makeFlatVector<int32_t>({10, 30, 50}),
          makeFlatVector<int32_t>({100, 300, 500}),
      });

  auto task = runPlan(
      makePlanForVectors(
          probe,
          build,
          {"t_k"},
          {"u_k"},
          {"t_k", "t_payload", "u_payload"}),
      expected,
      true);

  expectBmBackend(*task);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(BmHashJoinTest, duplicateCompositeKeysSpanOutputBatchesOnBmBackend) {
  auto probe = makeRowVector(
      {"t_k1", "t_k2", "t_payload"},
      {
          makeFlatVector<int32_t>({7}),
          makeFlatVector<StringView>({"seven"}),
          makeFlatVector<int32_t>({70}),
      });
  auto build = makeRowVector(
      {"u_k1", "u_k2", "u_payload"},
      {
          makeFlatVector<int32_t>({7, 7, 7, 7, 7}),
          makeFlatVector<StringView>(
              {"seven", "seven", "seven", "seven", "seven"}),
          makeFlatVector<int32_t>({100, 101, 102, 103, 104}),
      });
  auto expected = makeRowVector(
      {"t_k1", "t_k2", "t_payload", "u_payload"},
      {
          makeFlatVector<int32_t>({7, 7, 7, 7, 7}),
          makeFlatVector<StringView>(
              {"seven", "seven", "seven", "seven", "seven"}),
          makeFlatVector<int32_t>({70, 70, 70, 70, 70}),
          makeFlatVector<int32_t>({100, 101, 102, 103, 104}),
      });

  auto task = runPlan(
      makePlanForVectors(
          probe,
          build,
          {"t_k1", "t_k2"},
          {"u_k1", "u_k2"},
          {"t_k1", "t_k2", "t_payload", "u_payload"}),
      expected,
      true,
      true,
      1,
      true,
      {{core::QueryConfig::kPreferredOutputBatchRows, "2"}});

  expectBmBackend(*task);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(BmHashJoinTest, forcedSpillUsesRealBmIoMetricsAcrossThresholds) {
  const auto data = makeForcedSpillJoinData(384, 8192);
  const auto plan = makePlanForVectors(
      data.probe,
      data.build,
      {"t_k"},
      {"u_k"},
      {"t_k", "t_payload", "u_payload"});

  auto tinyTask = runPlan(
      plan,
      data.expected,
      true,
      true,
      1,
      true,
      {{core::QueryConfig::kBmHashJoinSpillThreshold, "16384"}});
  auto moderateTask = runPlan(
      plan,
      data.expected,
      true,
      true,
      1,
      true,
      {{core::QueryConfig::kBmHashJoinSpillThreshold, "262144"}});
  auto legacyTask = runPlan(
      plan,
      data.expected,
      false,
      true,
      1,
      true,
      {{core::QueryConfig::kBmHashJoinSpillThreshold, "16384"}});

  const auto tinyStats = hashBuildBmStats(*tinyTask);
  const auto moderateStats = hashBuildBmStats(*moderateTask);
  const auto legacyStats = hashBuildBmStats(*legacyTask);

  EXPECT_EQ(1, tinyStats.backend);
  EXPECT_EQ(0, tinyStats.fallback);
  EXPECT_GT(tinyStats.spilledRows, 0);
  EXPECT_GT(tinyStats.spilledBytes, 0);
  EXPECT_GT(tinyStats.spilledSegments, 0);
  EXPECT_GT(tinyStats.restoreCount, 0);
  EXPECT_GT(tinyStats.spillWriteCount, 0);
  EXPECT_GT(tinyStats.spillReadCount, 0);
  EXPECT_GT(tinyStats.spillWriteBytes, 0);
  EXPECT_GT(tinyStats.spillReadBytes, 0);
  EXPECT_GT(tinyStats.spillPhysicalWriteBytes, 0);
  EXPECT_GT(tinyStats.spillPhysicalReadBytes, 0);
  EXPECT_EQ(0, tinyStats.legacySpilledRows);
  EXPECT_EQ(0, tinyStats.legacySpilledBytes);
  EXPECT_EQ(0, tinyStats.legacySpilledFiles);

  EXPECT_EQ(1, moderateStats.backend);
  EXPECT_EQ(0, moderateStats.fallback);
  EXPECT_GT(moderateStats.spilledRows, 0);
  EXPECT_GT(moderateStats.spilledBytes, 0);
  EXPECT_GT(moderateStats.spilledSegments, 0);
  EXPECT_GT(moderateStats.restoreCount, 0);
  EXPECT_GT(moderateStats.spillWriteCount, 0);
  EXPECT_GT(moderateStats.spillReadCount, 0);
  EXPECT_GT(moderateStats.spillWriteBytes, 0);
  EXPECT_GT(moderateStats.spillReadBytes, 0);
  EXPECT_GT(moderateStats.spillPhysicalWriteBytes, 0);
  EXPECT_GT(moderateStats.spillPhysicalReadBytes, 0);
  EXPECT_EQ(0, moderateStats.legacySpilledRows);
  EXPECT_EQ(0, moderateStats.legacySpilledBytes);
  EXPECT_EQ(0, moderateStats.legacySpilledFiles);

  EXPECT_GT(tinyStats.spilledSegments, moderateStats.spilledSegments);
  EXPECT_GT(tinyStats.spilledRows, moderateStats.spilledRows);

  expectFallback(*legacyTask, BmFallback::kDisabled);

  moderateTask.reset();
  legacyTask.reset();
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(tinyTask);
}

TEST_F(BmHashJoinTest, forcedSpillRepeatsWithoutFallingBack) {
  const auto data = makeForcedSpillJoinData(384, 8192);
  const auto plan = makePlanForVectors(
      data.probe,
      data.build,
      {"t_k"},
      {"u_k"},
      {"t_k", "t_payload", "u_payload"});

  for (auto iteration = 0; iteration < 2; ++iteration) {
    auto task = runPlan(
        plan,
        data.expected,
        true,
        true,
        1,
        true,
        {{core::QueryConfig::kBmHashJoinSpillThreshold, "16384"}});
    const auto stats = hashBuildBmStats(*task);
    EXPECT_EQ(1, stats.backend);
    EXPECT_EQ(0, stats.fallback);
    EXPECT_GT(stats.spilledRows, 0);
    EXPECT_GT(stats.spilledSegments, 0);
    EXPECT_GT(stats.restoreCount, 0);
    EXPECT_GT(stats.spillWriteCount, 0);
    EXPECT_GT(stats.spillReadCount, 0);
    EXPECT_EQ(0, stats.legacySpilledRows);
    EXPECT_EQ(0, stats.legacySpilledBytes);
    EXPECT_EQ(0, stats.legacySpilledFiles);
    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  }
}

TEST_F(BmHashJoinTest, dateAndDecimalAliasesUseBmBackend) {
  const auto dateType = DATE();
  const auto shortDecimalType = DECIMAL(10, 2);
  const auto longDecimalType = DECIMAL(30, 8);
  ASSERT_TRUE(dateType->isDate());
  ASSERT_TRUE(shortDecimalType->isShortDecimal());
  ASSERT_TRUE(longDecimalType->isLongDecimal());
  ASSERT_EQ(TypeKind::INTEGER, dateType->kind());
  ASSERT_EQ(TypeKind::BIGINT, shortDecimalType->kind());
  ASSERT_EQ(TypeKind::HUGEINT, longDecimalType->kind());

  auto probe = makeRowVector(
      {"t_date", "t_short", "t_long", "t_payload"},
      {
          makeFlatVector<int32_t>(
              {DATE()->toDays("2024-01-01"),
               DATE()->toDays("2024-01-02"),
               DATE()->toDays("2024-01-03")},
              dateType),
          makeFlatVector<int64_t>({12345, 22222, 33333}, shortDecimalType),
          makeFlatVector<int128_t>(
              {static_cast<int128_t>(900000000000000000LL),
               static_cast<int128_t>(800000000000000000LL),
               static_cast<int128_t>(700000000000000000LL)},
              longDecimalType),
          makeFlatVector<int32_t>({10, 20, 30}),
      });
  auto build = makeRowVector(
      {"u_date", "u_short", "u_long", "u_payload_short", "u_payload_long"},
      {
          makeFlatVector<int32_t>(
              {DATE()->toDays("2024-01-03"),
               DATE()->toDays("2024-01-01"),
               DATE()->toDays("2024-01-04")},
              dateType),
          makeFlatVector<int64_t>({33333, 12345, 44444}, shortDecimalType),
          makeFlatVector<int128_t>(
              {static_cast<int128_t>(700000000000000000LL),
               static_cast<int128_t>(900000000000000000LL),
               static_cast<int128_t>(600000000000000000LL)},
              longDecimalType),
          makeFlatVector<int64_t>({30303, 10101, 40404}, shortDecimalType),
          makeFlatVector<int128_t>(
              {static_cast<int128_t>(707070700000000000LL),
               static_cast<int128_t>(101010100000000000LL),
               static_cast<int128_t>(404040400000000000LL)},
              longDecimalType),
      });
  auto expected = makeRowVector(
      {"t_date", "t_short", "t_long", "t_payload", "u_payload_short", "u_payload_long"},
      {
          makeFlatVector<int32_t>(
              {DATE()->toDays("2024-01-01"),
               DATE()->toDays("2024-01-03")},
              dateType),
          makeFlatVector<int64_t>({12345, 33333}, shortDecimalType),
          makeFlatVector<int128_t>(
              {static_cast<int128_t>(900000000000000000LL),
               static_cast<int128_t>(700000000000000000LL)},
              longDecimalType),
          makeFlatVector<int32_t>({10, 30}),
          makeFlatVector<int64_t>({10101, 30303}, shortDecimalType),
          makeFlatVector<int128_t>(
              {static_cast<int128_t>(101010100000000000LL),
               static_cast<int128_t>(707070700000000000LL)},
              longDecimalType),
      });

  auto task = runPlan(
      makePlanForVectors(
          probe,
          build,
          {"t_date", "t_short", "t_long"},
          {"u_date", "u_short", "u_long"},
          {"t_date",
           "t_short",
           "t_long",
           "t_payload",
           "u_payload_short",
           "u_payload_long"}),
      expected,
      true);

  expectBmBackend(*task);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

} // namespace
} // namespace bytedance::bolt::exec::test
