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
  int64_t spilledBytes{0};
  int64_t restoreCount{0};
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
      result.spilledBytes +=
          runtimeStatSum(op.runtimeStats, "bmHashJoinSpilledBytes");
      result.restoreCount +=
          runtimeStatSum(op.runtimeStats, "bmHashJoinRestoreCount");
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

  void expectBmBackend(const Task& task) {
    const auto stats = hashBuildBmStats(task);
    EXPECT_EQ(1, stats.backend);
    EXPECT_EQ(0, stats.fallback);
    EXPECT_EQ(0, stats.spilledBytes);
    EXPECT_EQ(0, stats.restoreCount);
  }

  void expectFallback(const Task& task, BmFallback reason) {
    const auto stats = hashBuildBmStats(task);
    EXPECT_EQ(0, stats.backend);
    EXPECT_EQ(static_cast<int64_t>(reason), stats.fallback);
    EXPECT_EQ(0, stats.spilledBytes);
    EXPECT_EQ(0, stats.restoreCount);
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

} // namespace
} // namespace bytedance::bolt::exec::test
