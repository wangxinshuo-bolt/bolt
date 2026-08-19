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

#include <gtest/gtest.h>

namespace bytedance::bolt::exec::test {
namespace {

struct HashBuildBmStats {
  int64_t backend{0};
  int64_t fallback{0};
  int64_t spilledBytes{0};
  int64_t restoreCount{0};
};

int64_t runtimeStatSum(
    const std::unordered_map<std::string, RuntimeMetric>& stats,
    const std::string& name) {
  auto it = stats.find(name);
  return it == stats.end() ? 0 : it->second.sum;
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
          runtimeStatSum(op.runtimeStats, "bmHashJoinFallbackReason");
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
      std::vector<std::string> outputLayout = {"t_k", "t_payload", "u_payload"}) {
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

  std::shared_ptr<Task> runBmHashJoin(
      const core::PlanNodePtr& plan,
      const RowVectorPtr& expected,
      bool enableBmHashJoin,
      bool serialExecution = true,
      int32_t maxDrivers = 1) {
    auto spillDirectory = TempDirectoryPath::create();
    return AssertQueryBuilder(plan)
        .serialExecution(serialExecution)
        .maxDrivers(maxDrivers)
        .spillDirectory(spillDirectory->getPath())
        .config(
            core::QueryConfig::kBmHashJoinEnabled,
            enableBmHashJoin ? "true" : "false")
        .config(
            core::QueryConfig::kBufferManagerEnabled,
            enableBmHashJoin ? "true" : "false")
        .config(core::QueryConfig::kJitLevel, "0")
        .config(core::QueryConfig::kHybridJoinEnabled, "false")
        .assertResults(expected);
  }

  RowVectorPtr probe_;
  RowVectorPtr build_;
  RowVectorPtr expectedInner_;
};

TEST_F(BmHashJoinTest, disabledFlagUsesLegacyHashJoinBackend) {
  auto task = runBmHashJoin(makePlan(), expectedInner_, false);

  const auto stats = hashBuildBmStats(*task);
  EXPECT_EQ(0, stats.backend);
  EXPECT_NE(0, stats.fallback);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(BmHashJoinTest, serialInnerJoinUsesResidentBmBackend) {
  auto task = runBmHashJoin(makePlan(), expectedInner_, true);

  const auto stats = hashBuildBmStats(*task);
  EXPECT_EQ(1, stats.backend);
  EXPECT_EQ(0, stats.spilledBytes);
  EXPECT_EQ(0, stats.restoreCount);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(BmHashJoinTest, multiDriverFallsBackToLegacyHashJoinBackend) {
  auto task = runBmHashJoin(
      makePlan(), expectedInner_, true, false, 2);

  const auto stats = hashBuildBmStats(*task);
  EXPECT_EQ(0, stats.backend);
  EXPECT_NE(0, stats.fallback);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(BmHashJoinTest, parallelSingleDriverFallsBackToLegacyHashJoinBackend) {
  auto task = runBmHashJoin(
      makePlan(), expectedInner_, true, false, 1);

  const auto stats = hashBuildBmStats(*task);
  EXPECT_EQ(0, stats.backend);
  EXPECT_NE(0, stats.fallback);
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

  const auto stats = hashBuildBmStats(*task);
  EXPECT_EQ(0, stats.backend);
  EXPECT_NE(0, stats.fallback);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

} // namespace
} // namespace bytedance::bolt::exec::test
