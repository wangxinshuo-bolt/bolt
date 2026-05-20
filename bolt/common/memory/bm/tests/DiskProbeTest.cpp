/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <chrono>
#include <filesystem>

#include <gtest/gtest.h>

#include "bolt/common/memory/bm/DiskProbe.h"

namespace bytedance::bolt::memory::bm {
namespace {

TEST(DiskProbeTest, honorsForcedKind) {
  DiskProbeConfig config;
  config.directory =
      (std::filesystem::temp_directory_path() / "bolt_bm_forced_probe").string();
  config.forcedKind = DiskKind::kNvme;
  config.fallbackKind = DiskKind::kHdd;

  auto result = ProbeDisk(config);

  EXPECT_EQ(result.kind, DiskKind::kNvme);
  EXPECT_FALSE(result.activeProbeRan);
  EXPECT_FALSE(result.directIoUsed);
  EXPECT_EQ(result.targetP95LatencyUs, TargetP95LatencyForDisk(DiskKind::kNvme));
}

TEST(DiskProbeTest, usesFallbackWithoutActiveProbe) {
  DiskProbeConfig config;
  config.directory =
      (std::filesystem::temp_directory_path() / "bolt_bm_disabled_probe").string();
  config.duration = std::chrono::milliseconds(0);
  config.fallbackKind = DiskKind::kSsd;

  auto result = ProbeDisk(config);

  EXPECT_EQ(result.kind, DiskKind::kSsd);
  EXPECT_FALSE(result.activeProbeRan);
  EXPECT_FALSE(result.directIoUsed);
  EXPECT_EQ(result.writeIops, 0);
  EXPECT_EQ(result.readIops, 0);
}

TEST(DiskProbeTest, runsActiveIopsProbe) {
  auto root = std::filesystem::temp_directory_path() /
      "bolt_bm_active_disk_probe";
  std::filesystem::remove_all(root);
  DiskProbeConfig config;
  config.directory = root.string();
  config.duration = std::chrono::milliseconds(50);
  config.fallbackKind = DiskKind::kHdd;

  auto result = ProbeDisk(config);

  EXPECT_TRUE(result.activeProbeRan);
  EXPECT_TRUE(result.directIoUsed);
  EXPECT_NE(result.kind, DiskKind::kUnknown);
  EXPECT_GT(result.writeIops, 0);
  EXPECT_GT(result.readIops, 0);
  EXPECT_EQ(result.targetP95LatencyUs, TargetP95LatencyForDisk(result.kind));
}

} // namespace
} // namespace bytedance::bolt::memory::bm
