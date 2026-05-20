/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <cstring>
#include <filesystem>
#include <vector>

#include <gtest/gtest.h>

#include "bolt/common/memory/bm/DiskIo.h"
#include "bolt/common/memory/bm/SpillStore.h"
#include "bolt/common/memory/bm/tests/BufferManagerTestUtil.h"

namespace bytedance::bolt::memory::bm {
namespace {

class RecordingDiskIoEngine : public DiskIoEngine {
 public:
  DiskIoCompletion Execute(const DiskIoRequest& request) override {
    requests.push_back(request);
    if (request.op == DiskIoOp::kWrite) {
      written.assign(
          static_cast<const uint8_t*>(request.buffer),
          static_cast<const uint8_t*>(request.buffer) + request.size);
      return DiskIoCompletion{
          request.op,
          request.priority,
          request.userData,
          static_cast<int64_t>(request.size),
          request.size,
          latencyUs};
    }
    if (request.op == DiskIoOp::kRead) {
      std::memcpy(request.buffer, written.data(), written.size());
      return DiskIoCompletion{
          request.op,
          request.priority,
          request.userData,
          static_cast<int64_t>(written.size()),
          request.size,
          latencyUs};
    }
    return DiskIoCompletion{
        request.op, request.priority, request.userData, 0, 0, latencyUs};
  }

  uint64_t latencyUs{10};
  std::vector<DiskIoRequest> requests;
  std::vector<uint8_t> written;
};

DiskProbeResult ssdProbe() {
  return DiskProbeResult{
      DiskKind::kSsd,
      10'000,
      12'000,
      true,
      true};
}

DiskIoConfig syncConfig() {
  DiskIoConfig config;
  config.backend = DiskIoBackend::kSync;
  config.initialQueueDepth = 4;
  config.minQueueDepth = 1;
  config.maxQueueDepth = 16;
  return config;
}

TEST(SpillStoreTest, usesSchedulerPrioritiesWithoutFsync) {
  auto root = std::filesystem::temp_directory_path() /
      "bolt_bm_spill_store_disk_io_test";
  std::filesystem::remove_all(root);

  auto engine = std::make_unique<RecordingDiskIoEngine>();
  auto* raw = engine.get();
  DiskIoScheduler scheduler(std::move(engine), syncConfig());

  SpillStore store(
      SpillStoreConfig{.spillDir = root.string(),
                       .cleanupOnDestroy = true,
                       .diskProbe = ssdProbe(),
                       .smallSpill = SmallSpillConfig{},
                       .compression = SpillCompressionConfig{}},
      nullptr,
      &scheduler);

  uint8_t payload[4] = {1, 2, 3, 4};
  auto location = store.Write(MemoryTag::kShuffle, payload, sizeof(payload));
  ASSERT_TRUE(location.Valid());
  ASSERT_EQ(raw->requests.size(), 1);
  EXPECT_EQ(raw->requests[0].op, DiskIoOp::kWrite);
  EXPECT_EQ(raw->requests[0].priority, DiskIoPriority::kLow);
  EXPECT_EQ(raw->requests[0].size, sizeof(payload));
  EXPECT_EQ(raw->requests[0].offset, 0);

  uint8_t readBack[4] = {};
  store.Read(location, readBack, sizeof(readBack));
  ASSERT_EQ(raw->requests.size(), 2);
  EXPECT_EQ(raw->requests[1].op, DiskIoOp::kRead);
  EXPECT_EQ(raw->requests[1].priority, DiskIoPriority::kHigh);
  EXPECT_EQ(raw->requests[1].size, sizeof(readBack));
  EXPECT_EQ(raw->requests[1].offset, 0);
  EXPECT_EQ(0, std::memcmp(payload, readBack, sizeof(payload)));

  store.Release(location);
}

TEST(SpillStoreTest, smallBlocksShareSizeClassSlabAndReuseSlots) {
  auto root = std::filesystem::temp_directory_path() /
      "bolt_bm_small_spill_slab_test";
  std::filesystem::remove_all(root);

  SmallSpillConfig small;
  small.enabled = true;
  small.dedicatedFileThresholdBytes = 1 << 20;
  small.slabFileBytes = 64 << 10;
  small.sizeClasses = {4 << 10, 8 << 10};
  SpillCompressionConfig compression;
  compression.enabled = false;

  SpillStore store(
      SpillStoreConfig{.spillDir = root.string(),
                       .cleanupOnDestroy = true,
                       .diskProbe = ssdProbe(),
                       .smallSpill = small,
                       .compression = compression});

  std::vector<uint8_t> a(1024, 11);
  std::vector<uint8_t> b(3000, 22);
  std::vector<uint8_t> c(7000, 33);

  auto locA = store.Write(MemoryTag::kShuffle, a.data(), a.size());
  auto locB = store.Write(MemoryTag::kShuffle, b.data(), b.size());
  auto locC = store.Write(MemoryTag::kShuffle, c.data(), c.size());

  ASSERT_TRUE(locA.smallSlot);
  ASSERT_TRUE(locB.smallSlot);
  ASSERT_TRUE(locC.smallSlot);
  EXPECT_EQ(locA.path, locB.path);
  EXPECT_NE(locA.offset, locB.offset);
  EXPECT_EQ(locA.slotBytes, 4 << 10);
  EXPECT_EQ(locB.slotBytes, 4 << 10);
  EXPECT_EQ(locC.slotBytes, 8 << 10);

  std::vector<uint8_t> outB(b.size());
  store.Read(locB, outB.data(), outB.size());
  EXPECT_EQ(outB, b);

  store.Release(locA);
  auto locD = store.Write(MemoryTag::kShuffle, a.data(), a.size());
  EXPECT_TRUE(locD.smallSlot);
  EXPECT_EQ(locD.path, locA.path);
  EXPECT_EQ(locD.offset, locA.offset);

  size_t regularFiles = 0;
  for (const auto& entry : std::filesystem::directory_iterator(root)) {
    if (entry.is_regular_file()) {
      ++regularFiles;
    }
  }
  EXPECT_EQ(regularFiles, 2);

  store.Release(locB);
  store.Release(locC);
  store.Release(locD);
}

TEST(SpillStoreTest, largeRawBlockUsesDedicatedFile) {
  auto root = std::filesystem::temp_directory_path() /
      "bolt_bm_dedicated_spill_test";
  std::filesystem::remove_all(root);

  SmallSpillConfig small;
  small.enabled = true;
  small.dedicatedFileThresholdBytes = 4 << 10;
  small.slabFileBytes = 64 << 10;
  small.sizeClasses = {4 << 10};
  SpillCompressionConfig compression;
  compression.enabled = false;

  SpillStore store(
      SpillStoreConfig{.spillDir = root.string(),
                       .cleanupOnDestroy = true,
                       .diskProbe = ssdProbe(),
                       .smallSpill = small,
                       .compression = compression});

  std::vector<uint8_t> payload((4 << 10) + 1, 44);
  auto location =
      store.Write(MemoryTag::kShuffle, payload.data(), payload.size());
  EXPECT_FALSE(location.smallSlot);
  EXPECT_EQ(location.offset, 0);
  EXPECT_EQ(location.slotBytes, 0);
  EXPECT_TRUE(std::filesystem::exists(location.path));

  store.Release(location);
  EXPECT_FALSE(std::filesystem::exists(location.path));
}

TEST(SpillStoreTest, compressesByDefaultAndReadsBackLogicalBytes) {
  auto root = std::filesystem::temp_directory_path() /
      "bolt_bm_compressed_spill_test";
  std::filesystem::remove_all(root);

  SmallSpillConfig small;
  small.enabled = true;
  small.dedicatedFileThresholdBytes = 1 << 20;
  small.slabFileBytes = 64 << 10;
  small.sizeClasses = {4 << 10, 8 << 10, 16 << 10};

  SpillStore store(
      SpillStoreConfig{.spillDir = root.string(),
                       .cleanupOnDestroy = true,
                       .diskProbe = ssdProbe(),
                       .smallSpill = small,
                       .compression = SpillCompressionConfig{}});

  std::vector<uint8_t> payload(64 << 10, 7);
  auto location =
      store.Write(MemoryTag::kShuffle, payload.data(), payload.size());

  EXPECT_EQ(location.compressionCodec, SpillCompressionCodec::kZstd);
  EXPECT_EQ(location.logicalBytes, payload.size());
  EXPECT_LT(location.storedBytes, location.logicalBytes);
  EXPECT_TRUE(location.smallSlot);
  EXPECT_LE(location.storedBytes, location.slotBytes);

  std::vector<uint8_t> readBack(payload.size());
  store.Read(location, readBack.data(), readBack.size());
  EXPECT_EQ(readBack, payload);

  store.Release(location);
}

TEST(SpillStoreTest, fallsBackToRawWhenCompressionDoesNotSaveSpace) {
  auto root = std::filesystem::temp_directory_path() /
      "bolt_bm_raw_spill_fallback_test";
  std::filesystem::remove_all(root);

  SpillCompressionConfig compression;
  compression.enabled = true;
  compression.minSavingsRatio = 0.95;

  SpillStore store(
      SpillStoreConfig{.spillDir = root.string(),
                       .cleanupOnDestroy = true,
                       .diskProbe = ssdProbe(),
                       .smallSpill = SmallSpillConfig{},
                       .compression = compression});

  std::vector<uint8_t> payload(32 << 10);
  uint32_t random = 0x12345678;
  for (size_t i = 0; i < payload.size(); ++i) {
    random ^= random << 13;
    random ^= random >> 17;
    random ^= random << 5;
    payload[i] = static_cast<uint8_t>(random);
  }

  auto location =
      store.Write(MemoryTag::kShuffle, payload.data(), payload.size());
  EXPECT_EQ(location.compressionCodec, SpillCompressionCodec::kNone);
  EXPECT_EQ(location.storedBytes, location.logicalBytes);

  std::vector<uint8_t> readBack(payload.size());
  store.Read(location, readBack.data(), readBack.size());
  EXPECT_EQ(readBack, payload);

  store.Release(location);
}

TEST(SpillStoreTest, recordsMetricsForWriteReadReleaseAndCompressionPath) {
  auto root = std::filesystem::temp_directory_path() /
      "bolt_bm_spill_store_metrics_test";
  std::filesystem::remove_all(root);

  test::RecordingMetricsRegistry metrics;
  SmallSpillConfig small;
  small.enabled = true;
  small.dedicatedFileThresholdBytes = 128 << 10;
  small.slabFileBytes = 64 << 10;
  small.sizeClasses = {4 << 10, 8 << 10};
  auto engine = std::make_unique<RecordingDiskIoEngine>();
  DiskIoScheduler scheduler(std::move(engine), syncConfig());

  SpillStore store(
      SpillStoreConfig{.spillDir = root.string(),
                       .cleanupOnDestroy = true,
                       .diskProbe = ssdProbe(),
                       .smallSpill = small,
                       .compression = SpillCompressionConfig{}},
      &metrics,
      &scheduler);

  std::vector<uint8_t> payload(32 << 10, 7);
  auto location =
      store.Write(MemoryTag::kShuffle, payload.data(), payload.size());
  ASSERT_TRUE(location.Valid());
  ASSERT_TRUE(location.smallSlot);
  ASSERT_LT(location.storedBytes, location.logicalBytes);

  std::vector<uint8_t> readBack(payload.size());
  store.Read(location, readBack.data(), readBack.size());
  store.Release(location);

  EXPECT_EQ(metrics.CounterValue("bm_spill_bytes_written", "disk=ssd"),
            payload.size());
  EXPECT_EQ(metrics.CounterValue("bm_spill_bytes_stored", "disk=ssd"),
            location.storedBytes);
  EXPECT_EQ(metrics.CounterValue("bm_spill_small_slot_total", "disk=ssd"), 1);
  EXPECT_EQ(metrics.CounterValue("bm_spill_compress_attempt_total", "disk=ssd"),
            1);
  EXPECT_EQ(metrics.CounterValue("bm_spill_compress_saved_bytes", "disk=ssd"),
            location.logicalBytes - location.storedBytes);
  EXPECT_EQ(metrics.CounterValue("bm_spill_bytes_read", "disk=ssd"),
            payload.size());
  EXPECT_EQ(metrics.CounterValue("bm_spill_release_total", "disk=ssd"), 1);
  EXPECT_EQ(metrics.HistogramCount("bm_spill_write_duration_us", "disk=ssd"),
            1);
  EXPECT_EQ(metrics.HistogramCount("bm_spill_read_duration_us", "disk=ssd"),
            1);
  EXPECT_EQ(metrics.HistogramCount("bm_spill_release_duration_us", "disk=ssd"),
            1);
}

} // namespace
} // namespace bytedance::bolt::memory::bm
