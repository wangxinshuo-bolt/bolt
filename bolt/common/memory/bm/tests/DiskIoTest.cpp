/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <fcntl.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/DiskIo.h"
#include "bolt/common/memory/bm/DiskProbe.h"
#include "bolt/common/memory/bm/SpillStore.h"

namespace bytedance::bolt::memory::bm {
namespace {

std::filesystem::path tempPath(const std::string& name) {
  auto path = std::filesystem::temp_directory_path() / name;
  std::filesystem::remove(path);
  return path;
}

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

void assertRoundTrip(DiskIoEngine& engine, const std::string& name) {
  const auto path = tempPath(name);
  int fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0644);
  ASSERT_GE(fd, 0);

  std::vector<uint8_t> input(8192);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<uint8_t>(i % 251);
  }

  DiskIoRequest write;
  write.op = DiskIoOp::kWrite;
  write.priority = DiskIoPriority::kLow;
  write.fd = fd;
  write.buffer = input.data();
  write.size = input.size();
  write.offset = 0;
  write.userData = 10;
  auto writeDone = engine.Execute(write);
  ASSERT_EQ(writeDone.op, DiskIoOp::kWrite);
  ASSERT_EQ(writeDone.priority, DiskIoPriority::kLow);
  ASSERT_EQ(writeDone.userData, 10);
  ASSERT_EQ(writeDone.result, input.size());

  std::vector<uint8_t> output(input.size());
  DiskIoRequest read;
  read.op = DiskIoOp::kRead;
  read.priority = DiskIoPriority::kHigh;
  read.fd = fd;
  read.buffer = output.data();
  read.size = output.size();
  read.offset = 0;
  read.userData = 12;
  auto readDone = engine.Execute(read);
  ASSERT_EQ(readDone.op, DiskIoOp::kRead);
  ASSERT_EQ(readDone.priority, DiskIoPriority::kHigh);
  ASSERT_EQ(readDone.userData, 12);
  ASSERT_EQ(readDone.result, output.size());
  ASSERT_EQ(output, input);

  ::close(fd);
  std::filesystem::remove(path);
}

DiskIoConfig syncConfig() {
  DiskIoConfig config;
  config.backend = DiskIoBackend::kSync;
  config.initialQueueDepth = 4;
  config.minQueueDepth = 1;
  config.maxQueueDepth = 16;
  return config;
}

} // namespace

TEST(DiskIoTest, syncEngineRoundTripsReadWrite) {
  SyncDiskIoEngine engine;
  assertRoundTrip(engine, "bolt_bm_sync_disk_io_test.bin");
}

TEST(DiskIoTest, uringEngineRoundTripsReadWrite) {
  std::unique_ptr<UringDiskIoEngine> engine;
  try {
    engine = std::make_unique<UringDiskIoEngine>(8);
  } catch (const std::exception& e) {
    GTEST_SKIP() << "io_uring is unavailable in this environment: "
                 << e.what();
  }
  assertRoundTrip(*engine, "bolt_bm_uring_disk_io_test.bin");
}

TEST(DiskIoTest, adaptiveQueueDepthDropsOnHighLatency) {
  auto config = syncConfig();
  config.initialQueueDepth = 8;
  config.maxQueueDepth = 16;
  config.targetP95LatencyUs = 100;
  AdaptiveQueueDepth depth(config);

  DiskIoCompletion completion;
  completion.op = DiskIoOp::kRead;
  completion.result = 4096;
  completion.latencyUs = 1000;
  for (int i = 0; i < 8; ++i) {
    depth.Observe(completion);
  }
  ASSERT_LT(depth.Limit(), 8);
}

TEST(DiskIoTest, diskProbeHonorsForcedKind) {
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

TEST(DiskIoTest, diskProbeCanUseFallbackWithoutActiveProbe) {
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

TEST(DiskIoTest, diskProbeRunsActiveIopsProbe) {
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

TEST(DiskIoTest, schedulerUsesWeightedPriorityOrder) {
  auto engine = std::make_unique<RecordingDiskIoEngine>();
  auto* raw = engine.get();
  DiskIoScheduler scheduler(std::move(engine), syncConfig());

  std::array<uint8_t, 1> a{1};
  std::array<uint8_t, 1> b{2};
  std::array<uint8_t, 1> c{3};
  std::vector<DiskIoRequest> requests;
  requests.push_back(DiskIoRequest{
      .op = DiskIoOp::kWrite,
      .priority = DiskIoPriority::kLow,
      .fd = 1,
      .buffer = a.data(),
      .size = a.size(),
      .userData = 1});
  requests.push_back(DiskIoRequest{
      .op = DiskIoOp::kWrite,
      .priority = DiskIoPriority::kMedium,
      .fd = 1,
      .buffer = b.data(),
      .size = b.size(),
      .userData = 2});
  requests.push_back(DiskIoRequest{
      .op = DiskIoOp::kWrite,
      .priority = DiskIoPriority::kHigh,
      .fd = 1,
      .buffer = c.data(),
      .size = c.size(),
      .userData = 3});

  scheduler.SubmitAndWait(requests);
  ASSERT_EQ(raw->requests.size(), 3);
  EXPECT_EQ(raw->requests[0].priority, DiskIoPriority::kHigh);
  EXPECT_EQ(raw->requests[1].priority, DiskIoPriority::kMedium);
  EXPECT_EQ(raw->requests[2].priority, DiskIoPriority::kLow);
}

TEST(DiskIoTest, spillStoreUsesProcessWideSchedulerPrioritiesWithoutFsync) {
  auto root = std::filesystem::temp_directory_path() /
      "bolt_bm_spill_store_disk_io_test";
  std::filesystem::remove_all(root);

  auto engine = std::make_unique<RecordingDiskIoEngine>();
  auto* raw = engine.get();
  DiskIoScheduler scheduler(std::move(engine), syncConfig());

  SpillStore store(
      SpillStoreConfig{.spillDir = root.string(),
                       .cleanupOnDestroy = true,
                       .diskProbe = DiskProbeResult{
                           DiskKind::kSsd,
                           10'000,
                           12'000,
                           TargetP95LatencyForDisk(DiskKind::kSsd),
                           true,
                           true},
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

TEST(DiskIoTest, smallSpillBlocksShareSizeClassSlabAndReuseSlots) {
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
                       .diskProbe = DiskProbeResult{
                           DiskKind::kSsd,
                           10'000,
                           12'000,
                           TargetP95LatencyForDisk(DiskKind::kSsd),
                           true,
                           true},
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

TEST(DiskIoTest, largeSpillBlockUsesDedicatedFile) {
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
                       .diskProbe = DiskProbeResult{
                           DiskKind::kSsd,
                           10'000,
                           12'000,
                           TargetP95LatencyForDisk(DiskKind::kSsd),
                           true,
                           true},
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

TEST(DiskIoTest, spillStoreCompressesByDefaultAndReadsBackLogicalBytes) {
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
                       .diskProbe = DiskProbeResult{
                           DiskKind::kSsd,
                           10'000,
                           12'000,
                           TargetP95LatencyForDisk(DiskKind::kSsd),
                           true,
                           true},
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

TEST(DiskIoTest, spillStoreFallsBackToRawWhenCompressionDoesNotSaveSpace) {
  auto root = std::filesystem::temp_directory_path() /
      "bolt_bm_raw_spill_fallback_test";
  std::filesystem::remove_all(root);

  SpillCompressionConfig compression;
  compression.enabled = true;
  compression.minSavingsRatio = 0.95;

  SpillStore store(
      SpillStoreConfig{.spillDir = root.string(),
                       .cleanupOnDestroy = true,
                       .diskProbe = DiskProbeResult{
                           DiskKind::kSsd,
                           10'000,
                           12'000,
                           TargetP95LatencyForDisk(DiskKind::kSsd),
                           true,
                           true},
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

} // namespace bytedance::bolt::memory::bm
