/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/BufferPool.h"

#include <algorithm>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"

namespace bytedance::bolt::memory::bm {
namespace {

size_t tagIndex(MemoryTag tag) {
  return static_cast<size_t>(tag);
}

} // namespace

BufferPoolReservation::BufferPoolReservation(
    QuotaSink* sink,
    MemoryTag tag,
    ByteCount bytes,
    ReservationKind kind)
    : sink_(sink), tag_(tag), bytes_(bytes), kind_(kind) {}

BufferPoolReservation::~BufferPoolReservation() {
  Reset();
}

BufferPoolReservation::BufferPoolReservation(
    BufferPoolReservation&& other) noexcept
    : sink_(other.sink_),
      tag_(other.tag_),
      bytes_(other.bytes_),
      kind_(other.kind_) {
  other.sink_ = nullptr;
  other.bytes_ = 0;
}

BufferPoolReservation& BufferPoolReservation::operator=(
    BufferPoolReservation&& other) noexcept {
  if (this != &other) {
    Reset();
    sink_ = other.sink_;
    tag_ = other.tag_;
    bytes_ = other.bytes_;
    kind_ = other.kind_;
    other.sink_ = nullptr;
    other.bytes_ = 0;
  }
  return *this;
}

void BufferPoolReservation::Resize(ByteCount newBytes) {
  if (newBytes == bytes_) {
    return;
  }
  BOLT_USER_CHECK_NOT_NULL(sink_, "Cannot resize an empty reservation");
  if (newBytes < bytes_) {
    const auto delta = bytes_ - newBytes;
    sink_->Release(tag_, delta, kind_);
    bytes_ = newBytes;
    return;
  }
  auto extra = sink_->Reserve(tag_, newBytes - bytes_, kind_);
  bytes_ = newBytes;
  extra.sink_ = nullptr;
  extra.bytes_ = 0;
}

void BufferPoolReservation::Reset() noexcept {
  if (sink_ != nullptr && bytes_ != 0) {
    sink_->Release(tag_, bytes_, kind_);
  }
  sink_ = nullptr;
  bytes_ = 0;
}

BufferPool::BufferPool(BufferManagerConfig /*config*/) {}

BufferPoolReservation BufferPool::Reserve(
    MemoryTag tag,
    ByteCount bytes,
    ReservationKind kind) {
  BOLT_USER_CHECK_LT(tagIndex(tag), tagIndex(MemoryTag::kNumTags));
  if (bytes == 0) {
    return BufferPoolReservation(this, tag, 0, kind);
  }

  {
    std::lock_guard<std::mutex> l(mutex_);
    ReserveLocked(tag, bytes, kind);
    BOLT_MEM_LOG(INFO) << "BufferManager reserve bytes=" << bytes
                       << " tag=" << ToString(tag)
                       << " kind=" << ToString(kind)
                       << " used=" << usedTotalBytes_;
  }
  return BufferPoolReservation(this, tag, bytes, kind);
}

void BufferPool::Release(
    MemoryTag tag,
    ByteCount bytes,
    ReservationKind kind) noexcept {
  if (bytes == 0) {
    return;
  }
  std::lock_guard<std::mutex> l(mutex_);
  ReleaseLocked(tag, bytes, kind);
  BOLT_MEM_LOG(INFO) << "BufferManager release bytes=" << bytes
                     << " tag=" << ToString(tag)
                     << " kind=" << ToString(kind)
                     << " used=" << usedTotalBytes_;
}

BufferPoolSnapshot BufferPool::Snapshot() const {
  std::lock_guard<std::mutex> l(mutex_);
  BufferPoolSnapshot snapshot;
  snapshot.usedTotalBytes = usedTotalBytes_;
  snapshot.usedPinnedBytes = usedPinnedBytes_;
  snapshot.usedScratchBytes = usedScratchBytes_;
  snapshot.usedEmergencyScratchBytes = usedEmergencyScratchBytes_;
  return snapshot;
}

ByteCount BufferPool::GetMemoryUsage() const {
  std::lock_guard<std::mutex> l(mutex_);
  return usedTotalBytes_;
}

ByteCount BufferPool::GetMemoryUsage(MemoryTag tag) const {
  BOLT_USER_CHECK_LT(tagIndex(tag), tagIndex(MemoryTag::kNumTags));
  std::lock_guard<std::mutex> l(mutex_);
  return usedByTag_[tagIndex(tag)];
}

void BufferPool::ReserveLocked(
    MemoryTag tag,
    ByteCount bytes,
    ReservationKind kind) {
  usedTotalBytes_ += bytes;
  usedByTag_[tagIndex(tag)] += bytes;
  if (kind == ReservationKind::kPinned) {
    usedPinnedBytes_ += bytes;
  }
  if (kind == ReservationKind::kScratch ||
      kind == ReservationKind::kScratchEmergency) {
    usedScratchBytes_ += bytes;
  }
  if (kind == ReservationKind::kScratchEmergency) {
    usedEmergencyScratchBytes_ += bytes;
  }
}

void BufferPool::ReleaseLocked(
    MemoryTag tag,
    ByteCount bytes,
    ReservationKind kind) {
  usedTotalBytes_ -= std::min(usedTotalBytes_, bytes);
  auto& tagBytes = usedByTag_[tagIndex(tag)];
  tagBytes -= std::min(tagBytes, bytes);
  if (kind == ReservationKind::kPinned) {
    usedPinnedBytes_ -= std::min(usedPinnedBytes_, bytes);
  }
  if (kind == ReservationKind::kScratch ||
      kind == ReservationKind::kScratchEmergency) {
    usedScratchBytes_ -= std::min(usedScratchBytes_, bytes);
  }
  if (kind == ReservationKind::kScratchEmergency) {
    usedEmergencyScratchBytes_ -=
        std::min(usedEmergencyScratchBytes_, bytes);
  }
}

std::unique_ptr<AccountedMemory> AccountedMemory::Make(
    BufferPool& pool,
    MemoryPool& memoryPool,
    MemoryTag tag,
    ByteCount bytes,
    ReservationKind kind) {
  auto reservation = pool.Reserve(tag, bytes, kind);
  DataPtr data = nullptr;
  try {
    if (bytes != 0) {
      data = static_cast<DataPtr>(memoryPool.allocate(bytes));
    }
  } catch (...) {
    reservation.Reset();
    throw;
  }
  return std::unique_ptr<AccountedMemory>(
      new AccountedMemory(std::move(reservation), memoryPool, data, bytes));
}

AccountedMemory::AccountedMemory(
    BufferPoolReservation reservation,
    MemoryPool& memoryPool,
    DataPtr data,
    ByteCount bytes)
    : reservation_(std::move(reservation)),
      memoryPool_(&memoryPool),
      data_(data),
      bytes_(bytes) {}

AccountedMemory::~AccountedMemory() {
  if (data_ != nullptr) {
    memoryPool_->free(data_, bytes_);
  }
}

BufferAllocator::BufferAllocator(BufferPool& pool, MemoryPool& memoryPool)
    : pool_(pool), memoryPool_(memoryPool) {}

std::unique_ptr<AccountedMemory> BufferAllocator::Allocate(
    MemoryTag tag,
    ByteCount bytes,
    ReservationKind kind) {
  return AccountedMemory::Make(pool_, memoryPool_, tag, bytes, kind);
}

std::unique_ptr<AccountedMemory> BufferAllocator::AllocateScratch(
    MemoryTag tag,
    ByteCount bytes) {
  return Allocate(tag, bytes, ReservationKind::kScratch);
}

std::unique_ptr<AccountedMemory> BufferAllocator::AllocateEmergencyScratch(
    MemoryTag tag,
    ByteCount bytes) {
  return Allocate(tag, bytes, ReservationKind::kScratchEmergency);
}

} // namespace bytedance::bolt::memory::bm
