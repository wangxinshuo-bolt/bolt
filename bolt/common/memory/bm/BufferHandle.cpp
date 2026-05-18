/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/BufferHandle.h"

#include <mutex>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/BlockHandle.h"

namespace bytedance::bolt::memory::bm {

BufferHandle::BufferHandle(std::shared_ptr<BlockHandle> block, bool initialWrite)
    : block_(std::move(block)), initialWrite_(initialWrite) {}

BufferHandle::BufferHandle(BufferHandle&& other) noexcept
    : block_(std::move(other.block_)), initialWrite_(other.initialWrite_) {
  other.initialWrite_ = false;
}

BufferHandle& BufferHandle::operator=(BufferHandle&& other) noexcept {
  if (this != &other) {
    Reset();
    block_ = std::move(other.block_);
    initialWrite_ = other.initialWrite_;
    other.initialWrite_ = false;
  }
  return *this;
}

BufferHandle::~BufferHandle() {
  Reset();
}

ConstDataPtr BufferHandle::Data() const {
  BOLT_USER_CHECK_NOT_NULL(block_, "Invalid BufferHandle");
  std::lock_guard<std::mutex> l(block_->mutex_);
  return block_->DataLocked();
}

DataPtr BufferHandle::MutableData() {
  BOLT_USER_CHECK_NOT_NULL(block_, "Invalid BufferHandle");
  std::lock_guard<std::mutex> l(block_->mutex_);
  return block_->MutableDataLocked(initialWrite_);
}

ByteCount BufferHandle::Size() const {
  return block_ == nullptr ? 0 : block_->Size();
}

void BufferHandle::Reset() noexcept {
  if (block_ != nullptr) {
    block_->Unpin(initialWrite_);
    block_.reset();
  }
  initialWrite_ = false;
}

} // namespace bytedance::bolt::memory::bm
