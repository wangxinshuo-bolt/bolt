/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <memory>

#include "bolt/common/memory/bm/Types.h"

namespace bytedance::bolt::memory::bm {

class BufferManager;
class BlockHandle;

// RAII pin into a BlockHandle. Owning a valid BufferHandle guarantees the
// referenced block is pinned and its bytes are accessible via Data() /
// MutableData(). Move-only: copy would force shared pin semantics that the
// design doc explicitly forbids (see §6.1).
//
// Two flavors exist:
//   * "initial write" handles -- returned by BufferManager::Allocate. They
//     allow MutableData() and seal the block on Reset/destruction, after
//     which the block becomes immutable.
//   * "reader" handles -- returned by BufferManager::Pin / BlockHandle::Pin.
//     They are read-only; MutableData() throws.
//
// Default-constructed handles are "invalid" (IsValid()==false) and act as
// no-ops on destruction. Reset() also leaves the handle invalid.
class BufferHandle {
 public:
  // Creates an invalid (empty) handle. IsValid() returns false; accessor
  // methods on an invalid handle throw BoltUserError.
  BufferHandle() = default;

  // Transfers ownership of the pin from 'other'. 'other' becomes invalid;
  // the underlying block's pin count is unchanged.
  BufferHandle(BufferHandle&& other) noexcept;

  // Releases this handle's pin (if any) and then takes ownership from
  // 'other'. 'other' becomes invalid. Self-assignment is a no-op.
  BufferHandle& operator=(BufferHandle&& other) noexcept;
  BufferHandle(const BufferHandle&) = delete;
  BufferHandle& operator=(const BufferHandle&) = delete;

  // Releases the held pin via BlockHandle::Unpin. Initial-write handles
  // additionally seal the block here. Never throws.
  ~BufferHandle();

  // Returns true iff this handle owns a live pin.
  bool IsValid() const {
    return block_ != nullptr;
  }

  // Returns a const view into the pinned block's bytes. The pointer remains
  // valid for the lifetime of this handle. Throws BoltUserError on an
  // invalid handle or on a block whose state has no resident bytes.
  ConstDataPtr Data() const;

  // Returns a mutable view into the pinned block's bytes. ONLY valid on the
  // initial-write handle returned by Allocate(); throws BoltUserError for
  // reader handles or after the block has been sealed.
  DataPtr MutableData();

  // Returns the logical block size in bytes. Returns 0 for an invalid
  // handle.
  ByteCount Size() const;

  // Returns the underlying shared BlockHandle so callers can keep it alive
  // beyond this pin (e.g. by re-pinning later through BufferManager::Pin).
  // The returned pointer is null iff IsValid()==false.
  const std::shared_ptr<BlockHandle>& Block() const {
    return block_;
  }

 private:
  friend class BufferManager;
  friend class BlockHandle;
  BufferHandle(std::shared_ptr<BlockHandle> block, bool initialWrite);

  // Releases the pin and turns this handle into an invalid one. Called by
  // operator=, the destructor, and explicit reset paths. Never throws.
  void Reset() noexcept;

  std::shared_ptr<BlockHandle> block_;
  bool initialWrite_{false};
};

} // namespace bytedance::bolt::memory::bm
