/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/TemporaryMemoryManager.h"

#include <algorithm>
#include <vector>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"

namespace bytedance::bolt::memory::bm {

TemporaryMemoryState::TemporaryMemoryState(
    TemporaryMemoryManager* manager,
    uint64_t id,
    TempMemoryRegisterOptions options)
    : manager_(manager), id_(id), options_(std::move(options)) {}

TemporaryMemoryState::~TemporaryMemoryState() {
  TemporaryMemoryManager* manager = nullptr;
  uint64_t id = 0;
  {
    std::lock_guard<std::mutex> l(mutex_);
    manager = manager_;
    id = id_;
    manager_ = nullptr;
  }
  if (manager != nullptr) {
    manager->Unregister(id);
  }
}

ByteCount TemporaryMemoryState::Reservation() const {
  std::lock_guard<std::mutex> l(mutex_);
  return invalidated_ ? 0 : decision_.reservation;
}

ReservationDecision TemporaryMemoryState::Decision() const {
  std::lock_guard<std::mutex> l(mutex_);
  if (invalidated_) {
    return ReservationDecision{0, true, false};
  }
  return decision_;
}

void TemporaryMemoryState::UpdateRemaining(ByteCount remainingBytes) {
  TemporaryMemoryManager* manager = nullptr;
  {
    std::lock_guard<std::mutex> l(mutex_);
    options_.estimatedRemainingBytes = remainingBytes;
    manager = manager_;
  }
  if (manager != nullptr) {
    manager->RecomputeBudgets();
  }
}

void TemporaryMemoryState::InvalidateForManagerDestruction() noexcept {
  std::lock_guard<std::mutex> l(mutex_);
  invalidated_ = true;
  manager_ = nullptr;
  decision_ = ReservationDecision{0, true, false};
}

TemporaryMemoryManager::TemporaryMemoryManager(BufferPool& pool) : pool_(pool) {}

TemporaryMemoryManager::~TemporaryMemoryManager() {
  InvalidateAllStatesForManagerDestruction();
}

std::shared_ptr<TemporaryMemoryState> TemporaryMemoryManager::Register(
    TempMemoryRegisterOptions options) {
  std::shared_ptr<TemporaryMemoryState> state;
  {
    std::lock_guard<std::mutex> l(mutex_);
    BOLT_USER_CHECK(!shuttingDown_, "TemporaryMemoryManager is shutting down");
    const auto id = nextStateId_++;
    state = std::shared_ptr<TemporaryMemoryState>(
        new TemporaryMemoryState(this, id, std::move(options)));
    states_[id] = state;
    BOLT_MEM_LOG(INFO) << "Registered BufferManager temporary memory state id="
                       << id;
  }
  RecomputeBudgets();
  return state;
}

void TemporaryMemoryManager::Unregister(uint64_t id) noexcept {
  {
    std::lock_guard<std::mutex> l(mutex_);
    states_.erase(id);
  }
  BOLT_MEM_LOG(INFO) << "Unregistered BufferManager temporary memory state id="
                     << id;
  RecomputeBudgets();
}

void TemporaryMemoryManager::RecomputeBudgets() {
  std::vector<std::shared_ptr<TemporaryMemoryState>> states;
  size_t activeOperators = 0;
  {
    std::lock_guard<std::mutex> l(mutex_);
    for (auto it = states_.begin(); it != states_.end();) {
      if (auto state = it->second.lock()) {
        states.push_back(std::move(state));
        ++it;
      } else {
        it = states_.erase(it);
      }
    }
    // Capture the active count under manager mutex so we never need to lock it
    // again while holding state mutex (avoids state -> manager lock inversion
    // versus Register/Unregister, per design doc §15).
    activeOperators = std::max<size_t>(states.size(), 1);
  }
  const auto snapshot = pool_.Snapshot();
  for (auto& state : states) {
    std::lock_guard<std::mutex> l(state->mutex_);
    state->decision_ =
        ComputeDecision(state->options_, snapshot, activeOperators);
  }
}

void TemporaryMemoryManager::InvalidateAllStatesForManagerDestruction()
    noexcept {
  std::vector<std::shared_ptr<TemporaryMemoryState>> states;
  {
    std::lock_guard<std::mutex> l(mutex_);
    shuttingDown_ = true;
    for (auto& [unused, weak] : states_) {
      if (auto state = weak.lock()) {
        states.push_back(std::move(state));
      }
    }
    states_.clear();
  }
  for (auto& state : states) {
    state->InvalidateForManagerDestruction();
  }
}

ReservationDecision TemporaryMemoryManager::ComputeDecision(
    const TempMemoryRegisterOptions& options,
    const BufferPoolSnapshot& snapshot,
    size_t activeOperators) {
  const auto active = std::max<size_t>(activeOperators, 1);
  const auto fairShare = snapshot.availableForOperators / active;
  ReservationDecision decision;
  decision.reservation = std::min(options.estimatedRemainingBytes, fairShare);
  decision.shouldExternalize =
      decision.reservation < options.minimumReservationBytes;
  decision.shouldWait =
      !decision.shouldExternalize && snapshot.availableForOperators == 0;
  return decision;
}

} // namespace bytedance::bolt::memory::bm
