#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

#include <exception>

namespace bytedance::bolt::exec::bm {

BmRoundLease::BmRoundLease(
    std::shared_ptr<BmRoundLeaseState> state,
    uint64_t generation)
    : state_(std::move(state)),
      partition_(state_ != nullptr ? state_->partition : kDefaultPartition),
      generation_(generation) {}

BmRoundLease::~BmRoundLease() noexcept {
  releaseForDestructor();
}

BmRoundLease::BmRoundLease(BmRoundLease&& other) noexcept
    : state_(std::move(other.state_)),
      partition_(other.partition_),
      generation_(other.generation_) {
  other.partition_ = kDefaultPartition;
  other.generation_ = 0;
}

BmRoundLease& BmRoundLease::operator=(BmRoundLease&& other) {
  if (this != &other) {
    releaseForMoveAssignment();
    state_ = std::move(other.state_);
    partition_ = other.partition_;
    generation_ = other.generation_;
    other.partition_ = kDefaultPartition;
    other.generation_ = 0;
  }
  return *this;
}

void BmRoundLease::releaseForMoveAssignment() {
  if (state_ == nullptr) {
    return;
  }
  if (state_->ownerAlive) {
    state_->owner->releaseRoundLease(*this);
    return;
  }
  BOLT_FAIL("Cannot move-assign from a lease whose owner has been destroyed");
}

void BmRoundLease::releaseForDestructor() noexcept {
  if (state_ == nullptr) {
    return;
  }
  if (state_->ownerAlive) {
    try {
      state_->owner->releaseRoundLease(*this);
    } catch (...) {
      std::terminate();
    }
    return;
  }

  if (generation_ != state_->generation || state_->activeLeaseCount != 1) {
    std::terminate();
  }
  --state_->activeLeaseCount;
  ++state_->generation;
  state_.reset();
  partition_ = kDefaultPartition;
  generation_ = 0;
}

} // namespace bytedance::bolt::exec::bm
