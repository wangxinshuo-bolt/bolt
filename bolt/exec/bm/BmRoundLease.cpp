#include "bolt/exec/bm/BmRowContainer.h"

namespace bytedance::bolt::exec::bm {

BmRoundLease::BmRoundLease(
    BmRowContainer* container,
    PartitionId partition,
    uint64_t generation)
    : container_(container), partition_(partition), generation_(generation) {}

BmRoundLease::~BmRoundLease() noexcept {
  if (container_ != nullptr) {
    try {
      container_->releaseRoundLease(*this);
    } catch (...) {
    }
  }
}

BmRoundLease::BmRoundLease(BmRoundLease&& other) noexcept
    : container_(other.container_),
      partition_(other.partition_),
      generation_(other.generation_) {
  other.container_ = nullptr;
}

BmRoundLease& BmRoundLease::operator=(BmRoundLease&& other) noexcept {
  if (this != &other) {
    if (container_ != nullptr) {
      container_->releaseRoundLease(*this);
    }
    container_ = other.container_;
    partition_ = other.partition_;
    generation_ = other.generation_;
    other.container_ = nullptr;
  }
  return *this;
}

} // namespace bytedance::bolt::exec::bm
