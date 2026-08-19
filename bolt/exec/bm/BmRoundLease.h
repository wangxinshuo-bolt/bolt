#pragma once

#include "bolt/exec/bm/BmRowContainerPublicTypes.h"

#include <cstdint>
#include <memory>

namespace bytedance::bolt::exec::bm {

class BmRowContainer;
struct BmRowContainerTestPeer;

struct BmRoundLeaseState {
  BmRowContainer* owner{nullptr};
  PartitionId partition{kDefaultPartition};
  uint64_t generation{0};
  uint32_t activeLeaseCount{0};
  bool ownerAlive{true};
};

// Logical lease over one BM partition epoch. The owning container must outlive
// any active lease; the lease intentionally does not retain container
// ownership or add a second physical pin.
class BmRoundLease {
 public:
  BmRoundLease() = default;
  BmRoundLease(const BmRoundLease&) = delete;
  BmRoundLease& operator=(const BmRoundLease&) = delete;
  ~BmRoundLease() noexcept;
  BmRoundLease(BmRoundLease&& other) noexcept;
  BmRoundLease& operator=(BmRoundLease&& other);

  PartitionId partition() const {
    return partition_;
  }

  uint64_t generation() const {
    return generation_;
  }

  bool active() const {
    return state_ != nullptr;
  }

 private:
  BmRoundLease(std::shared_ptr<BmRoundLeaseState> state, uint64_t generation);
  void releaseForMoveAssignment();
  void releaseForDestructor() noexcept;

  std::shared_ptr<BmRoundLeaseState> state_;
  PartitionId partition_{kDefaultPartition};
  uint64_t generation_{0};

  friend class BmRowContainer;
  friend struct BmRowContainerTestPeer;
};

} // namespace bytedance::bolt::exec::bm
