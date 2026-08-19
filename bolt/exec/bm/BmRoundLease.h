#pragma once

#include "bolt/exec/bm/BmRowContainerPublicTypes.h"

#include <cstdint>

namespace bytedance::bolt::exec::bm {

class BmRowContainer;
struct BmRowContainerTestPeer;

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
  BmRoundLease& operator=(BmRoundLease&& other) noexcept;

  PartitionId partition() const {
    return partition_;
  }

  uint64_t generation() const {
    return generation_;
  }

  bool active() const {
    return container_ != nullptr;
  }

 private:
  BmRoundLease(
      BmRowContainer* container,
      PartitionId partition,
      uint64_t generation);

  BmRowContainer* container_{nullptr};
  PartitionId partition_{kDefaultPartition};
  uint64_t generation_{0};

  friend class BmRowContainer;
  friend struct BmRowContainerTestPeer;
};

} // namespace bytedance::bolt::exec::bm
