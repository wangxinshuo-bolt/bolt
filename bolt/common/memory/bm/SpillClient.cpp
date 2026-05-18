/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/SpillClient.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/ProcessSpillService.h"

namespace bytedance::bolt::memory::bm {

SpillClient::SpillClient(
    ProcessSpillService* service,
    uint64_t clientId,
    SpillClientConfig config)
    : service_(service), clientId_(clientId), config_(std::move(config)) {}

SpillClient::~SpillClient() {
  if (service_ != nullptr) {
    service_->Scheduler().UnregisterClient(clientId_);
    service_->UnregisterClient(clientId_);
  }
}

EvictResult SpillClient::SubmitSpill(EvictionNode node) {
  if (service_ == nullptr) {
    return EvictResult{EvictResultKind::kFailed, 0};
  }
  node.clientId = clientId_;
  return service_->Scheduler().SubmitSpill(std::move(node));
}

bool SpillClient::WaitForProgress(
    ByteCount bytesNeeded,
    std::chrono::milliseconds timeout) {
  if (service_ == nullptr) {
    return false;
  }
  return service_->Scheduler().WaitForProgress(bytesNeeded, timeout);
}

SpillLocation SpillClient::Write(
    MemoryTag tag,
    ConstDataPtr src,
    ByteCount bytes) {
  BOLT_USER_CHECK_NOT_NULL(service_, "SpillClient has no service");
  service_->ChargeQuota(*this, bytes);
  try {
    auto location = service_->PickStore().Write(tag, src, bytes);
    return location;
  } catch (...) {
    service_->CreditQuota(*this, bytes);
    throw;
  }
}

void SpillClient::Read(
    const SpillLocation& location,
    DataPtr dst,
    ByteCount dstCapacity) {
  BOLT_USER_CHECK_NOT_NULL(service_, "SpillClient has no service");
  // The location contains its source path; SpillStore::Read does not need
  // the originating store. Use the first store as the read entry point.
  // SpillStore::Read is stateless w.r.t. which store wrote the file, since
  // it opens 'location.path' directly.
  service_->PickStore().Read(location, dst, dstCapacity);
}

void SpillClient::Release(const SpillLocation& location) noexcept {
  if (!location.Valid() || service_ == nullptr) {
    return;
  }
  try {
    service_->PickStore().Release(location);
  } catch (...) {
    // Release errors are already logged by SpillStore; never propagate.
  }
  service_->CreditQuota(*this, location.storedBytes);
}

} // namespace bytedance::bolt::memory::bm
