/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/SpillTypes.h"

namespace bytedance::bolt::memory::bm {

const char* ToString(DiskKind kind) {
  switch (kind) {
    case DiskKind::kUnknown:
      return "unknown";
    case DiskKind::kHdd:
      return "hdd";
    case DiskKind::kSsd:
      return "ssd";
    case DiskKind::kNvme:
      return "nvme";
    case DiskKind::kNetworkFs:
      return "network_fs";
  }
  return "unknown";
}

} // namespace bytedance::bolt::memory::bm
