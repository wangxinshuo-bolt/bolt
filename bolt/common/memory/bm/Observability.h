/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <chrono>

#include "bolt/common/memory/bm/Metrics.h"

namespace bytedance::bolt::memory::bm {

inline MetricsRegistry& EffectiveMetricsRegistry(MetricsRegistry* metrics) {
  return metrics == nullptr ? NoOpMetricsRegistry() : *metrics;
}

class ScopedBmTimer {
 public:
  explicit ScopedBmTimer(Histogram& histogram)
      : histogram_(&histogram), start_(Clock::now()) {}

  ~ScopedBmTimer() {
    if (histogram_ == nullptr) {
      return;
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        Clock::now() - start_);
    histogram_->Observe(static_cast<double>(elapsed.count()));
  }

  ScopedBmTimer(const ScopedBmTimer&) = delete;
  ScopedBmTimer& operator=(const ScopedBmTimer&) = delete;

 private:
  using Clock = std::chrono::steady_clock;

  Histogram* histogram_{nullptr};
  Clock::time_point start_;
};

} // namespace bytedance::bolt::memory::bm
