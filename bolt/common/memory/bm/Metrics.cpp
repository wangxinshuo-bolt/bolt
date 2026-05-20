/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/Metrics.h"

namespace bytedance::bolt::memory::bm {
namespace {

class NoOpCounter final : public Counter {
 public:
  void Add(uint64_t /*value*/) override {}
};

class NoOpGauge final : public Gauge {
 public:
  void Set(int64_t /*value*/) override {}
  void Add(int64_t /*delta*/) override {}
};

class NoOpHistogram final : public Histogram {
 public:
  void Observe(double /*value*/) override {}
};

class NoOpRegistry final : public MetricsRegistry {
 public:
  Counter& GetCounter(
      std::string_view /*name*/,
      std::string_view /*labels*/) override {
    return counter_;
  }

  Gauge& GetGauge(
      std::string_view /*name*/,
      std::string_view /*labels*/) override {
    return gauge_;
  }

  Histogram& GetHistogram(
      std::string_view /*name*/,
      std::string_view /*labels*/) override {
    return histogram_;
  }

 private:
  NoOpCounter counter_;
  NoOpGauge gauge_;
  NoOpHistogram histogram_;
};

} // namespace

MetricsRegistry& NoOpMetricsRegistry() {
  static NoOpRegistry instance;
  return instance;
}

} // namespace bytedance::bolt::memory::bm
