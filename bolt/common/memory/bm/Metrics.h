/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <cstdint>
#include <string_view>

namespace bytedance::bolt::memory::bm {

// Pluggable metric interfaces. All BufferManager components accept
// `MetricsRegistry*`; nullptr installs the NoOp implementation. Instruments
// returned from MetricsRegistry are owned by the registry and must outlive
// every component that holds a reference – do NOT cache instruments across
// registry destruction.

// Monotonic uint64 counter. Add(0) is a legal no-op. Implementations must
// be thread-safe; concurrent Add() calls are guaranteed to accumulate
// without loss.
class Counter {
 public:
  virtual ~Counter() = default;
  // Increments the counter by 'value'. value==0 is a no-op (still legal).
  virtual void Add(uint64_t value = 1) = 0;
};

// Latest-snapshot int64 gauge. Set() overrides the current value, Add()
// adjusts it atomically. Implementations must be thread-safe.
class Gauge {
 public:
  virtual ~Gauge() = default;
  // Replaces the current value with 'value'.
  virtual void Set(int64_t value) = 0;
  // Adjusts the value by 'delta' atomically (delta may be negative).
  virtual void Add(int64_t delta) = 0;
};

// Histogram of double observations. The exact bucket layout is delegated to
// the registry implementation; from a black-box perspective, every Observe()
// records exactly one sample.
class Histogram {
 public:
  virtual ~Histogram() = default;
  // Records exactly one observation of 'value'.
  virtual void Observe(double value) = 0;
};

// Registry that vends instruments by (name, labels) pairs. Implementations
// must dedupe instruments per pair so concurrent callers see the same
// underlying counter/gauge/histogram. Returned references are valid for the
// lifetime of the registry.
class MetricsRegistry {
 public:
  virtual ~MetricsRegistry() = default;
  // Returns a Counter for 'name'/'labels'. Repeated calls with the same
  // (name, labels) must return references aliasing the same instrument.
  virtual Counter& GetCounter(
      std::string_view name,
      std::string_view labels) = 0;
  // Returns a Gauge for 'name'/'labels' with the same dedup contract.
  virtual Gauge& GetGauge(std::string_view name, std::string_view labels) = 0;
  // Returns a Histogram for 'name'/'labels' with the same dedup contract.
  virtual Histogram& GetHistogram(
      std::string_view name,
      std::string_view labels) = 0;
};

// Returns a process-wide thread-safe NoOp registry. Every Get* call returns
// the same singleton instrument (a NoOp); Add()/Observe()/Set() do nothing
// but are still safe to call. BufferManager components fall back to this
// registry when their config supplies metrics == nullptr.
MetricsRegistry& NoOpMetricsRegistry();

} // namespace bytedance::bolt::memory::bm
