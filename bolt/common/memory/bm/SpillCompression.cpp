/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/SpillCompression.h"

#include <zstd.h>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::memory::bm {

PreparedSpillPayload PrepareSpillPayload(
    const SpillCompressionConfig& config,
    ConstDataPtr src,
    ByteCount bytes) {
  PreparedSpillPayload payload;
  payload.data = src;
  payload.storedBytes = bytes;
  if (!config.enabled || config.codec != SpillCompressionCodec::kZstd ||
      bytes < config.minBytes) {
    return payload;
  }

  const auto bound = ZSTD_compressBound(static_cast<size_t>(bytes));
  payload.compressed.resize(bound);
  const auto compressedBytes = ZSTD_compress(
      payload.compressed.data(),
      payload.compressed.size(),
      src,
      static_cast<size_t>(bytes),
      config.level);
  if (ZSTD_isError(compressedBytes)) {
    payload.compressed.clear();
    return payload;
  }

  const auto requiredSavings =
      static_cast<double>(bytes) * config.minSavingsRatio;
  if (compressedBytes >= bytes ||
      static_cast<double>(bytes - compressedBytes) < requiredSavings) {
    payload.compressed.clear();
    return payload;
  }

  payload.compressed.resize(compressedBytes);
  payload.data = payload.compressed.data();
  payload.storedBytes = compressedBytes;
  payload.codec = SpillCompressionCodec::kZstd;
  return payload;
}

void DecompressSpillPayload(
    const SpillLocation& location,
    ConstDataPtr compressed,
    DataPtr dst) {
  BOLT_USER_CHECK_NOT_NULL(compressed, "Cannot decompress a null spill buffer");
  BOLT_USER_CHECK_NOT_NULL(dst, "Cannot decompress spill into a null buffer");
  BOLT_USER_CHECK(
      location.compressionCodec == SpillCompressionCodec::kZstd,
      "Unsupported spill compression codec {}",
      static_cast<int>(location.compressionCodec));

  const auto decompressed = ZSTD_decompress(
      dst,
      static_cast<size_t>(location.logicalBytes),
      compressed,
      static_cast<size_t>(location.storedBytes));
  BOLT_USER_CHECK(
      !ZSTD_isError(decompressed),
      "Failed to decompress spill file {}: {}",
      location.path,
      ZSTD_getErrorName(decompressed));
  BOLT_USER_CHECK_EQ(
      decompressed,
      location.logicalBytes,
      "Decompressed spill size mismatch for {}: got {}, expected {}",
      location.path,
      decompressed,
      location.logicalBytes);
}

} // namespace bytedance::bolt::memory::bm
