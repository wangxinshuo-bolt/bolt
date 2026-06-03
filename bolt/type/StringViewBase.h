/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is released under the Apache License 2.0.
 * --------------------------------------------------------------------------
 */

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <ostream>
#include <string>
#include <string_view>

#include <folly/FBString.h>
#include <folly/Format.h>
#include <folly/Portability.h>
#include <folly/Range.h>
#include <folly/dynamic.h>

#include <fmt/format.h>

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt {

template <size_t PREFIX_LEN>
  requires(PREFIX_LEN == 4 || PREFIX_LEN == 12)
class StringViewBase {
 public:
  enum CompareOp { LT, LE, GT, GE };

  using value_type = char;
  using This = StringViewBase<PREFIX_LEN>;

  static constexpr size_t kPrefixSize = PREFIX_LEN * sizeof(char);
  static constexpr size_t kInlineRemainderSize = sizeof(const char*);
  static constexpr size_t kInlineSize = kPrefixSize + kInlineRemainderSize;

  StringViewBase() = default;

  StringViewBase(const char* data, int32_t len) {
    set(data, len);
  }

  void set(const char* data, int32_t len) {
    BOLT_CHECK_GE(len, 0);
    BOLT_DCHECK(data || len == 0);

    size_ = len;
    if (isInline()) {
      std::memset(prefix_, 0, sizeof(prefix_));
      std::memset(value_.inlined, 0, sizeof(value_.inlined));
      if (size_ == 0) {
        return;
      }

      const auto prefixBytes = std::min<size_t>(size_, kPrefixSize);
      std::memcpy(prefix_, data, prefixBytes);
      if (size_ > kPrefixSize) {
        std::memcpy(value_.inlined, data + kPrefixSize, size_ - kPrefixSize);
      }
    } else {
      std::memcpy(prefix_, data, kPrefixSize);
      value_.data = data;
    }
  }

  static This makeInline(std::string str) {
    BOLT_DCHECK(isInline(str.size()));
    return This{str};
  }

  /* implicit */ StringViewBase(const char* data)
      : StringViewBase(data, strlen(data)) {}

  explicit StringViewBase(const folly::fbstring& value)
      : StringViewBase(value.data(), value.size()) {}
  explicit StringViewBase(folly::fbstring&& value) = delete;

  explicit StringViewBase(const std::string& value)
      : StringViewBase(value.data(), value.size()) {}
  explicit StringViewBase(std::string&& value) = delete;

  explicit StringViewBase(std::string_view value)
      : StringViewBase(value.data(), value.size()) {}

  FOLLY_ALWAYS_INLINE bool isInline() const {
    return isInline(size_);
  }

  FOLLY_ALWAYS_INLINE static constexpr bool isInline(uint32_t size) {
    return size <= kInlineSize;
  }

  const char* data() && = delete;
  const char* data() const& {
    return isInline() ? prefix_ : value_.data;
  }

  const char* fastData() const& {
    return value_.data;
  }

  size_t size() const {
    return size_;
  }

  size_t capacity() const {
    return size_;
  }

  friend std::ostream& operator<<(std::ostream& os, const This& stringView) {
    os.write(stringView.data(), stringView.size());
    return os;
  }

  bool operator==(const This& other) const {
    if (size_ != other.size_) {
      return false;
    }
    if constexpr (kPrefixSize == 12) {
      uint64_t prefix = longPrefixAsInt64();
      uint64_t otherPrefix = other.longPrefixAsInt64();
      if (prefix != otherPrefix) {
        return false;
      }
    }
    uint32_t prefix = shortPrefixAsInt();
    uint32_t otherPrefix = other.shortPrefixAsInt();
    if (prefix != otherPrefix) {
      return false;
    }
    if (size_ <= kPrefixSize) {
      return true;
    }
    if (isInline()) {
      uint64_t inlined = inlinedAsInt64();
      uint64_t otherInlined = other.inlinedAsInt64();
      return inlined == otherInlined;
    }
    return std::memcmp(fastData(), other.fastData(), size_) == 0;
  }

  bool operator!=(const This& other) const {
    return !(*this == other);
  }

  // Just for compatibility.
  int32_t compare(const This& other) const {
    if constexpr (kPrefixSize == 12) {
      uint64_t prefix = longPrefixAsInt64();
      uint64_t otherPrefix = other.longPrefixAsInt64();
      if (prefix != otherPrefix) {
        if constexpr (folly::kIsLittleEndian) {
          prefix = __builtin_bswap64(prefix);
          otherPrefix = __builtin_bswap64(otherPrefix);
        }
        return prefix < otherPrefix ? -1 : 1;
      }
    }
    uint32_t prefix = shortPrefixAsInt();
    uint32_t otherPrefix = other.shortPrefixAsInt();
    if (prefix != otherPrefix) {
      // The result is decided on prefix. The shorter will be less
      // because the prefix is padded with zeros.
      if constexpr (folly::kIsLittleEndian) {
        prefix = __builtin_bswap32(prefix);
        otherPrefix = __builtin_bswap32(otherPrefix);
      }
      return prefix < otherPrefix ? -1 : 1;
    }
    int32_t size = std::min(size_, other.size_) - kPrefixSize;
    if (size <= 0) {
      // One ends within the prefix.
      return size_ - other.size_;
    }
    if (size <= kInlineSize && isInline() && other.isInline()) {
      uint64_t inlined = inlinedAsInt64();
      uint64_t otherInlined = other.inlinedAsInt64();
      if constexpr (folly::kIsLittleEndian) {
        inlined = __builtin_bswap64(inlined);
        otherInlined = __builtin_bswap64(otherInlined);
      }
      if (inlined == otherInlined) {
        return size_ - other.size_;
      }
      return (inlined < otherInlined) ? -1 : 1;
    }
    int32_t result =
        memcmp(fastData() + kPrefixSize, other.fastData() + kPrefixSize, size);
    return (result != 0) ? result : size_ - other.size_;
  }

  template <CompareOp op>
  FOLLY_ALWAYS_INLINE bool compare(const This& other) const {
    const auto compareByOp = [](const auto lhs, const auto rhs) noexcept {
      if constexpr (op == LT) {
        return lhs < rhs;
      }
      if constexpr (op == LE) {
        return lhs <= rhs;
      }
      if constexpr (op == GE) {
        return lhs >= rhs;
      }
      return lhs > rhs;
    };

    if constexpr (kPrefixSize == 12) {
      uint64_t prefix = longPrefixAsInt64();
      uint64_t otherPrefix = other.longPrefixAsInt64();
      if (prefix != otherPrefix) {
        if constexpr (folly::kIsLittleEndian) {
          prefix = __builtin_bswap64(prefix);
          otherPrefix = __builtin_bswap64(otherPrefix);
        }
        return compareByOp(prefix, otherPrefix);
      }
    }
    uint32_t prefix = shortPrefixAsInt();
    uint32_t otherPrefix = other.shortPrefixAsInt();
    if (prefix != otherPrefix) {
      // The result is decided on prefix. The shorter will be less
      // because the prefix is padded with zeros.
      if constexpr (folly::kIsLittleEndian) {
        prefix = __builtin_bswap32(prefix);
        otherPrefix = __builtin_bswap32(otherPrefix);
      }
      return compareByOp(prefix, otherPrefix);
    }

    int32_t size = std::min(size_, other.size_) - kPrefixSize;
    if (size <= 0) {
      // One ends within the prefix.
      return compareByOp(size_, other.size_);
    }
    if (size <= kInlineSize && isInline() && other.isInline()) {
      uint64_t inlined = inlinedAsInt64();
      uint64_t otherInlined = other.inlinedAsInt64();
      if constexpr (folly::kIsLittleEndian) {
        inlined = __builtin_bswap64(inlined);
        otherInlined = __builtin_bswap64(otherInlined);
      }
      if (inlined == otherInlined) {
        return compareByOp(size_, other.size_);
      }
      return compareByOp(inlined, otherInlined);
    }

    int32_t result =
        memcmp(fastData() + kPrefixSize, other.fastData() + kPrefixSize, size);

    if (result != 0) {
      return compareByOp(result, 0);
    }
    return compareByOp(size_ , other.size_);
  }

  bool operator<(const This& other) const {
    return compare<LT>(other);
  }

  bool operator<=(const This& other) const {
    return compare<LE>(other);
  }

  bool operator>(const This& other) const {
    return compare<GT>(other);
  }

  bool operator>=(const This& other) const {
    return compare<GE>(other);
  }

  operator folly::StringPiece() && = delete;
  operator folly::StringPiece() const& {
    return folly::StringPiece(data(), size());
  }

  operator std::string() const {
    return std::string(data(), size());
  }

  std::string str() const {
    return *this;
  }

  std::string getString() const {
    return *this;
  }

  std::string materialize() const {
    return *this;
  }

  operator folly::dynamic() && = delete;
  operator folly::dynamic() const& {
    return folly::dynamic(folly::StringPiece(data(), size()));
  }

  operator std::string_view() && = delete;
  explicit operator std::string_view() const& {
    return std::string_view(data(), size());
  }

  const char* begin() && = delete;
  const char* begin() const& {
    return data();
  }

  const char* end() && = delete;
  const char* end() const& {
    return data() + size();
  }

  bool empty() const {
    return size() == 0;
  }

  static int32_t linearSearch(
      This key,
      const This* strings,
      const int32_t* indices,
      int32_t numStrings);

  const char* prefix() const {
    return prefix_;
  }

  const char* value() const {
    static constexpr uintptr_t kPointerMask = (uintptr_t{1} << 52) - 1;
    return isInline()
        ? nullptr
        : reinterpret_cast<const char*>(
              reinterpret_cast<uintptr_t>(value_.data) & kPointerMask);
  }

  int64_t offset() const noexcept {
    static constexpr uint64_t kOffsetFlag = uint64_t{1} << 63;
    static constexpr uint64_t kOffsetMask = ~kOffsetFlag;
    const auto offset = static_cast<uint64_t>(value_.offset);
    if (isInline() || (offset & kOffsetFlag) == 0) {
      return -1;
    }
    return offset & kOffsetMask;
  }

 private:
  inline int64_t sizeAndPrefixAsInt64() const {
    return reinterpret_cast<const int64_t*>(this)[0];
  }

  inline int64_t inlinedAsInt64() const {
    return reinterpret_cast<const int64_t*>(
        this)[(kPrefixSize + sizeof(uint32_t)) / sizeof(int64_t)];
  }

  int32_t shortPrefixAsInt() const {
    return *reinterpret_cast<const int32_t*>(
      reinterpret_cast<const char*>(&prefix_) + (kPrefixSize / sizeof(int64_t))  * sizeof(int64_t));
  }

  int32_t longPrefixAsInt64() const {
    return *reinterpret_cast<const int64_t*>(&prefix_);
  }

  uint64_t suffixPrefixAsInt64() const {
    return *reinterpret_cast<const uint64_t*>(
        reinterpret_cast<const char*>(&prefix_) + sizeof(uint32_t));
  }

  uint32_t size_{0};
  char prefix_[kPrefixSize]{0};
  union {
    char inlined[kInlineRemainderSize];
    const char* data;
    int64_t offset;
  } value_{.data = nullptr};
};

static_assert(sizeof(StringViewBase<4>) == 16);
static_assert(sizeof(StringViewBase<12>) == 24);

} // namespace bytedance::bolt

namespace std {
template <size_t PREFIX_LEN>
  requires(PREFIX_LEN == 4 || PREFIX_LEN == 12)
struct hash<::bytedance::bolt::StringViewBase<PREFIX_LEN>> {
  size_t operator()(
      const ::bytedance::bolt::StringViewBase<PREFIX_LEN> view) const {
    return bytedance::bolt::bits::hashBytes(1, view.data(), view.size());
  }
};
} // namespace std

namespace folly {
template <size_t PREFIX_LEN>
  requires(PREFIX_LEN == 4 || PREFIX_LEN == 12)
struct hasher<::bytedance::bolt::StringViewBase<PREFIX_LEN>> {
  size_t operator()(
      const ::bytedance::bolt::StringViewBase<PREFIX_LEN> view) const {
    return bytedance::bolt::bits::hashBytes(1, view.data(), view.size());
  }
};

} // namespace folly

namespace fmt {
template <size_t PREFIX_LEN>
  requires(PREFIX_LEN == 4 || PREFIX_LEN == 12)
struct formatter<bytedance::bolt::StringViewBase<PREFIX_LEN>>
    : private formatter<string_view> {
  using formatter<string_view>::parse;

  template <typename Context>
  typename Context::iterator format(
      bytedance::bolt::StringViewBase<PREFIX_LEN> s,
      Context& ctx) const {
    return formatter<string_view>::format(string_view{s.data(), s.size()}, ctx);
  }
};
} // namespace fmt
