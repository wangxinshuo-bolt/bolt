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

#include "bolt/type/StringViewBase.h"

namespace bytedance::bolt {

namespace {
template <size_t PREFIX_LEN>
  requires(PREFIX_LEN == 4 || PREFIX_LEN == 12)
int32_t linearSearchSimple(
    StringViewBase<PREFIX_LEN> key,
    const StringViewBase<PREFIX_LEN>* strings,
    const int32_t* indices,
    int32_t numStrings) {
  if (indices) {
    for (auto i = 0; i < numStrings; ++i) {
      if (strings[indices[i]] == key) {
        return i;
      }
    }
  } else {
    for (auto i = 0; i < numStrings; ++i) {
      if (strings[i] == key) {
        return i;
      }
    }
  }
  return -1;
}
} // namespace

template <size_t PREFIX_LEN>
  requires(PREFIX_LEN == 4 || PREFIX_LEN == 12)
int32_t StringViewBase<PREFIX_LEN>::linearSearch(
    StringViewBase<PREFIX_LEN> key,
    const StringViewBase<PREFIX_LEN>* strings,
    const int32_t* indices,
    int32_t numStrings) {
  return linearSearchSimple(key, strings, indices, numStrings);
}

template int32_t StringViewBase<4>::linearSearch(
    StringViewBase<4> key,
    const StringViewBase<4>* strings,
    const int32_t* indices,
    int32_t numStrings);

template int32_t StringViewBase<12>::linearSearch(
    StringViewBase<12> key,
    const StringViewBase<12>* strings,
    const int32_t* indices,
    int32_t numStrings);

} // namespace bytedance::bolt
