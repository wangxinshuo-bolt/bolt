/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

// Umbrella header for the BufferManager type system. Prefer including the
// themed headers directly in new code; this file exists so existing
// downstream includes continue to compile.
#include "bolt/common/memory/bm/BufferManagerConfig.h"
#include "bolt/common/memory/bm/EvictionTypes.h"
#include "bolt/common/memory/bm/MemoryTypes.h"
#include "bolt/common/memory/bm/Metrics.h"
#include "bolt/common/memory/bm/SpillTypes.h"
