#include "bolt/exec/bm/BmRowLayout.h"

#include <gtest/gtest.h>

namespace bytedance::bolt::exec::bm {
namespace {

TEST(BmStorageLinkTest, ComputesRowSizeWithoutBoltExec) {
  BmRowLayout layout(
      {BIGINT(), VARCHAR()},
      {false, false},
      1024);

  EXPECT_GT(layout.rowSize(), 0);
}

} // namespace
} // namespace bytedance::bolt::exec::bm
