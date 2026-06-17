#include "robotaxi/capacity_executor.h"

#include <cstdint>
#include <vector>

#include "gtest/gtest.h"

namespace robotaxi {
namespace {

TEST(CapacityExecutorTest, RecordsInvalidPayloadWhenPayloadIsNull) {
  ResetCapacityExecutorMetrics();

  const Task task{
      .task_id = 1,
      .payload = nullptr,
      .payload_size = 0,
      .response_handle = nullptr,
      .executor = &CapacityModeExecutor,
  };

  task.executor(task);

  const CapacityExecutorMetrics m = GetCapacityExecutorMetrics();
  EXPECT_EQ(m.executed_tasks, 1U);
  EXPECT_EQ(m.invalid_payload_tasks, 1U);
  EXPECT_EQ(m.parsed_header_tasks, 0U);
  EXPECT_EQ(m.total_payload_bytes, 0U);
}

TEST(CapacityExecutorTest, ParsesHeaderAndAccumulatesChecksum) {
  ResetCapacityExecutorMetrics();

  // [op_type=0x12][sequence=0x01020304][body=0xAA,0xBB]
  const std::vector<std::uint8_t> payload{0x12, 0x01, 0x02, 0x03, 0x04, 0xAA, 0xBB};
  const Task task{
      .task_id = 2,
      .payload = payload.data(),
      .payload_size = static_cast<std::uint32_t>(payload.size()),
      .response_handle = nullptr,
      .executor = &CapacityModeExecutor,
  };

  task.executor(task);

  const CapacityExecutorMetrics m = GetCapacityExecutorMetrics();
  EXPECT_EQ(m.executed_tasks, 1U);
  EXPECT_EQ(m.invalid_payload_tasks, 0U);
  EXPECT_EQ(m.parsed_header_tasks, 1U);
  EXPECT_EQ(m.total_payload_bytes, payload.size());
  EXPECT_NE(m.checksum_accumulator, 0U);
}

}  // namespace
}  // namespace robotaxi
