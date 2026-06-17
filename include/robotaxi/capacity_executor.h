#ifndef ROBOTAXI_CAPACITY_EXECUTOR_H_
#define ROBOTAXI_CAPACITY_EXECUTOR_H_

#include <cstdint>

#include "robotaxi/types.h"

namespace robotaxi {

// 容量模式执行器指标快照。
// 说明：该指标仅用于容量压测阶段的趋势观测，不作为业务语义指标。
struct CapacityExecutorMetrics {
  std::uint64_t executed_tasks;
  std::uint64_t invalid_payload_tasks;
  std::uint64_t parsed_header_tasks;
  std::uint64_t total_payload_bytes;
  std::uint64_t checksum_accumulator;
};

// 容量模式执行器：
// 1) 不做外部 IO，不做回包；
// 2) 做轻量 payload 读取与头字段解析；
// 3) 维护聚合指标以支持性能测试观测。
void CapacityModeExecutor(const Task& task);

// 重置容量模式执行器指标。
void ResetCapacityExecutorMetrics();

// 获取容量模式执行器指标快照。
[[nodiscard]] CapacityExecutorMetrics GetCapacityExecutorMetrics();

}  // namespace robotaxi

#endif  // ROBOTAXI_CAPACITY_EXECUTOR_H_
