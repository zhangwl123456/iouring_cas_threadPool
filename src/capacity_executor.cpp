#include "robotaxi/capacity_executor.h"

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace robotaxi {
namespace {

constexpr std::size_t kCapacityHeaderBytes = 5;

std::atomic<std::uint64_t> g_executed_tasks{0};
std::atomic<std::uint64_t> g_invalid_payload_tasks{0};
std::atomic<std::uint64_t> g_parsed_header_tasks{0};
std::atomic<std::uint64_t> g_total_payload_bytes{0};
std::atomic<std::uint64_t> g_checksum_accumulator{0};

std::uint32_t ReadBe32(const std::uint8_t* data) {
  return (static_cast<std::uint32_t>(data[0]) << 24U) |
         (static_cast<std::uint32_t>(data[1]) << 16U) |
         (static_cast<std::uint32_t>(data[2]) << 8U) |
         static_cast<std::uint32_t>(data[3]);
}

}  // namespace

// CapacityModeExecutor：容量压测阶段的默认执行器。
// 设计目标：
// 1) 以轻量解析/校验模拟数据触达路径；
// 2) 禁止外部 IO 与回包，避免把非框架成本引入压测结果；
// 3) 通过原子聚合指标输出执行统计，支持吞吐与效率分析。
void CapacityModeExecutor(const Task& task) {
  g_executed_tasks.fetch_add(1, std::memory_order_relaxed);
  g_total_payload_bytes.fetch_add(task.payload_size, std::memory_order_relaxed);

  if (task.payload == nullptr || task.payload_size == 0U) {
    g_invalid_payload_tasks.fetch_add(1, std::memory_order_relaxed);
    return;
  }

  const auto* bytes = static_cast<const std::uint8_t*>(task.payload);

  // FNV-1a 轻量校验，用于模拟读取 payload 的 CPU 开销与数据触达路径。
  std::uint64_t checksum = 1469598103934665603ULL;
  for (std::uint32_t i = 0; i < task.payload_size; ++i) {
    checksum ^= static_cast<std::uint64_t>(bytes[i]);
    checksum *= 1099511628211ULL;
  }

  if (task.payload_size >= kCapacityHeaderBytes) {
    const std::uint8_t op_type = bytes[0];
    const std::uint32_t sequence = ReadBe32(bytes + 1);
    checksum ^= static_cast<std::uint64_t>(op_type);
    checksum ^= static_cast<std::uint64_t>(sequence);
    g_parsed_header_tasks.fetch_add(1, std::memory_order_relaxed);
  }

  g_checksum_accumulator.fetch_xor(checksum, std::memory_order_relaxed);
}

void ResetCapacityExecutorMetrics() {
  g_executed_tasks.store(0, std::memory_order_relaxed);
  g_invalid_payload_tasks.store(0, std::memory_order_relaxed);
  g_parsed_header_tasks.store(0, std::memory_order_relaxed);
  g_total_payload_bytes.store(0, std::memory_order_relaxed);
  g_checksum_accumulator.store(0, std::memory_order_relaxed);
}

CapacityExecutorMetrics GetCapacityExecutorMetrics() {
  return CapacityExecutorMetrics{
      .executed_tasks = g_executed_tasks.load(std::memory_order_relaxed),
      .invalid_payload_tasks = g_invalid_payload_tasks.load(std::memory_order_relaxed),
      .parsed_header_tasks = g_parsed_header_tasks.load(std::memory_order_relaxed),
      .total_payload_bytes = g_total_payload_bytes.load(std::memory_order_relaxed),
      .checksum_accumulator = g_checksum_accumulator.load(std::memory_order_relaxed),
  };
}

}  // namespace robotaxi
