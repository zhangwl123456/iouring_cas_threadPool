#ifndef ROBOTAXI_RING_QUEUE_H_
#define ROBOTAXI_RING_QUEUE_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "robotaxi/status.h"
#include "robotaxi/types.h"

namespace robotaxi {

class RingQueue {
 public:
  virtual ~RingQueue() = default;

  virtual Status Enqueue(const Task& task, std::uint32_t timeout_ms) = 0;
  virtual Status Dequeue(Task* out_task, std::uint32_t timeout_ms) = 0;
  [[nodiscard]] virtual std::uint32_t Capacity() const = 0;
  [[nodiscard]] virtual std::uint32_t Size() const = 0;
  [[nodiscard]] virtual QueueMetrics Metrics() const = 0;
};

class CasRingQueue final : public RingQueue {
 public:
  explicit CasRingQueue(std::uint32_t capacity_power_of_two);

  Status Enqueue(const Task& task, std::uint32_t timeout_ms) override;
  Status Dequeue(Task* out_task, std::uint32_t timeout_ms) override;
  [[nodiscard]] std::uint32_t Capacity() const override;
  [[nodiscard]] std::uint32_t Size() const override;
  [[nodiscard]] QueueMetrics Metrics() const override;

 private:
  struct Slot {
    std::atomic<std::uint64_t> sequence;
    Task task;
  };

  static bool IsPowerOfTwo(std::uint32_t value) noexcept;
  bool TryEnqueueOnce(const Task& task) noexcept;
  bool TryDequeueOnce(Task* out_task) noexcept;

  const std::uint32_t capacity_;
  const std::uint32_t mask_;
  std::vector<Slot> slots_;

  alignas(64) std::atomic<std::uint64_t> head_;
  alignas(64) std::atomic<std::uint64_t> tail_;

  std::atomic<std::uint64_t> enqueue_success_;
  std::atomic<std::uint64_t> enqueue_timeout_;
  std::atomic<std::uint64_t> dequeue_success_;
  std::atomic<std::uint64_t> dequeue_empty_;
  std::atomic<std::uint64_t> cas_retry_;
  std::atomic<std::uint64_t> backpressure_on_;
  std::atomic<std::uint64_t> backpressure_off_;
};

std::unique_ptr<RingQueue> MakeCasRingQueue(std::uint32_t capacity_power_of_two);

}  // namespace robotaxi

#endif  // ROBOTAXI_RING_QUEUE_H_
