#ifndef ROBOTAXI_TYPES_H_
#define ROBOTAXI_TYPES_H_

#include <cstdint>

namespace robotaxi {

struct Task;
using TaskExecutor = void (*)(const Task& task);

struct Task {
  std::uint64_t task_id;
  const void* payload;
  std::uint32_t payload_size;
  void* response_handle;
  TaskExecutor executor;
};

struct QueueMetrics {
  std::uint32_t capacity;
  std::uint32_t size;
  std::uint64_t enqueue_success;
  std::uint64_t enqueue_timeout;
  std::uint64_t dequeue_success;
  std::uint64_t dequeue_empty;
  std::uint64_t cas_retry;
  std::uint64_t backpressure_on;
  std::uint64_t backpressure_off;
};

}  // namespace robotaxi

#endif  // ROBOTAXI_TYPES_H_
