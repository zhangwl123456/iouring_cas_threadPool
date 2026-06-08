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

// 队列抽象接口。
// 设计目标：为网络接入层和执行层提供无锁、低延迟、可观测的任务缓冲。
// 线程安全：接口允许多生产者/多消费者并发调用。
class RingQueue {
 public:
  virtual ~RingQueue() = default;

  // 入队任务。
  // 参数：
  // - task: 待入队任务，按值语义拷贝到队列槽位。
  // - timeout_ms: 超时窗口；0 表示即时模式，不等待。
  // 返回：
  // - kOk: 入队成功。
  // - kQueueFull: 即时模式且队列满。
  // - kTimeout: 等待超时仍未入队。
  // - kInvalidArgument: 任务非法（例如 executor 为空）。
  virtual Status Enqueue(const Task& task, std::uint32_t timeout_ms) = 0;

  // 出队任务。
  // 参数：
  // - out_task: 输出参数，成功时写入任务。
  // - timeout_ms: 超时窗口；0 表示即时模式，不等待。
  // 返回：
  // - kOk: 出队成功。
  // - kQueueEmpty: 即时模式且队列空。
  // - kTimeout: 等待超时仍未出队。
  // - kInvalidArgument: out_task 为空。
  virtual Status Dequeue(Task* out_task, std::uint32_t timeout_ms) = 0;

  // 返回固定容量（槽位数）。
  [[nodiscard]] virtual std::uint32_t Capacity() const = 0;

  // 返回当前估算深度（并发条件下为近似瞬时值）。
  [[nodiscard]] virtual std::uint32_t Size() const = 0;

  // 返回统计快照（用于监控与诊断）。
  [[nodiscard]] virtual QueueMetrics Metrics() const = 0;
};

// 基于 CAS 的 MPMC 环形队列实现。
// 核心算法：每个槽位携带 sequence 序号，生产/消费线程通过序号与全局 head/tail 协同，
// 在无锁条件下完成占用、发布、回收三个阶段。
class CasRingQueue final : public RingQueue {
 public:
  // 构造函数。
  // 参数要求：capacity_power_of_two 必须为 2 的幂且 >= 2。
  // 异常：参数不满足约束时抛出 std::invalid_argument。
  explicit CasRingQueue(std::uint32_t capacity_power_of_two);

  Status Enqueue(const Task& task, std::uint32_t timeout_ms) override;
  Status Dequeue(Task* out_task, std::uint32_t timeout_ms) override;
  [[nodiscard]] std::uint32_t Capacity() const override;
  [[nodiscard]] std::uint32_t Size() const override;
  [[nodiscard]] QueueMetrics Metrics() const override;

 private:
  // 槽位状态：
  // - sequence: 槽位时序戳，决定该槽位当前可写/可读/已回收状态。
  // - task: 槽位承载的数据。
  struct Slot {
    std::atomic<std::uint64_t> sequence;
    Task task;
  };

  // 判断值是否为 2 的幂。
  static bool IsPowerOfTwo(std::uint32_t value) noexcept;

  // 单次无阻塞尝试入队；失败表示当前无法入队（通常为队列满或竞争失败）。
  bool TryEnqueueOnce(const Task& task) noexcept;

  // 单次无阻塞尝试出队；失败表示当前无可读元素或竞争失败。
  bool TryDequeueOnce(Task* out_task) noexcept;

  // 容量与掩码：mask = capacity - 1，用于快速取模（capacity 为 2 的幂时成立）。
  const std::uint32_t capacity_;
  const std::uint32_t mask_;

  // 固定槽位数组。构造时一次分配，运行期不扩容，降低内存抖动。
  std::vector<Slot> slots_;

  // head/tail 分离并按 cache line 对齐，减少伪共享。
  alignas(64) std::atomic<std::uint64_t> head_;
  alignas(64) std::atomic<std::uint64_t> tail_;

  // 运行指标原子计数器。
  std::atomic<std::uint64_t> enqueue_success_;
  std::atomic<std::uint64_t> enqueue_timeout_;
  std::atomic<std::uint64_t> dequeue_success_;
  std::atomic<std::uint64_t> dequeue_empty_;
  std::atomic<std::uint64_t> cas_retry_;
  std::atomic<std::uint64_t> backpressure_on_;
  std::atomic<std::uint64_t> backpressure_off_;
};

// 工厂函数：创建 CAS 队列实例。
// 返回值拥有对象所有权（unique_ptr）。
std::unique_ptr<RingQueue> MakeCasRingQueue(std::uint32_t capacity_power_of_two);

}  // namespace robotaxi

#endif  // ROBOTAXI_RING_QUEUE_H_
