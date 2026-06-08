#include "robotaxi/ring_queue.h"

#include <chrono>
#include <stdexcept>
#include <thread>

namespace robotaxi {
namespace {

// 入队超时路径中的自旋阈值。
// 设计意图：
// 1) 先用较短自旋覆盖“消费者马上释放槽位”的高概率场景；
// 2) 超过阈值后逐步让出 CPU，避免高并发下生产者形成忙等风暴。
constexpr std::uint32_t kSpinThreshold = 1024;

}  // namespace

bool CasRingQueue::IsPowerOfTwo(const std::uint32_t value) noexcept {
  return value != 0U && (value & (value - 1U)) == 0U;
}

CasRingQueue::CasRingQueue(const std::uint32_t capacity_power_of_two)
    : capacity_(capacity_power_of_two),
      mask_(capacity_power_of_two - 1U),
      slots_(capacity_power_of_two),
      head_(0),
      tail_(0),
      enqueue_success_(0),
      enqueue_timeout_(0),
      dequeue_success_(0),
      dequeue_empty_(0),
      cas_retry_(0),
      backpressure_on_(0),
      backpressure_off_(0) {
  // 约束原因：
  // mask_ 使用位与完成取模，只有容量为 2 的幂时该优化成立。
  // 容量至少为 2，避免队列无法区分有效推进与空转。
  if (!IsPowerOfTwo(capacity_power_of_two) || capacity_power_of_two < 2U) {
    throw std::invalid_argument("capacity must be power-of-two and >= 2");
  }

  // 初始化槽位序号：第 i 个槽位初始可写条件为 seq == i。
  // 这让首轮生产者可直接按 tail 命中对应槽位并完成占用。
  for (std::uint64_t i = 0; i < capacity_; ++i) {
    slots_[static_cast<std::size_t>(i)].sequence.store(i, std::memory_order_relaxed);
  }
}

bool CasRingQueue::TryEnqueueOnce(const Task& task) noexcept {
  std::uint64_t tail = tail_.load(std::memory_order_acquire);

  for (;;) {
    Slot& slot = slots_[static_cast<std::size_t>(tail & mask_)];
    const std::uint64_t seq = slot.sequence.load(std::memory_order_acquire);

    // 底层原理：
    // seq == tail 表示这个槽位正好属于当前生产者轮次，可安全写入。
    // seq < tail  表示消费者尚未释放该槽位，队列在当前 tail 视角下已满。
    // seq > tail  表示别的生产者推进了 tail，本线程需要重新对齐 tail。
    //
    // 槽位状态变化示意（capacity = 8）：
    //   [FREE(seq=10)] --生产者占用--> [WRITE task] --发布 seq=11--> [READY]
    //   [READY(seq=11)] --消费者取走--> [RECYCLE seq=18] --> [FREE for next lap]
    if (seq == tail) {
      if (tail_.compare_exchange_weak(
              tail, tail + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
        slot.task = task;
        // 发布写入完成：消费者只在看到 seq == head+1 时读取 task。
        slot.sequence.store(tail + 1, std::memory_order_release);
        enqueue_success_.fetch_add(1, std::memory_order_relaxed);
        return true;
      }
      cas_retry_.fetch_add(1, std::memory_order_relaxed);
      continue;
    }

    if (seq < tail) {
      return false;
    }

    tail = tail_.load(std::memory_order_acquire);
    cas_retry_.fetch_add(1, std::memory_order_relaxed);
  }
}

Status CasRingQueue::Enqueue(const Task& task, const std::uint32_t timeout_ms) {
  // 执行入口为空会导致消费阶段无法调用业务逻辑，属于硬错误。
  if (task.executor == nullptr) {
    return {ErrorCode::kInvalidArgument, "task.executor is null"};
  }

  if (TryEnqueueOnce(task)) {
    return Status::Success();
  }

  if (timeout_ms == 0U) {
    return {ErrorCode::kQueueFull, "queue is full"};
  }

  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeout_ms);
  std::uint32_t spins = 0;

  while (std::chrono::steady_clock::now() < deadline) {
    if (TryEnqueueOnce(task)) {
      return Status::Success();
    }

    ++spins;
    // 底层原理：
    // 先短自旋让出最小调度开销，适合消费者即将释放槽位的短临界区；
    // 超过阈值后 yield，避免大量生产者同时忙等导致 CPU 风暴。
    if (spins < kSpinThreshold) {
      std::this_thread::yield();
      continue;
    }

    std::this_thread::sleep_for(std::chrono::microseconds(50));
  }

  enqueue_timeout_.fetch_add(1, std::memory_order_relaxed);
  return {ErrorCode::kTimeout, "enqueue timeout"};
}

bool CasRingQueue::TryDequeueOnce(Task* out_task) noexcept {
  std::uint64_t head = head_.load(std::memory_order_acquire);

  for (;;) {
    Slot& slot = slots_[static_cast<std::size_t>(head & mask_)];
    const std::uint64_t seq = slot.sequence.load(std::memory_order_acquire);
    const std::uint64_t expected_ready = head + 1;

    // 出队侧状态机（与入队侧 sequence 规则对偶）：
    // seq == head + 1 : 槽位已有可读任务，可尝试 CAS 推进 head。
    // seq <  head + 1 : 该槽位尚未被当前轮次生产者发布，视为队列空。
    // seq >  head + 1 : 其他消费者已推进或观测跨轮次，刷新 head 重试。
    //
    // 简图（capacity = 8）：
    //   [READY(seq=11)] --消费者占用--> [READ task] --发布 seq=18--> [FREE(next lap)]
    if (seq == expected_ready) {
      if (head_.compare_exchange_weak(
              head, head + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
        *out_task = slot.task;
        // 消费完成后，把槽位序号跳到下一轮 free 值。
        slot.sequence.store(head + capacity_, std::memory_order_release);
        dequeue_success_.fetch_add(1, std::memory_order_relaxed);
        return true;
      }
      cas_retry_.fetch_add(1, std::memory_order_relaxed);
      continue;
    }

    if (seq < expected_ready) {
      return false;
    }

    head = head_.load(std::memory_order_acquire);
    cas_retry_.fetch_add(1, std::memory_order_relaxed);
  }
}

Status CasRingQueue::Dequeue(Task* out_task, const std::uint32_t timeout_ms) {
  // 输出指针为空无法承载结果，直接拒绝。
  if (out_task == nullptr) {
    return {ErrorCode::kInvalidArgument, "out_task is null"};
  }

  if (TryDequeueOnce(out_task)) {
    return Status::Success();
  }

  if (timeout_ms == 0U) {
    dequeue_empty_.fetch_add(1, std::memory_order_relaxed);
    return {ErrorCode::kQueueEmpty, "queue is empty"};
  }

  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    if (TryDequeueOnce(out_task)) {
      return Status::Success();
    }
    std::this_thread::yield();
  }

  dequeue_empty_.fetch_add(1, std::memory_order_relaxed);
  return {ErrorCode::kTimeout, "dequeue timeout"};
}

std::uint32_t CasRingQueue::Capacity() const { return capacity_; }

std::uint32_t CasRingQueue::Size() const {
  // 并发语义说明：
  // 这是瞬时近似值，不保证和任一时刻严格一致，但对监控和背压判定足够。
  const std::uint64_t tail = tail_.load(std::memory_order_acquire);
  const std::uint64_t head = head_.load(std::memory_order_acquire);
  return static_cast<std::uint32_t>(tail - head);
}

QueueMetrics CasRingQueue::Metrics() const {
  // 快照读取采用 relaxed，目标是低开销观测；
  // 指标用于趋势分析与告警，不承担强一致业务语义。
  return QueueMetrics{
      .capacity = capacity_,
      .size = Size(),
      .enqueue_success = enqueue_success_.load(std::memory_order_relaxed),
      .enqueue_timeout = enqueue_timeout_.load(std::memory_order_relaxed),
      .dequeue_success = dequeue_success_.load(std::memory_order_relaxed),
      .dequeue_empty = dequeue_empty_.load(std::memory_order_relaxed),
      .cas_retry = cas_retry_.load(std::memory_order_relaxed),
      .backpressure_on = backpressure_on_.load(std::memory_order_relaxed),
      .backpressure_off = backpressure_off_.load(std::memory_order_relaxed),
  };
}

std::unique_ptr<RingQueue> MakeCasRingQueue(const std::uint32_t capacity_power_of_two) {
  return std::make_unique<CasRingQueue>(capacity_power_of_two);
}

}  // namespace robotaxi
