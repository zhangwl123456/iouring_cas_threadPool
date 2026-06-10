#include "robotaxi/thread_pool.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace robotaxi {
namespace {

// 工作线程出队采用短超时轮询，平衡停止响应速度与空转开销。
constexpr std::uint32_t kWorkerDequeueTimeoutMs = 1;

class BasicThreadPool final : public ThreadPool {
 public:
  explicit BasicThreadPool(std::shared_ptr<RingQueue> queue)
      : queue_(std::move(queue)),
        running_(false),
        configured_threads_(0),
        active_threads_(0),
        executed_tasks_(0),
        worker_empty_poll_(0),
        worker_block_wait_(0) {}

  ~BasicThreadPool() override {
    // 析构阶段做兜底停止，避免 joinable 线程触发 std::terminate。
    const Status s = Stop(1000);
    if (s.code == ErrorCode::kTimeout) {
      JoinAllWorkers();
    }
  }

  Status Start(const std::uint32_t thread_count) override {
    if (thread_count == 0U) {
      return {ErrorCode::kInvalidArgument, "thread_count must be > 0"};
    }
    if (queue_ == nullptr) {
      return {ErrorCode::kInvalidArgument, "queue is null"};
    }

    std::lock_guard<std::mutex> lock(state_mu_);
    if (running_.load(std::memory_order_acquire)) {
      return {ErrorCode::kAlreadyRunning, "thread pool already running"};
    }

    executed_tasks_.store(0, std::memory_order_relaxed);
    worker_empty_poll_.store(0, std::memory_order_relaxed);
    worker_block_wait_.store(0, std::memory_order_relaxed);
    active_threads_.store(0, std::memory_order_relaxed);

    workers_.clear();
    workers_.reserve(thread_count);
    configured_threads_.store(thread_count, std::memory_order_release);
    running_.store(true, std::memory_order_release);

    for (std::uint32_t i = 0; i < thread_count; ++i) {
      workers_.emplace_back([this]() { WorkerLoop(); });
    }

    return Status::Success();
  }

  Status Stop(const std::uint32_t wait_timeout_ms) override {
    {
      std::lock_guard<std::mutex> lock(state_mu_);
      if (!running_.load(std::memory_order_acquire)) {
        return {ErrorCode::kNotRunning, "thread pool not running"};
      }
      running_.store(false, std::memory_order_release);
    }

    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(wait_timeout_ms);

    while (active_threads_.load(std::memory_order_acquire) != 0U) {
      if (std::chrono::steady_clock::now() >= deadline) {
        return {ErrorCode::kTimeout, "stop timeout"};
      }
      std::this_thread::yield();
    }

    JoinAllWorkers();
    configured_threads_.store(0, std::memory_order_release);
    return Status::Success();
  }

  Status Submit(const Task& task, const std::uint32_t timeout_ms) override {
    if (!running_.load(std::memory_order_acquire)) {
      return {ErrorCode::kNotRunning, "thread pool not running"};
    }
    return queue_->Enqueue(task, timeout_ms);
  }

  [[nodiscard]] std::uint32_t ActiveThreads() const override {
    return active_threads_.load(std::memory_order_acquire);
  }

  [[nodiscard]] ThreadPoolMetrics Metrics() const override {
    return ThreadPoolMetrics{
        .configured_threads = configured_threads_.load(std::memory_order_acquire),
        .active_threads = active_threads_.load(std::memory_order_acquire),
        .executed_tasks = executed_tasks_.load(std::memory_order_relaxed),
        .worker_empty_poll = worker_empty_poll_.load(std::memory_order_relaxed),
        .worker_block_wait = worker_block_wait_.load(std::memory_order_relaxed),
    };
  }

 private:
  void WorkerLoop() noexcept {
    active_threads_.fetch_add(1, std::memory_order_acq_rel);

    while (running_.load(std::memory_order_acquire)) {
      Task task{};
      const Status s = queue_->Dequeue(&task, kWorkerDequeueTimeoutMs);
      if (s.Ok()) {
        try {
          task.executor(task);
        } catch (...) {
          // 线程池边界吞掉未捕获异常，避免工作线程崩溃扩散。
        }
        executed_tasks_.fetch_add(1, std::memory_order_relaxed);
        continue;
      }

      if (s.code == ErrorCode::kQueueEmpty || s.code == ErrorCode::kTimeout) {
        worker_empty_poll_.fetch_add(1, std::memory_order_relaxed);
        worker_block_wait_.fetch_add(1, std::memory_order_relaxed);
      }
    }

    active_threads_.fetch_sub(1, std::memory_order_acq_rel);
  }

  void JoinAllWorkers() {
    std::lock_guard<std::mutex> lock(state_mu_);
    for (std::thread& worker : workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
    workers_.clear();
  }

  std::shared_ptr<RingQueue> queue_;

  mutable std::mutex state_mu_;
  std::vector<std::thread> workers_;

  std::atomic<bool> running_;
  std::atomic<std::uint32_t> configured_threads_;
  std::atomic<std::uint32_t> active_threads_;

  std::atomic<std::uint64_t> executed_tasks_;
  std::atomic<std::uint64_t> worker_empty_poll_;
  std::atomic<std::uint64_t> worker_block_wait_;
};

}  // namespace

std::unique_ptr<ThreadPool> MakeThreadPool(std::shared_ptr<RingQueue> queue) {
  return std::make_unique<BasicThreadPool>(std::move(queue));
}

}  // namespace robotaxi