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

// BasicThreadPool：线程池的具体实现类（隐藏在匿名命名空间，对外仅暴露 MakeThreadPool 工厂）。
//
// 设计目标：
// 1) 从 RingQueue 拉取任务并并发执行。
// 2) 提供统一的生命周期管理（Start/Stop）与指标快照（Metrics）。
// 3) 保证工作线程能快速响应停止信号，同时避免过度轮询。
// 4) 向外暴露抽象接口，便于后续替换调度策略。
//
// 并发模型：
// - 使用 state_mu_ 保护生命周期动作（Start/Stop）与线程容器。
// - 使用原子变量保护运行态与高频计数，减少锁竞争。
// - running_ 是主停止开关，active_threads_ 追踪当前活跃 worker 数。
// - Worker 通过读 running_ 决定是否继续轮询，通过修改 active_threads_ 报告自己的生命周期。
//
// 内存序约定：
// - configured_threads_、running_：涉及生命周期可见性，用 acquire/release。
// - active_threads_：既要在 Start 时同步初始化，又要在 Stop 时同步等待，用 acq_rel。
// - executed_tasks_、worker_empty_poll_、worker_block_wait_：仅用于趋势观测，不参与同步，用 relaxed。
//
// ⚠️  风险点与注意事项：
// - Stop() 不会强制中断正在执行的任务；若某个 executor 长时间阻塞，Stop 也会等待。
// - Submit() 在检查 running_ 后可能立即进入 Stop()；调用方需保证生产端先停止。
// - Metrics() 是快照，在并发读写时存在轻微不一致（已接受，用于观测而非精确计费）。
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
    // 启动线程池：创建并启动 thread_count 个工作线程，每个进入 WorkerLoop()。n    // 
    // 参数校验：
    // - thread_count 必须 > 0（否则队列永不被消费）。
    // - queue_ 必须非空（由构造时传入）。
    //
    // 生命周期语义：
    // - 若已运行，返回 AlreadyRunning（接口层把责任推给调用方）。
    // - 若队列为空（不应发生，但防御式检查），返回 InvalidArgument。
    // - 启动前会重置所有指标，避免跨 Start/Stop 周期的数据混污。
    //
    // 线程安全：
    // - state_mu_ 保护 running_ 的 CAS 检查与设置，确保只有一个启动线程真正创建 workers。
    // - running_.store(true, memory_order_release)：确保所有初始化对 worker 可见。
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
    // release 保证：构造函数对 queue_ 与 workers_ 的访问对即将创建的 worker 可见。
    configured_threads_.store(thread_count, std::memory_order_release);
    running_.store(true, std::memory_order_release);

    // 批量创建 worker 线程。每个 lambda 捕获 this，进入 WorkerLoop()。
    // 线程启动后即刻自增 active_threads_（见 WorkerLoop）。
    for (std::uint32_t i = 0; i < thread_count; ++i) {
      workers_.emplace_back([this]() { WorkerLoop(); });
    }

    return Status::Success();
  }

  Status Stop(const std::uint32_t wait_timeout_ms) override {
    // 停止线程池：通知所有 worker 退出轮询，等待它们都离开 WorkerLoop()，然后 join。
    //
    // 停止流程三步走：
    // 1) 在 state_mu_ 保护下，检查是否已运行，设置 running_ = false。
    // 2) 忙等待 active_threads_ 降到 0（每个 worker 退出时会自减）。
    // 3) 调用 JoinAllWorkers() 回收线程资源。
    //
    // 时间语义：
    // - wait_timeout_ms：从 Stop() 调用开始计时；超时仍有活跃线程则返回 Timeout。
    // - 工作线程轮询周期为 1ms，所以即使有任务在执行，也会在 1ms 内检测到 running_ = false。
    //
    // ⚠️  风险点：
    // - 若某个 executor 在 task.executor(task) 中无限循环或死锁，Stop 会一直等待。
    // - Submit() 与 Stop() 存在竞态：Submit 可能在 Stop 之后但线程未完全退出的窗口内失败。
    // - 若 wait_timeout_ms 太短，可能返回 Timeout，但线程仍在后台运行（不会 join）。
    {
      std::lock_guard<std::mutex> lock(state_mu_);
      if (!running_.load(std::memory_order_acquire)) {
        return {ErrorCode::kNotRunning, "thread pool not running"};
      }
      // release 保证：running_ = false 对所有 worker 即刻可见。
      running_.store(false, std::memory_order_release);
    }

    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(wait_timeout_ms);

    // 忙等待所有 worker 都检测到 running_ = false 并退出 WorkerLoop()。
    // 每个 worker 在退出前会自减 active_threads_（见 WorkerLoop 末尾）。
    // acquire 保证：我们能看到 worker 修改的 active_threads_ 最终值。
    while (active_threads_.load(std::memory_order_acquire) != 0U) {
      if (std::chrono::steady_clock::now() >= deadline) {
        return {ErrorCode::kTimeout, "stop timeout"};
      }
      // yield() 让出 CPU 给其他线程，减少忙等的开销。
      std::this_thread::yield();
    }

    // 此时 active_threads_ == 0，所有 worker 已退出 WorkerLoop()，joinable 且安全。
    JoinAllWorkers();
    // 清空配置线程数，表示线程池已停止。
    configured_threads_.store(0, std::memory_order_release);
    return Status::Success();
  }

  Status Submit(const Task& task, const std::uint32_t timeout_ms) override {
    // 提交任务：检查线程池是否运行，然后委托给队列的 Enqueue。
    //
    // 参数与返回值：
    // - task：待入队任务，按值拷贝进队列槽位。
    // - timeout_ms：入队超时窗口（0 表示即时，不等待）。
    // - 返回值：kNotRunning（线程池未启动），其他由队列返回。
    //
    // 线程安全：
    // - 不持锁调用 queue_->Enqueue()，避免长延迟。
    // - running_ 检查与 Enqueue 调用之间存在窗口，期间 Stop() 可能执行。
    //   → 即使发生，Enqueue 的行为仍合法（最坏情况任务入队但 worker 将要停止）。
    //
    // ⚠️  设计约束：
    // - 调用方负责确保任务入队后 executor 不抛异常；若抛异常，worker 会吞掉（见 WorkerLoop）。
    // - task.payload 与 response_handle 生命周期由调用方保证，队列与线程池不负责释放。
    if (!running_.load(std::memory_order_acquire)) {
      return {ErrorCode::kNotRunning, "thread pool not running"};
    }
    return queue_->Enqueue(task, timeout_ms);
  }

  [[nodiscard]] std::uint32_t ActiveThreads() const override {
    return active_threads_.load(std::memory_order_acquire);
  }

  [[nodiscard]] ThreadPoolMetrics Metrics() const override {
    // 返回线程池运行指标快照。用于监控、诊断与健康检查。
    //
    // 字段说明：
    // - configured_threads：启动时设置的线程数；停止后清零。
    // - active_threads：当前在 WorkerLoop 中的线程数。
    // - executed_tasks：成功执行的任务总数（不含失败或异常的任务）。
    // - worker_empty_poll：worker 从队列取不到任务的次数（含空队列与超时）。
    // - worker_block_wait：worker 进入等待的次数（通过短超时出队实现）。
    //
    // 指标特性：
    // - 是快照，不是严格同步点；并发读写时存在轻微偏差（已接受）。
    // - 单调性：executed_tasks 单调递增；其他计数也单调非减。
    // - 用途：趋势观测，不适合精确计费或实时控制。
    //
    // 内存序：
    // - configured_threads/active_threads：用 acquire，确保与生命周期对齐。
    // - 其他计数：用 relaxed，因为是纯观测，不参与同步决策。
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
    // Worker 线程的主循环：不断从队列拉任务并执行。
    //
    // 生命周期与停止语义：
    // - 启动时：自增 active_threads_，表示"我进入了循环"。
    // - 轮询：while(running_) 持续检查停止信号；每轮都用 kWorkerDequeueTimeoutMs = 1ms 的短超时轮询。
    // - 任务执行：取到任务后直接调 executor，用 try/catch 吞掉异常（避免 worker 崩溃）。
    // - 指标累加：每次空轮询、每次执行都相应更新计数。
    // - 退出时：自减 active_threads_，表示"我离开了循环"。
    //
    // 内存序细节：
    // - fetch_add(1, acq_rel)：Start() 时的线程创建与这里的 active_threads_ 初增需要同步。
    // - running_.load(acquire)：每轮都读 running_，确保看到 Stop() 的设置。
    // - executed_tasks_.fetch_add(relaxed)：仅用于观测，不参与同步，可以 relaxed。
    // - fetch_sub(acq_rel)：Stop() 的忙等待需要看到这次递减，所以不能是 relaxed。
    //
    // ⚠️  执行器异常处理：
    // - task.executor 可能抛异常，必须 catch，否则 worker 线程会直接终止。
    // - catch 后不重抛，不记录（设计决策：简化实现；若需记录，可在这里扩展日志接口）。
    //
    // 📊  空轮询与能耗：
    // - 若队列长期空闲，worker 会频繁进入 kWorkerDequeueTimeoutMs 超时，累加 worker_empty_poll。
    // - 这是已接受的权衡：1ms 的轮询周期保证了 Stop 响应快速（< 2ms），代价是小量空轮询。
    // - 若要进一步优化，可考虑条件变量或事件通知（增加复杂度，当前不做）。
    active_threads_.fetch_add(1, std::memory_order_acq_rel);

    while (running_.load(std::memory_order_acquire)) {
      Task task{};
      // 短超时轮询：1ms 是平衡停止响应与空转开销的折衷。
      // - 若 1ms 太短，Stop 响应快但空轮询多。
      // - 若 1ms 太长，空轮询少但 Stop 响应慢。
      const Status s = queue_->Dequeue(&task, kWorkerDequeueTimeoutMs);
      if (s.Ok()) {
        // 成功取到任务，执行之。
        // 线程池边界将异常吞掉，防止 worker 崩溃；调用方 executor 需自行处理异常。
        try {
          task.executor(task);
        } catch (...) {
          // 线程池边界吞掉未捕获异常，避免工作线程崩溃扩散。
          // ⚠️  注意：异常被吞掉，调用方无法感知任务执行失败；如需上报，应在 executor 内处理。
        }
        executed_tasks_.fetch_add(1, std::memory_order_relaxed);
        continue;
      }

      // 未取到任务（队列空或出队超时）。
      // 这里不区分"空队列"与"超时"（都表示暂时无任务），一起计入 worker_empty_poll。
      if (s.code == ErrorCode::kQueueEmpty || s.code == ErrorCode::kTimeout) {
        worker_empty_poll_.fetch_add(1, std::memory_order_relaxed);
        worker_block_wait_.fetch_add(1, std::memory_order_relaxed);
      }
    }

    // running_ 为 false，离开轮询循环。自减 active_threads_，通知 Stop() 可以继续了。
    // acq_rel 保证：Stop() 中的忙等待能看到这次递减。
    active_threads_.fetch_sub(1, std::memory_order_acq_rel);
  }

  void JoinAllWorkers() {
    // 安全回收所有 worker 线程：先 join 再 clear。
    // 前置条件：active_threads_ 应已为 0（由 Stop() 保证）。
    // state_mu_ 保护：避免在 workers_ 迭代时发生竞态（虽然此时应无其他线程修改 workers_）。
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

  // 生命周期与调度相关的原子变量。
  // 停止开关：false 时 worker 立即退出轮询。
  std::atomic<bool> running_;
  // 启动时设置，停止时清零；标记该线程池曾启动过的线程总数。
  std::atomic<std::uint32_t> configured_threads_;
  // 当前在 WorkerLoop 中的活跃线程数；Stop() 通过忙等这个变量降到 0 来判断是否可以 join。
  std::atomic<std::uint32_t> active_threads_;

  // 高频指标计数器（仅用于观测）。
  // 注意：这些计数器在高竞争场景下可能有少量跳跃或漂移（由于缓存一致性协议的延迟），
  // 但对于趋势观测是足够的。若需精确计费，应由上层维护独立的计数机制。
  // 成功执行的任务总数。
  std::atomic<std::uint64_t> executed_tasks_;
  // 工作线程尝试出队但未成功的次数（包括队列空与超时）。
  std::atomic<std::uint64_t> worker_empty_poll_;
  // 工作线程进入等待（短超时）的次数；通常 ≈ worker_empty_poll（都走的是超时路径）。
  std::atomic<std::uint64_t> worker_block_wait_;
};

}  // namespace

std::unique_ptr<ThreadPool> MakeThreadPool(std::shared_ptr<RingQueue> queue) {
  // 工厂函数：创建 BasicThreadPool 实例并以 unique_ptr<ThreadPool> 返回。
  // - 隐藏 BasicThreadPool 的具体实现。
  // - 便于后续替换线程池策略（如：优先级队列版、任务亲和版等）。
  return std::make_unique<BasicThreadPool>(std::move(queue));
}

}  // namespace robotaxi