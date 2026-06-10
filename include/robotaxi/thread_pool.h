#ifndef ROBOTAXI_THREAD_POOL_H_
#define ROBOTAXI_THREAD_POOL_H_

#include <cstdint>
#include <memory>

#include "robotaxi/ring_queue.h"
#include "robotaxi/status.h"
#include "robotaxi/types.h"

namespace robotaxi {

// 线程池抽象接口。
// 设计目标：从队列拉取任务并并发执行，向上层暴露统一生命周期与指标接口。
class ThreadPool {
 public:
  virtual ~ThreadPool() = default;

  // 启动线程池。
  // 返回：
  // - kOk: 启动成功。
  // - kInvalidArgument: thread_count 非法。
  // - kAlreadyRunning: 已处于运行态。
  virtual Status Start(std::uint32_t thread_count) = 0;

  // 停止线程池。
  // 返回：
  // - kOk: 在超时窗口内停止成功。
  // - kTimeout: 超时仍有工作线程未退出。
  // - kNotRunning: 线程池未启动。
  virtual Status Stop(std::uint32_t wait_timeout_ms) = 0;

  // 提交任务。
  // 返回：
  // - kNotRunning: 线程池未启动。
  // - 其余语义与队列 Enqueue 一致。
  virtual Status Submit(const Task& task, std::uint32_t timeout_ms) = 0;

  // 返回当前活跃工作线程数量。
  [[nodiscard]] virtual std::uint32_t ActiveThreads() const = 0;

  // 返回线程池指标快照。
  [[nodiscard]] virtual ThreadPoolMetrics Metrics() const = 0;
};

// 工厂函数：创建基于给定队列的线程池实例。
// 所有权：返回 unique_ptr，调用方拥有生命周期。
std::unique_ptr<ThreadPool> MakeThreadPool(std::shared_ptr<RingQueue> queue);

}  // namespace robotaxi

#endif  // ROBOTAXI_THREAD_POOL_H_