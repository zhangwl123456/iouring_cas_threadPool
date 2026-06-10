#include "robotaxi/thread_pool.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "gtest/gtest.h"

namespace robotaxi {
namespace {

void CounterExecutor(const Task& task) {
  auto* counter = static_cast<std::atomic<int>*>(const_cast<void*>(task.payload));
  counter->fetch_add(1, std::memory_order_relaxed);
}

Task MakeCounterTask(const std::uint64_t id, std::atomic<int>* counter) {
  return Task{
      .task_id = id,
      .payload = counter,
      .payload_size = sizeof(std::atomic<int>),
      .response_handle = nullptr,
      .executor = &CounterExecutor,
  };
}

TEST(ThreadPoolTest, StartRejectsZeroThreads) {
  auto queue = std::make_shared<CasRingQueue>(64);
  auto pool = MakeThreadPool(queue);

  const Status s = pool->Start(0);
  EXPECT_EQ(s.code, ErrorCode::kInvalidArgument);
}

TEST(ThreadPoolTest, SubmitFailsWhenNotRunning) {
  auto queue = std::make_shared<CasRingQueue>(64);
  auto pool = MakeThreadPool(queue);

  std::atomic<int> counter{0};
  const Status s = pool->Submit(MakeCounterTask(1, &counter), 0);
  EXPECT_EQ(s.code, ErrorCode::kNotRunning);
}

TEST(ThreadPoolTest, StartTwiceReturnsAlreadyRunning) {
  auto queue = std::make_shared<CasRingQueue>(64);
  auto pool = MakeThreadPool(queue);

  EXPECT_TRUE(pool->Start(2).Ok());
  const Status s = pool->Start(2);
  EXPECT_EQ(s.code, ErrorCode::kAlreadyRunning);

  EXPECT_TRUE(pool->Stop(100).Ok());
}

TEST(ThreadPoolTest, ExecutesSubmittedTasks) {
  auto queue = std::make_shared<CasRingQueue>(256);
  auto pool = MakeThreadPool(queue);
  ASSERT_TRUE(pool->Start(2).Ok());

  std::atomic<int> counter{0};
  constexpr int kTaskCount = 64;
  for (int i = 0; i < kTaskCount; ++i) {
    const Status s = pool->Submit(MakeCounterTask(static_cast<std::uint64_t>(i), &counter), 20);
    ASSERT_TRUE(s.Ok());
  }

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
  while (counter.load(std::memory_order_relaxed) != kTaskCount &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  EXPECT_EQ(counter.load(std::memory_order_relaxed), kTaskCount);
  const ThreadPoolMetrics metrics = pool->Metrics();
  EXPECT_GE(metrics.executed_tasks, static_cast<std::uint64_t>(kTaskCount));

  EXPECT_TRUE(pool->Stop(200).Ok());
}

TEST(ThreadPoolTest, StopWhenNotRunningReturnsNotRunning) {
  auto queue = std::make_shared<CasRingQueue>(64);
  auto pool = MakeThreadPool(queue);

  const Status s = pool->Stop(10);
  EXPECT_EQ(s.code, ErrorCode::kNotRunning);
}

}  // namespace
}  // namespace robotaxi