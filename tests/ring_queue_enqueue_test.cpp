#include "robotaxi/ring_queue.h"

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace robotaxi {
namespace {

void NoopExecutor(const Task&) {}

Task MakeTask(const std::uint64_t id) {
  return Task{
      .task_id = id,
      .payload = nullptr,
      .payload_size = 0,
      .response_handle = nullptr,
      .executor = &NoopExecutor,
  };
}

TEST(CasRingQueueEnqueueTest, EnqueueSucceedsWhenQueueHasSpace) {
  CasRingQueue queue(8);

  const Status s = queue.Enqueue(MakeTask(1), 0);

  EXPECT_TRUE(s.Ok());
  EXPECT_EQ(queue.Size(), 1U);
}

TEST(CasRingQueueEnqueueTest, EnqueueReturnsQueueFullWhenImmediateModeAndFull) {
  CasRingQueue queue(2);

  EXPECT_TRUE(queue.Enqueue(MakeTask(1), 0).Ok());
  EXPECT_TRUE(queue.Enqueue(MakeTask(2), 0).Ok());
  const Status s = queue.Enqueue(MakeTask(3), 0);

  EXPECT_EQ(s.code, ErrorCode::kQueueFull);
}

TEST(CasRingQueueEnqueueTest, EnqueueReturnsTimeoutWhenQueueRemainsFull) {
  CasRingQueue queue(2);

  EXPECT_TRUE(queue.Enqueue(MakeTask(1), 0).Ok());
  EXPECT_TRUE(queue.Enqueue(MakeTask(2), 0).Ok());
  const Status s = queue.Enqueue(MakeTask(3), 2);

  EXPECT_EQ(s.code, ErrorCode::kTimeout);
}

TEST(CasRingQueueEnqueueTest, EnqueueEventuallySucceedsWithinTimeout) {
  CasRingQueue queue(2);
  EXPECT_TRUE(queue.Enqueue(MakeTask(1), 0).Ok());

  std::thread consumer([&queue]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    Task out{};
    EXPECT_TRUE(queue.Dequeue(&out, 5).Ok());
  });

  const Status s = queue.Enqueue(MakeTask(2), 20);
  consumer.join();

  EXPECT_TRUE(s.Ok());
}

TEST(CasRingQueueEnqueueTest, EnqueueIsSafeUnderConcurrentProducers) {
  CasRingQueue queue(1024);
  constexpr int kProducerCount = 4;
  constexpr int kPerProducer = 2000;

  std::atomic<int> ok_count{0};
  std::vector<std::thread> producers;
  producers.reserve(kProducerCount);

  for (int p = 0; p < kProducerCount; ++p) {
    producers.emplace_back([p, &queue, &ok_count]() {
      for (int i = 0; i < kPerProducer; ++i) {
        const Status s = queue.Enqueue(MakeTask(static_cast<std::uint64_t>(p * 100000 + i)), 20);
        if (s.Ok()) {
          ok_count.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  std::thread consumer([&queue]() {
    int consumed = 0;
    Task out{};
    while (consumed < kProducerCount * kPerProducer) {
      const Status s = queue.Dequeue(&out, 20);
      if (s.Ok()) {
        ++consumed;
      }
    }
  });

  for (auto& t : producers) {
    t.join();
  }
  consumer.join();

  EXPECT_EQ(ok_count.load(std::memory_order_relaxed), kProducerCount * kPerProducer);
}

}  // namespace
}  // namespace robotaxi
