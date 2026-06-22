#include "robotaxi/network_ingress.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "robotaxi/capacity_executor.h"
#include "robotaxi/frame_decoder.h"
#include "robotaxi/ring_queue.h"

namespace robotaxi {
namespace {

FrameConfig DefaultFrameConfig() {
  return FrameConfig{
      .length_field_bytes = 4,
      .max_frame_length = 1024U * 1024U,
      .network_byte_order = true,
  };
}

IngressConfig MakeIngressConfig(const std::uint16_t port) {
  return IngressConfig{
      .backend_type = IngressBackendType::kEpoll,
      .bind_ip = "127.0.0.1",
      .bind_port = port,
      .listen_backlog = 16,
      .recv_buffer_size = 1U << 20,
      .send_buffer_size = 1U << 20,
      .max_connections = 128,
      .max_events_per_poll = 64,
  };
}

std::uint16_t FindFreePort() {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  EXPECT_GE(fd, 0);

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(0);
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  EXPECT_EQ(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);

  socklen_t len = sizeof(addr);
  EXPECT_EQ(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len), 0);
  const std::uint16_t port = ntohs(addr.sin_port);

  EXPECT_EQ(::close(fd), 0);
  return port;
}

std::vector<std::uint8_t> BuildFrame(const std::vector<std::uint8_t>& payload) {
  const std::uint32_t payload_size = static_cast<std::uint32_t>(payload.size());
  std::vector<std::uint8_t> frame;
  frame.reserve(4U + payload.size());
  frame.push_back(static_cast<std::uint8_t>((payload_size >> 24U) & 0xFFU));
  frame.push_back(static_cast<std::uint8_t>((payload_size >> 16U) & 0xFFU));
  frame.push_back(static_cast<std::uint8_t>((payload_size >> 8U) & 0xFFU));
  frame.push_back(static_cast<std::uint8_t>(payload_size & 0xFFU));
  frame.insert(frame.end(), payload.begin(), payload.end());
  return frame;
}

void NoopExecutor(const Task&) {}

TEST(NetworkIngressEpollTest, StartRejectsInvalidConfig) {
  auto queue = std::make_shared<CasRingQueue>(64);
  auto decoder = MakeFrameDecoder(DefaultFrameConfig());
  auto ingress = MakeEpollNetworkIngress(queue, std::move(decoder));

  IngressConfig cfg = MakeIngressConfig(FindFreePort());
  cfg.bind_ip = "";

  const Status s = ingress->Start(cfg, DefaultFrameConfig());
  EXPECT_EQ(s.code, ErrorCode::kInvalidArgument);
}

TEST(NetworkIngressEpollTest, PollOnceReturnsOkOnTimeoutWithoutEvents) {
  auto queue = std::make_shared<CasRingQueue>(64);
  auto decoder = MakeFrameDecoder(DefaultFrameConfig());
  auto ingress = MakeEpollNetworkIngress(queue, std::move(decoder));

  const std::uint16_t port = FindFreePort();
  ASSERT_TRUE(ingress->Start(MakeIngressConfig(port), DefaultFrameConfig()).Ok());

  const Status s = ingress->PollOnce(5);
  EXPECT_TRUE(s.Ok());

  EXPECT_TRUE(ingress->Stop(0).Ok());
}

TEST(NetworkIngressEpollTest, PollOnceAcceptsAndEnqueuesFrame) {
  auto queue = std::make_shared<CasRingQueue>(128);
  auto decoder = MakeFrameDecoder(DefaultFrameConfig());
  auto ingress = MakeEpollNetworkIngress(queue, std::move(decoder));

  const std::uint16_t port = FindFreePort();
  ASSERT_TRUE(ingress->Start(MakeIngressConfig(port), DefaultFrameConfig()).Ok());

  const int client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(client_fd, 0);

  sockaddr_in srv{};
  srv.sin_family = AF_INET;
  srv.sin_port = htons(port);
  ASSERT_EQ(::inet_pton(AF_INET, "127.0.0.1", &srv.sin_addr), 1);
  ASSERT_EQ(::connect(client_fd, reinterpret_cast<sockaddr*>(&srv), sizeof(srv)), 0);

  const std::vector<std::uint8_t> payload{0x11, 0x22, 0x33, 0x44};
  const std::vector<std::uint8_t> frame = BuildFrame(payload);
  ASSERT_EQ(::send(client_fd, frame.data(), frame.size(), 0), static_cast<ssize_t>(frame.size()));

  bool enqueued = false;
  for (int i = 0; i < 40; ++i) {
    const Status poll_s = ingress->PollOnce(5);
    ASSERT_TRUE(poll_s.Ok() || poll_s.code == ErrorCode::kBackpressure);
    if (queue->Size() > 0U) {
      enqueued = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  ASSERT_TRUE(enqueued);

  Task out{};
  const Status dq = queue->Dequeue(&out, 10);
  ASSERT_TRUE(dq.Ok());
  EXPECT_EQ(out.payload_size, payload.size());
  EXPECT_NE(out.executor, nullptr);
  out.executor(out);

  const IngressMetrics metrics = ingress->Metrics();
  EXPECT_GE(metrics.accepted_connections, 1U);
  EXPECT_GE(metrics.completed_frames, 1U);

  ASSERT_EQ(::close(client_fd), 0);
  EXPECT_TRUE(ingress->Stop(0).Ok());
}

TEST(NetworkIngressEpollTest, PollOnceReturnsBackpressureWhenQueueIsFull) {
  auto queue = std::make_shared<CasRingQueue>(2);
  auto decoder = MakeFrameDecoder(DefaultFrameConfig());
  auto ingress = MakeEpollNetworkIngress(queue, std::move(decoder));

  ASSERT_TRUE(queue->Enqueue(Task{.task_id = 1,
                                  .payload = nullptr,
                                  .payload_size = 0,
                                  .response_handle = nullptr,
                                  .executor = &NoopExecutor},
                             0)
                  .Ok());
  ASSERT_TRUE(queue->Enqueue(Task{.task_id = 2,
                                  .payload = nullptr,
                                  .payload_size = 0,
                                  .response_handle = nullptr,
                                  .executor = &NoopExecutor},
                             0)
                  .Ok());

  const std::uint16_t port = FindFreePort();
  ASSERT_TRUE(ingress->Start(MakeIngressConfig(port), DefaultFrameConfig()).Ok());

  const Status s = ingress->PollOnce(0);
  EXPECT_EQ(s.code, ErrorCode::kBackpressure);

  EXPECT_TRUE(ingress->Stop(0).Ok());
}

TEST(NetworkIngressEpollTest, BackpressureHysteresisKeepsBackpressureAboveLowWatermark) {
  auto queue = std::make_shared<CasRingQueue>(16);
  auto decoder = MakeFrameDecoder(DefaultFrameConfig());
  auto ingress = MakeEpollNetworkIngress(queue, std::move(decoder));

  for (std::uint64_t i = 0; i < 14U; ++i) {
    ASSERT_TRUE(queue->Enqueue(Task{.task_id = i + 1,
                                    .payload = nullptr,
                                    .payload_size = 0,
                                    .response_handle = nullptr,
                                    .executor = &NoopExecutor},
                               0)
                    .Ok());
  }

  const std::uint16_t port = FindFreePort();
  ASSERT_TRUE(ingress->Start(MakeIngressConfig(port), DefaultFrameConfig()).Ok());

  const Status first_poll = ingress->PollOnce(0);
  ASSERT_EQ(first_poll.code, ErrorCode::kBackpressure);

  Task out{};
  ASSERT_TRUE(queue->Dequeue(&out, 10).Ok());
  ASSERT_TRUE(queue->Dequeue(&out, 10).Ok());
  ASSERT_TRUE(queue->Dequeue(&out, 10).Ok());
  ASSERT_TRUE(queue->Dequeue(&out, 10).Ok());
  EXPECT_EQ(queue->Size(), 10U);

  const Status second_poll = ingress->PollOnce(0);
  EXPECT_EQ(second_poll.code, ErrorCode::kBackpressure);

  EXPECT_TRUE(ingress->Stop(0).Ok());
}

TEST(NetworkIngressEpollTest, BackpressureHysteresisRecoversAtLowWatermarkOrBelow) {
  auto queue = std::make_shared<CasRingQueue>(16);
  auto decoder = MakeFrameDecoder(DefaultFrameConfig());
  auto ingress = MakeEpollNetworkIngress(queue, std::move(decoder));

  for (std::uint64_t i = 0; i < 14U; ++i) {
    ASSERT_TRUE(queue->Enqueue(Task{.task_id = i + 1,
                                    .payload = nullptr,
                                    .payload_size = 0,
                                    .response_handle = nullptr,
                                    .executor = &NoopExecutor},
                               0)
                    .Ok());
  }

  const std::uint16_t port = FindFreePort();
  ASSERT_TRUE(ingress->Start(MakeIngressConfig(port), DefaultFrameConfig()).Ok());

  const Status first_poll = ingress->PollOnce(0);
  ASSERT_EQ(first_poll.code, ErrorCode::kBackpressure);

  Task out{};
  for (int i = 0; i < 5; ++i) {
    ASSERT_TRUE(queue->Dequeue(&out, 10).Ok());
  }
  EXPECT_EQ(queue->Size(), 9U);

  const Status second_poll = ingress->PollOnce(0);
  EXPECT_TRUE(second_poll.Ok());

  EXPECT_TRUE(ingress->Stop(0).Ok());
}

TEST(NetworkIngressEpollTest, EndToEndPayloadPassingFromIngressToThreadPool) {
  // 测试完整链路：ingress 接收 → 切帧 → 入队 → payload 验证。
  // 验证：payload 缓冲有效，Task 中的指针指向有效数据，生命周期覆盖出队后使用。
  auto queue = std::make_shared<CasRingQueue>(256);
  auto decoder = MakeFrameDecoder(DefaultFrameConfig());
  auto ingress = MakeEpollNetworkIngress(queue, std::move(decoder));

  const std::uint16_t port = FindFreePort();
  ASSERT_TRUE(ingress->Start(MakeIngressConfig(port), DefaultFrameConfig()).Ok());

  // 创建客户端连接并发送多个帧。
  const int client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(client_fd, 0);

  sockaddr_in srv{};
  srv.sin_family = AF_INET;
  srv.sin_port = htons(port);
  ASSERT_EQ(::inet_pton(AF_INET, "127.0.0.1", &srv.sin_addr), 1);
  ASSERT_EQ(::connect(client_fd, reinterpret_cast<sockaddr*>(&srv), sizeof(srv)), 0);

  // 发送 3 个帧，payload 分别为不同的内容。
  const std::vector<std::uint8_t> payload_1{0x11, 0x22, 0x33, 0x44};
  const std::vector<std::uint8_t> payload_2{0xAA, 0xBB, 0xCC};
  const std::vector<std::uint8_t> payload_3{0xFF, 0xEE};

  const std::vector<std::uint8_t> frame_1 = BuildFrame(payload_1);
  const std::vector<std::uint8_t> frame_2 = BuildFrame(payload_2);
  const std::vector<std::uint8_t> frame_3 = BuildFrame(payload_3);

  // 一次性发送所有帧数据。
  std::vector<std::uint8_t> all_frames;
  all_frames.insert(all_frames.end(), frame_1.begin(), frame_1.end());
  all_frames.insert(all_frames.end(), frame_2.begin(), frame_2.end());
  all_frames.insert(all_frames.end(), frame_3.begin(), frame_3.end());

  ASSERT_EQ(::send(client_fd, all_frames.data(), all_frames.size(), 0),
            static_cast<ssize_t>(all_frames.size()));

  // 轮询 ingress，处理接收、切帧、入队。
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (queue->Size() < 3U) {
    const Status poll_s = ingress->PollOnce(5);
    ASSERT_TRUE(poll_s.Ok() || poll_s.code == ErrorCode::kBackpressure);

    if (queue->Size() >= 3U) {
      break;  // 3 个帧已入队。
    }
    if (std::chrono::steady_clock::now() > deadline) {
      FAIL() << "Timeout waiting for frames to be enqueued";
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // 从队列出队并验证 payload 有效性。
  Task task_1{};
  const Status dq_1 = queue->Dequeue(&task_1, 100);
  ASSERT_TRUE(dq_1.Ok());
  EXPECT_NE(task_1.payload, nullptr);
  EXPECT_EQ(task_1.payload_size, payload_1.size());
  EXPECT_EQ(std::vector<std::uint8_t>(
                static_cast<const std::uint8_t*>(task_1.payload),
                static_cast<const std::uint8_t*>(task_1.payload) + task_1.payload_size),
            payload_1);

  Task task_2{};
  const Status dq_2 = queue->Dequeue(&task_2, 100);
  ASSERT_TRUE(dq_2.Ok());
  EXPECT_NE(task_2.payload, nullptr);
  EXPECT_EQ(task_2.payload_size, payload_2.size());
  EXPECT_EQ(std::vector<std::uint8_t>(
                static_cast<const std::uint8_t*>(task_2.payload),
                static_cast<const std::uint8_t*>(task_2.payload) + task_2.payload_size),
            payload_2);

  Task task_3{};
  const Status dq_3 = queue->Dequeue(&task_3, 100);
  ASSERT_TRUE(dq_3.Ok());
  EXPECT_NE(task_3.payload, nullptr);
  EXPECT_EQ(task_3.payload_size, payload_3.size());
  EXPECT_EQ(std::vector<std::uint8_t>(
                static_cast<const std::uint8_t*>(task_3.payload),
                static_cast<const std::uint8_t*>(task_3.payload) + task_3.payload_size),
            payload_3);

  // 验证 ingress 指标。
  const IngressMetrics ingress_metrics = ingress->Metrics();
  EXPECT_GE(ingress_metrics.completed_frames, 3U);
  EXPECT_EQ(ingress_metrics.frame_decode_error, 0U);

  ASSERT_EQ(::close(client_fd), 0);
  EXPECT_TRUE(ingress->Stop(0).Ok());
}

}  // namespace
}  // namespace robotaxi
