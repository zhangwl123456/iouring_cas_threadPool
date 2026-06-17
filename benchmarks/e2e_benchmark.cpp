#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "robotaxi/capacity_executor.h"
#include "robotaxi/frame_decoder.h"
#include "robotaxi/network_ingress.h"
#include "robotaxi/ring_queue.h"
#include "robotaxi/status.h"
#include "robotaxi/thread_pool.h"
#include "robotaxi/types.h"

namespace robotaxi {
namespace {

// ============================================================================
// 端到端基准压测配置与结果结构
// ============================================================================

struct E2EBenchmarkConfig {
  std::uint64_t total_tasks = 20000;
  std::uint32_t worker_threads = 4;
  std::uint32_t queue_capacity = 65536;
  std::uint32_t payload_bytes = 256;
  std::uint32_t client_connections = 4;
  std::uint32_t bind_port = 9999;
  std::string output_format = "json";
  std::string output_file;
};

struct E2EBenchmarkResult {
  std::uint64_t total_tasks = 0;
  std::uint32_t worker_threads = 0;
  std::uint32_t queue_capacity = 0;
  std::uint32_t payload_bytes = 0;
  std::uint32_t client_connections = 0;
  double duration_ms = 0.0;
  double throughput_fps = 0.0;  // frames/s
  double throughput_tps = 0.0;  // tasks/s
  double p50_latency_us = 0.0;
  double p99_latency_us = 0.0;
  double p999_latency_us = 0.0;
  std::uint64_t connection_success = 0;
  std::uint64_t connection_failed = 0;
  std::uint64_t send_failed = 0;
  QueueMetrics queue_metrics{};
  ThreadPoolMetrics thread_pool_metrics{};
  CapacityExecutorMetrics executor_metrics{};
  IngressMetrics ingress_metrics{};
};

// ============================================================================
// 全局数据与互斥量
// ============================================================================

std::mutex g_e2e_latency_mu;
std::vector<std::uint64_t> g_e2e_latencies_ns;  // client send -> executor complete
std::atomic<std::uint64_t> g_completed_tasks{0};
std::atomic<std::uint64_t> g_connection_success{0};
std::atomic<std::uint64_t> g_connection_failed{0};
std::atomic<std::uint64_t> g_send_failed{0};

constexpr std::uint32_t kPayloadHeaderBytes = 13;  // op_type(1) + sequence(4) + client_send_ts(8)

// ============================================================================
// 时间戳工具函数
// ============================================================================

std::uint64_t NowNs() {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

void WriteBe32(std::vector<std::uint8_t>* bytes, std::uint32_t offset, std::uint32_t value) {
  (*bytes)[offset + 0] = static_cast<std::uint8_t>((value >> 24U) & 0xFFU);
  (*bytes)[offset + 1] = static_cast<std::uint8_t>((value >> 16U) & 0xFFU);
  (*bytes)[offset + 2] = static_cast<std::uint8_t>((value >> 8U) & 0xFFU);
  (*bytes)[offset + 3] = static_cast<std::uint8_t>(value & 0xFFU);
}

void WriteBe64(std::vector<std::uint8_t>* bytes, std::uint32_t offset, std::uint64_t value) {
  for (std::uint32_t i = 0; i < 8U; ++i) {
    const std::uint32_t shift = (7U - i) * 8U;
    (*bytes)[offset + i] = static_cast<std::uint8_t>((value >> shift) & 0xFFU);
  }
}

std::uint64_t ReadBe64(const std::uint8_t* bytes) {
  std::uint64_t value = 0;
  for (std::uint32_t i = 0; i < 8U; ++i) {
    value = (value << 8U) | static_cast<std::uint64_t>(bytes[i]);
  }
  return value;
}

bool IsPowerOfTwo(std::uint32_t value) {
  return value != 0U && (value & (value - 1U)) == 0U;
}

// ============================================================================
// 端到端延迟采集 Executor 包装器
// ============================================================================

void E2EBenchmarkExecutor(const Task& task) {
  // 先执行容量模式执行器逻辑（轻量解析与校验）
  CapacityModeExecutor(task);

  // 从 payload 读取 client send timestamp
  std::uint64_t e2e_latency_ns = 0;
  if (task.payload != nullptr && task.payload_size >= kPayloadHeaderBytes) {
    const auto* bytes = static_cast<const std::uint8_t*>(task.payload);
    const std::uint64_t client_send_ns = ReadBe64(bytes + 5);  // op_type(1) + sequence(4) + client_send_ts(8)
    const std::uint64_t now_ns = NowNs();
    e2e_latency_ns = (now_ns > client_send_ns) ? (now_ns - client_send_ns) : 0U;
  }

  {
    std::lock_guard<std::mutex> lock(g_e2e_latency_mu);
    g_e2e_latencies_ns.push_back(e2e_latency_ns);
  }
  g_completed_tasks.fetch_add(1, std::memory_order_relaxed);
}

void ResetE2ECollector(std::uint64_t expected_tasks) {
  std::lock_guard<std::mutex> lock(g_e2e_latency_mu);
  g_e2e_latencies_ns.clear();
  g_e2e_latencies_ns.reserve(static_cast<std::size_t>(expected_tasks));
  g_completed_tasks.store(0, std::memory_order_relaxed);
  g_connection_success.store(0, std::memory_order_relaxed);
  g_connection_failed.store(0, std::memory_order_relaxed);
  g_send_failed.store(0, std::memory_order_relaxed);
}

// ============================================================================
// 参数解析与验证
// ============================================================================

bool ParseUnsigned64(const std::string& text, std::uint64_t* out) {
  if (out == nullptr || text.empty()) return false;
  try {
    *out = std::stoull(text);
    return true;
  } catch (...) {
    return false;
  }
}

bool ParseUnsigned32(const std::string& text, std::uint32_t* out) {
  if (out == nullptr || text.empty()) return false;
  try {
    const auto val = std::stoul(text);
    if (val > 0xFFFFFFFFU) return false;
    *out = static_cast<std::uint32_t>(val);
    return true;
  } catch (...) {
    return false;
  }
}

bool ParseE2EConfig(int argc, const char** argv, E2EBenchmarkConfig* out) {
  if (out == nullptr) return false;

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];

    if (arg == "--tasks" && i + 1 < argc) {
      if (!ParseUnsigned64(argv[++i], &out->total_tasks) || out->total_tasks == 0) {
        std::cerr << "Error: --tasks must be > 0" << std::endl;
        return false;
      }
    } else if (arg == "--threads" && i + 1 < argc) {
      if (!ParseUnsigned32(argv[++i], &out->worker_threads) || out->worker_threads == 0) {
        std::cerr << "Error: --threads must be > 0" << std::endl;
        return false;
      }
    } else if (arg == "--queue-capacity" && i + 1 < argc) {
      if (!ParseUnsigned32(argv[++i], &out->queue_capacity)) {
        std::cerr << "Error: --queue-capacity parse failed" << std::endl;
        return false;
      }
      if (!IsPowerOfTwo(out->queue_capacity)) {
        std::cerr << "Error: --queue-capacity must be power of 2" << std::endl;
        return false;
      }
    } else if (arg == "--payload-bytes" && i + 1 < argc) {
      if (!ParseUnsigned32(argv[++i], &out->payload_bytes) || out->payload_bytes < kPayloadHeaderBytes) {
        std::cerr << "Error: --payload-bytes must be >= " << kPayloadHeaderBytes << std::endl;
        return false;
      }
    } else if (arg == "--client-connections" && i + 1 < argc) {
      if (!ParseUnsigned32(argv[++i], &out->client_connections) || out->client_connections == 0) {
        std::cerr << "Error: --client-connections must be > 0" << std::endl;
        return false;
      }
    } else if (arg == "--port" && i + 1 < argc) {
      if (!ParseUnsigned32(argv[++i], &out->bind_port) || out->bind_port == 0 || out->bind_port > 65535) {
        std::cerr << "Error: --port must be in (0, 65535]" << std::endl;
        return false;
      }
    } else if (arg == "--output-format" && i + 1 < argc) {
      out->output_format = argv[++i];
      if (out->output_format != "json" && out->output_format != "csv") {
        std::cerr << "Error: --output-format must be json or csv" << std::endl;
        return false;
      }
    } else if (arg == "--output-file" && i + 1 < argc) {
      out->output_file = argv[++i];
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "Usage: " << argv[0] << " [OPTIONS]\n"
                << "Options:\n"
                << "  --tasks N                  Total tasks to send (default: 20000)\n"
                << "  --threads N                Worker thread count (default: 4)\n"
                << "  --queue-capacity N         Queue capacity, power of 2 (default: 65536)\n"
                << "  --payload-bytes N          Payload size (default: 256)\n"
                << "  --client-connections N     Client connection count (default: 4)\n"
                << "  --port N                   Server bind port (default: 9999)\n"
                << "  --output-format FMT        json or csv (default: json)\n"
                << "  --output-file FILE         Write result to file\n"
                << "  --help                     Show this help\n";
      return false;
    }
  }

  return true;
}

// ============================================================================
// 统计与输出函数
// ============================================================================

double CalculatePercentile(const std::vector<std::uint64_t>& sorted, double percentile) {
  if (sorted.empty()) return 0.0;
  const size_t idx = std::max(size_t{0}, static_cast<size_t>(sorted.size() * percentile / 100.0 - 1.0));
  return static_cast<double>(sorted[idx]) / 1000.0;  // ns -> us
}

void OutputJsonResult(const E2EBenchmarkResult& result, const std::string& output_file) {
  std::ostream* out_stream = &std::cout;
  std::ofstream file_stream;
  if (!output_file.empty()) {
    file_stream.open(output_file);
    if (!file_stream.is_open()) {
      std::cerr << "Error: cannot open output file " << output_file << std::endl;
      return;
    }
    out_stream = &file_stream;
  }

  *out_stream << "{\n";
  *out_stream << "  \"total_tasks\": " << result.total_tasks << ",\n";
  *out_stream << "  \"worker_threads\": " << result.worker_threads << ",\n";
  *out_stream << "  \"queue_capacity\": " << result.queue_capacity << ",\n";
  *out_stream << "  \"payload_bytes\": " << result.payload_bytes << ",\n";
  *out_stream << "  \"client_connections\": " << result.client_connections << ",\n";
  *out_stream << "  \"duration_ms\": " << std::fixed << std::setprecision(2) << result.duration_ms << ",\n";
  *out_stream << "  \"throughput_fps\": " << std::fixed << std::setprecision(2) << result.throughput_fps << ",\n";
  *out_stream << "  \"throughput_tps\": " << std::fixed << std::setprecision(2) << result.throughput_tps << ",\n";
  *out_stream << "  \"latency_us\": {\n";
  *out_stream << "    \"p50\": " << std::fixed << std::setprecision(2) << result.p50_latency_us << ",\n";
  *out_stream << "    \"p99\": " << std::fixed << std::setprecision(2) << result.p99_latency_us << ",\n";
  *out_stream << "    \"p999\": " << std::fixed << std::setprecision(2) << result.p999_latency_us << "\n";
  *out_stream << "  },\n";
  *out_stream << "  \"stability\": {\n";
  *out_stream << "    \"connection_success\": " << result.connection_success << ",\n";
  *out_stream << "    \"connection_failed\": " << result.connection_failed << ",\n";
  *out_stream << "    \"send_failed\": " << result.send_failed << "\n";
  *out_stream << "  },\n";
  *out_stream << "  \"queue_metrics\": {\n";
  *out_stream << "    \"capacity\": " << result.queue_metrics.capacity << ",\n";
  *out_stream << "    \"size\": " << result.queue_metrics.size << ",\n";
  *out_stream << "    \"enqueue_success\": " << result.queue_metrics.enqueue_success << ",\n";
  *out_stream << "    \"enqueue_timeout\": " << result.queue_metrics.enqueue_timeout << "\n";
  *out_stream << "  },\n";
  *out_stream << "  \"thread_pool_metrics\": {\n";
  *out_stream << "    \"configured_threads\": " << result.thread_pool_metrics.configured_threads << ",\n";
  *out_stream << "    \"active_threads\": " << result.thread_pool_metrics.active_threads << ",\n";
  *out_stream << "    \"executed_tasks\": " << result.thread_pool_metrics.executed_tasks << "\n";
  *out_stream << "  },\n";
  *out_stream << "  \"executor_metrics\": {\n";
  *out_stream << "    \"executed_tasks\": " << result.executor_metrics.executed_tasks << ",\n";
  *out_stream << "    \"invalid_payload_tasks\": " << result.executor_metrics.invalid_payload_tasks << ",\n";
  *out_stream << "    \"parsed_header_tasks\": " << result.executor_metrics.parsed_header_tasks << "\n";
  *out_stream << "  },\n";
  *out_stream << "  \"ingress_metrics\": {\n";
  *out_stream << "    \"accepted_connections\": " << result.ingress_metrics.accepted_connections << ",\n";
  *out_stream << "    \"active_connections\": " << result.ingress_metrics.active_connections << ",\n";
  *out_stream << "    \"completed_frames\": " << result.ingress_metrics.completed_frames << ",\n";
  *out_stream << "    \"frame_decode_error\": " << result.ingress_metrics.frame_decode_error << ",\n";
  *out_stream << "    \"dropped_on_backpressure\": " << result.ingress_metrics.dropped_on_backpressure << "\n";
  *out_stream << "  }\n";
  *out_stream << "}\n";

  if (!output_file.empty()) {
    std::cout << "Result written to " << output_file << std::endl;
  }
}

// ============================================================================
// TCP 客户端逻辑
// ============================================================================

class E2ETcpClient {
 public:
  E2ETcpClient(std::uint32_t port, std::uint32_t payload_bytes)
      : port_(port), payload_bytes_(payload_bytes) {}

  void SendTasksFromConnection(std::uint64_t tasks_per_conn) {
    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
      g_connection_failed.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    struct sockaddr_in server_addr {};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port_);
    server_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (::connect(sock, reinterpret_cast<const struct sockaddr*>(&server_addr),
                  sizeof(server_addr)) < 0) {
      g_connection_failed.fetch_add(1, std::memory_order_relaxed);
      ::close(sock);
      return;
    }

    g_connection_success.fetch_add(1, std::memory_order_relaxed);

    // 构造长度前缀帧
    std::vector<std::uint8_t> payload(payload_bytes_);

    for (std::uint64_t seq = 0; seq < tasks_per_conn; ++seq) {
      // 构造帧头 13 字节: length_field(4) + op_type(1) + sequence(4) + client_send_ts(8)
      const std::uint32_t frame_length = payload_bytes_ - 4;  // 不含长度字段本身
      WriteBe32(&payload, 0, frame_length);

      const std::uint8_t op_type = static_cast<std::uint8_t>(seq & 0xFF);
      payload[4] = op_type;

      WriteBe32(&payload, 5, static_cast<std::uint32_t>(seq & 0xFFFFFFFFU));

      const std::uint64_t client_send_ns = NowNs();
      WriteBe64(&payload, 9, client_send_ns);

      // 填充剩余字节（可选，这里置 0）
      for (std::uint32_t i = 17; i < payload_bytes_; ++i) {
        payload[i] = 0;
      }

      // 发送整个 payload（含长度字段）
      ssize_t sent = ::send(sock, payload.data(), payload.size(), 0);
      if (sent < 0 || static_cast<size_t>(sent) != payload.size()) {
        g_send_failed.fetch_add(1, std::memory_order_relaxed);
      }
    }

    ::close(sock);
  }

 private:
  std::uint32_t port_;
  std::uint32_t payload_bytes_;
};

// ============================================================================
// Server 侧事件循环
// ============================================================================

Status RunE2EServer(std::shared_ptr<RingQueue> queue, std::unique_ptr<ThreadPool> pool,
                     std::unique_ptr<NetworkIngress> ingress, std::uint64_t expected_tasks,
                     std::uint32_t poll_timeout_ms) {
  // 等待所有任务完成（或超时）
  const auto start_time = std::chrono::steady_clock::now();
  const auto max_wait = std::chrono::seconds(300);  // 5 分钟超时

  while (g_completed_tasks.load(std::memory_order_relaxed) < expected_tasks) {
    auto now = std::chrono::steady_clock::now();
    if (now - start_time > max_wait) {
      std::cerr << "Warning: Server timeout, completing early" << std::endl;
      break;
    }

    // 每轮轮询，处理网络事件
    Status s = ingress->PollOnce(poll_timeout_ms);
    if (s.code != ErrorCode::kOk && s.code != ErrorCode::kBackpressure) {
      std::cerr << "PollOnce failed: " << s.message << std::endl;
      break;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  return Status{ErrorCode::kOk, "Server loop completed"};
}

// ============================================================================
// 主基准程序
// ============================================================================

bool RunE2EBenchmark(const E2EBenchmarkConfig& config, E2EBenchmarkResult* result) {
  if (result == nullptr) return false;

  // 记录配置
  result->total_tasks = config.total_tasks;
  result->worker_threads = config.worker_threads;
  result->queue_capacity = config.queue_capacity;
  result->payload_bytes = config.payload_bytes;
  result->client_connections = config.client_connections;

  // 1. 创建队列与线程池
  auto queue_unique = MakeCasRingQueue(config.queue_capacity);
  if (queue_unique == nullptr) {
    std::cerr << "Failed to create queue" << std::endl;
    return false;
  }
  auto queue = std::shared_ptr<RingQueue>(std::move(queue_unique));

  auto pool = MakeThreadPool(queue);
  if (pool == nullptr) {
    std::cerr << "Failed to create thread pool" << std::endl;
    return false;
  }

  Status s = pool->Start(config.worker_threads);
  if (!s.Ok()) {
    std::cerr << "Failed to start thread pool: " << s.message << std::endl;
    return false;
  }

  // 2. 创建 FrameDecoder
  FrameConfig frame_config_tmp{
      .length_field_bytes = 4,
      .max_frame_length = 1 << 20,  // 1 MiB
      .network_byte_order = true,
  };
  auto decoder = MakeFrameDecoder(frame_config_tmp);
  if (decoder == nullptr) {
    std::cerr << "Failed to create frame decoder" << std::endl;
    pool->Stop(5000);
    return false;
  }

  // 3. 创建 NetworkIngress with custom executor
  auto ingress = MakeEpollNetworkIngressWithExecutor(queue, std::move(decoder), E2EBenchmarkExecutor);
  if (ingress == nullptr) {
    std::cerr << "Failed to create network ingress" << std::endl;
    pool->Stop(5000);
    return false;
  }

  // 4. 启动 NetworkIngress
  IngressConfig ingress_config{
      .backend_type = IngressBackendType::kEpoll,
      .bind_ip = "127.0.0.1",
      .bind_port = config.bind_port,
      .listen_backlog = 128,
      .recv_buffer_size = 65536,
      .send_buffer_size = 65536,
      .max_connections = 10000,
      .max_events_per_poll = 1024,
  };

  FrameConfig frame_config{
      .length_field_bytes = 4,
      .max_frame_length = 1 << 20,  // 1 MiB
      .network_byte_order = true,
  };

  s = ingress->Start(ingress_config, frame_config);
  if (!s.Ok()) {
    std::cerr << "Failed to start ingress: " << s.message << std::endl;
    pool->Stop(5000);
    return false;
  }

  // 5. 重置采集器
  ResetE2ECollector(config.total_tasks);
  ResetCapacityExecutorMetrics();

  // 6. 启动时钟
  const auto benchmark_start = std::chrono::steady_clock::now();

  // 7. 启动 Client 线程，并发发送任务
  std::vector<std::thread> client_threads;
  const std::uint64_t tasks_per_conn = (config.total_tasks + config.client_connections - 1) / config.client_connections;

  for (std::uint32_t i = 0; i < config.client_connections; ++i) {
    client_threads.emplace_back([&config, tasks_per_conn]() {
      E2ETcpClient client(config.bind_port, config.payload_bytes);
      client.SendTasksFromConnection(tasks_per_conn);
    });
  }

  // 8. Server 侧跑事件循环，等待任务完成
  s = RunE2EServer(queue, std::move(pool), std::move(ingress), config.total_tasks, 100);

  // 9. 等待 Client 线程全部退出
  for (auto& t : client_threads) {
    t.join();
  }

  // 10. 停止 ingress 与 pool
  ingress->Stop(5000);
  pool->Stop(5000);

  // 11. 记录时间
  const auto benchmark_end = std::chrono::steady_clock::now();
  const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(benchmark_end - benchmark_start);
  result->duration_ms = static_cast<double>(duration.count());

  // 12. 计算统计
  {
    std::lock_guard<std::mutex> lock(g_e2e_latency_mu);
    if (!g_e2e_latencies_ns.empty()) {
      auto sorted_latencies = g_e2e_latencies_ns;
      std::sort(sorted_latencies.begin(), sorted_latencies.end());

      result->p50_latency_us = CalculatePercentile(sorted_latencies, 50.0);
      result->p99_latency_us = CalculatePercentile(sorted_latencies, 99.0);
      result->p999_latency_us = CalculatePercentile(sorted_latencies, 99.9);
    }
  }

  result->throughput_fps = (result->duration_ms > 0.0)
                               ? static_cast<double>(config.total_tasks) * 1000.0 / result->duration_ms
                               : 0.0;
  result->throughput_tps = result->throughput_fps;

  result->connection_success = g_connection_success.load(std::memory_order_relaxed);
  result->connection_failed = g_connection_failed.load(std::memory_order_relaxed);
  result->send_failed = g_send_failed.load(std::memory_order_relaxed);

  result->queue_metrics = queue->Metrics();
  result->thread_pool_metrics = pool->Metrics();
  result->executor_metrics = GetCapacityExecutorMetrics();
  result->ingress_metrics = ingress->Metrics();

  return true;
}

}  // namespace
}  // namespace robotaxi

// ============================================================================
// Main Entry Point
// ============================================================================

int main(int argc, const char** argv) {
  using namespace robotaxi;

  E2EBenchmarkConfig config;
  if (!ParseE2EConfig(argc, argv, &config)) {
    return 1;
  }

  std::cout << "E2E Benchmark Configuration:\n"
            << "  Total Tasks: " << config.total_tasks << "\n"
            << "  Worker Threads: " << config.worker_threads << "\n"
            << "  Queue Capacity: " << config.queue_capacity << "\n"
            << "  Payload Bytes: " << config.payload_bytes << "\n"
            << "  Client Connections: " << config.client_connections << "\n"
            << "  Bind Port: " << config.bind_port << "\n"
            << std::endl;

  E2EBenchmarkResult result;
  if (!RunE2EBenchmark(config, &result)) {
    std::cerr << "Benchmark failed" << std::endl;
    return 1;
  }

  std::cout << "\nE2E Benchmark Result:\n"
            << "  Duration: " << result.duration_ms << " ms\n"
            << "  Throughput: " << result.throughput_fps << " fps\n"
            << "  P50 Latency: " << result.p50_latency_us << " us\n"
            << "  P99 Latency: " << result.p99_latency_us << " us\n"
            << "  P999 Latency: " << result.p999_latency_us << " us\n"
            << "  Connection Success: " << result.connection_success << "\n"
            << "  Connection Failed: " << result.connection_failed << "\n"
            << "  Send Failed: " << result.send_failed << "\n"
            << std::endl;

  OutputJsonResult(result, config.output_file);

  return 0;
}
