#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "robotaxi/capacity_executor.h"
#include "robotaxi/ring_queue.h"
#include "robotaxi/status.h"
#include "robotaxi/thread_pool.h"
#include "robotaxi/types.h"

namespace robotaxi {
namespace {

struct BenchmarkConfig {
  std::uint64_t task_count = 200000;
  std::uint32_t worker_threads = 8;
  std::uint32_t queue_capacity = 65536;
  std::uint32_t payload_bytes = 256;
  std::string output_format = "json";
  std::string output_file;
};

struct BenchmarkResult {
  std::uint64_t task_count = 0;
  std::uint32_t worker_threads = 0;
  std::uint32_t queue_capacity = 0;
  std::uint32_t payload_bytes = 0;
  double duration_ms = 0.0;
  double throughput_tps = 0.0;
  double p99_latency_us = 0.0;
  double p999_latency_us = 0.0;
  double cas_retry_rate = 0.0;
  double worker_empty_poll_ratio = 0.0;
  QueueMetrics queue_metrics{};
  ThreadPoolMetrics thread_pool_metrics{};
  CapacityExecutorMetrics executor_metrics{};
};

std::mutex g_latency_mu;
std::vector<std::uint64_t> g_latencies_ns;
std::atomic<std::uint64_t> g_completed_tasks{0};

constexpr std::uint32_t kHeaderTimestampOffset = 5;
constexpr std::uint32_t kHeaderBytesWithTimestamp = 13;

bool IsPowerOfTwo(const std::uint32_t value) {
  return value != 0U && (value & (value - 1U)) == 0U;
}

std::uint64_t NowNs() {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

void WriteBe32(std::vector<std::uint8_t>* bytes, const std::uint32_t offset, const std::uint32_t value) {
  (*bytes)[offset + 0] = static_cast<std::uint8_t>((value >> 24U) & 0xFFU);
  (*bytes)[offset + 1] = static_cast<std::uint8_t>((value >> 16U) & 0xFFU);
  (*bytes)[offset + 2] = static_cast<std::uint8_t>((value >> 8U) & 0xFFU);
  (*bytes)[offset + 3] = static_cast<std::uint8_t>(value & 0xFFU);
}

void WriteBe64(std::vector<std::uint8_t>* bytes, const std::uint32_t offset, const std::uint64_t value) {
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

void BenchmarkExecutor(const Task& task) {
  CapacityModeExecutor(task);

  std::uint64_t latency_ns = 0;
  if (task.payload != nullptr && task.payload_size >= kHeaderBytesWithTimestamp) {
    const auto* bytes = static_cast<const std::uint8_t*>(task.payload);
    const std::uint64_t enqueue_ns = ReadBe64(bytes + kHeaderTimestampOffset);
    const std::uint64_t now_ns = NowNs();
    latency_ns = (now_ns > enqueue_ns) ? (now_ns - enqueue_ns) : 0U;
  }

  {
    std::lock_guard<std::mutex> lock(g_latency_mu);
    g_latencies_ns.push_back(latency_ns);
  }
  g_completed_tasks.fetch_add(1, std::memory_order_relaxed);
}

void ResetBenchmarkCollector(const std::uint64_t expected_tasks) {
  std::lock_guard<std::mutex> lock(g_latency_mu);
  g_latencies_ns.clear();
  g_latencies_ns.reserve(static_cast<std::size_t>(expected_tasks));
  g_completed_tasks.store(0, std::memory_order_relaxed);
}

bool ParseUnsigned64(const std::string& text, std::uint64_t* out) {
  if (out == nullptr || text.empty()) {
    return false;
  }
  try {
    const auto value = std::stoull(text);
    *out = static_cast<std::uint64_t>(value);
    return true;
  } catch (...) {
    return false;
  }
}

bool ParseUnsigned32(const std::string& text, std::uint32_t* out) {
  if (out == nullptr || text.empty()) {
    return false;
  }
  try {
    const auto value = std::stoul(text);
    if (value > static_cast<unsigned long>(std::numeric_limits<std::uint32_t>::max())) {
      return false;
    }
    *out = static_cast<std::uint32_t>(value);
    return true;
  } catch (...) {
    return false;
  }
}

void PrintUsage() {
  std::cerr
      << "用法:\n"
      << "  ./capacity_benchmark [--tasks N] [--threads N] [--queue-capacity N]"
      << " [--payload-bytes N] [--output-format json|csv] [--output-file path]\n";
}

Status ParseArgs(int argc, char** argv, BenchmarkConfig* config) {
  if (config == nullptr) {
    return {ErrorCode::kInvalidArgument, "config is null"};
  }

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto require_value = [&](const char* name) -> const char* {
      if (i + 1 >= argc) {
        std::cerr << "参数 " << name << " 缺少取值\n";
        return nullptr;
      }
      ++i;
      return argv[i];
    };

    if (arg == "--tasks") {
      const char* value = require_value("--tasks");
      if (value == nullptr || !ParseUnsigned64(value, &config->task_count)) {
        return {ErrorCode::kInvalidArgument, "--tasks 非法"};
      }
      continue;
    }
    if (arg == "--threads") {
      const char* value = require_value("--threads");
      if (value == nullptr || !ParseUnsigned32(value, &config->worker_threads)) {
        return {ErrorCode::kInvalidArgument, "--threads 非法"};
      }
      continue;
    }
    if (arg == "--queue-capacity") {
      const char* value = require_value("--queue-capacity");
      if (value == nullptr || !ParseUnsigned32(value, &config->queue_capacity)) {
        return {ErrorCode::kInvalidArgument, "--queue-capacity 非法"};
      }
      continue;
    }
    if (arg == "--payload-bytes") {
      const char* value = require_value("--payload-bytes");
      if (value == nullptr || !ParseUnsigned32(value, &config->payload_bytes)) {
        return {ErrorCode::kInvalidArgument, "--payload-bytes 非法"};
      }
      continue;
    }
    if (arg == "--output-format") {
      const char* value = require_value("--output-format");
      if (value == nullptr) {
        return {ErrorCode::kInvalidArgument, "--output-format 缺值"};
      }
      config->output_format = value;
      continue;
    }
    if (arg == "--output-file") {
      const char* value = require_value("--output-file");
      if (value == nullptr) {
        return {ErrorCode::kInvalidArgument, "--output-file 缺值"};
      }
      config->output_file = value;
      continue;
    }
    if (arg == "--help" || arg == "-h") {
      PrintUsage();
      return {ErrorCode::kInvalidArgument, "help requested"};
    }

    std::cerr << "未知参数: " << arg << "\n";
    return {ErrorCode::kInvalidArgument, "unknown arg"};
  }

  if (config->task_count == 0U) {
    return {ErrorCode::kInvalidArgument, "--tasks 必须 > 0"};
  }
  if (config->worker_threads == 0U) {
    return {ErrorCode::kInvalidArgument, "--threads 必须 > 0"};
  }
  if (!IsPowerOfTwo(config->queue_capacity) || config->queue_capacity < 2U) {
    return {ErrorCode::kInvalidArgument, "--queue-capacity 必须为 >=2 的 2 次幂"};
  }
  if (config->payload_bytes < kHeaderBytesWithTimestamp) {
    return {ErrorCode::kInvalidArgument, "--payload-bytes 必须 >= 13"};
  }
  if (config->output_format != "json" && config->output_format != "csv") {
    return {ErrorCode::kInvalidArgument, "--output-format 仅支持 json/csv"};
  }

  return Status::Success();
}

std::size_t PercentileIndex(const std::size_t n, const double p) {
  if (n == 0U) {
    return 0U;
  }
  const double rank = std::ceil(p * static_cast<double>(n));
  const auto idx = static_cast<std::size_t>(rank <= 1.0 ? 0.0 : (rank - 1.0));
  return (idx >= n) ? (n - 1U) : idx;
}

BenchmarkResult RunBenchmark(const BenchmarkConfig& cfg) {
  BenchmarkResult result{};
  result.task_count = cfg.task_count;
  result.worker_threads = cfg.worker_threads;
  result.queue_capacity = cfg.queue_capacity;
  result.payload_bytes = cfg.payload_bytes;

  auto queue = std::make_shared<CasRingQueue>(cfg.queue_capacity);
  auto pool = MakeThreadPool(queue);

  const Status start_status = pool->Start(cfg.worker_threads);
  if (!start_status.Ok()) {
    throw std::runtime_error(std::string("线程池启动失败: ") + start_status.message);
  }

  ResetCapacityExecutorMetrics();
  ResetBenchmarkCollector(cfg.task_count);

  std::vector<std::unique_ptr<std::vector<std::uint8_t>>> payload_store;
  payload_store.reserve(static_cast<std::size_t>(cfg.task_count));

  const std::uint64_t begin_ns = NowNs();

  for (std::uint64_t i = 0; i < cfg.task_count; ++i) {
    auto payload = std::make_unique<std::vector<std::uint8_t>>(cfg.payload_bytes, 0x5AU);
    (*payload)[0] = static_cast<std::uint8_t>(i & 0xFFU);
    WriteBe32(payload.get(), 1, static_cast<std::uint32_t>(i & 0xFFFFFFFFU));
    WriteBe64(payload.get(), kHeaderTimestampOffset, NowNs());

    Task task{
        .task_id = i + 1U,
        .payload = payload->data(),
        .payload_size = static_cast<std::uint32_t>(payload->size()),
        .response_handle = payload.get(),
        .executor = &BenchmarkExecutor,
    };

    const Status submit_status = pool->Submit(task, 1000);
    if (!submit_status.Ok()) {
      (void)pool->Stop(30000);
      throw std::runtime_error(std::string("任务提交失败: ") + submit_status.message);
    }

    payload_store.emplace_back(std::move(payload));
  }

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::minutes(5);
  while (g_completed_tasks.load(std::memory_order_relaxed) < cfg.task_count) {
    if (std::chrono::steady_clock::now() >= deadline) {
      (void)pool->Stop(30000);
      throw std::runtime_error("等待任务完成超时");
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  const std::uint64_t end_ns = NowNs();

  result.queue_metrics = queue->Metrics();
  result.thread_pool_metrics = pool->Metrics();
  result.executor_metrics = GetCapacityExecutorMetrics();

  const Status stop_status = pool->Stop(30000);
  if (!stop_status.Ok()) {
    throw std::runtime_error(std::string("线程池停止失败: ") + stop_status.message);
  }

  std::vector<std::uint64_t> latencies;
  {
    std::lock_guard<std::mutex> lock(g_latency_mu);
    latencies = g_latencies_ns;
  }

  std::sort(latencies.begin(), latencies.end());

  const double elapsed_ns = static_cast<double>(end_ns - begin_ns);
  result.duration_ms = elapsed_ns / 1e6;
  result.throughput_tps = (elapsed_ns <= 0.0) ? 0.0 : (static_cast<double>(cfg.task_count) * 1e9 / elapsed_ns);

  if (!latencies.empty()) {
    const auto idx_p99 = PercentileIndex(latencies.size(), 0.99);
    const auto idx_p999 = PercentileIndex(latencies.size(), 0.999);
    result.p99_latency_us = static_cast<double>(latencies[idx_p99]) / 1e3;
    result.p999_latency_us = static_cast<double>(latencies[idx_p999]) / 1e3;
  }

  const double total_ops = static_cast<double>(result.queue_metrics.enqueue_success +
                                               result.queue_metrics.dequeue_success);
  result.cas_retry_rate = (total_ops <= 0.0)
                              ? 0.0
                              : (static_cast<double>(result.queue_metrics.cas_retry) / total_ops);

  const double empty_poll_denominator = static_cast<double>(result.thread_pool_metrics.executed_tasks +
                                                            result.thread_pool_metrics.worker_empty_poll);
  result.worker_empty_poll_ratio = (empty_poll_denominator <= 0.0)
                                       ? 0.0
                                       : (static_cast<double>(result.thread_pool_metrics.worker_empty_poll) /
                                          empty_poll_denominator);

  return result;
}

void PrintJson(const BenchmarkResult& r, std::ostream& os) {
  os << std::fixed << std::setprecision(3);
  os << "{\n";
  os << "  \"task_count\": " << r.task_count << ",\n";
  os << "  \"worker_threads\": " << r.worker_threads << ",\n";
  os << "  \"queue_capacity\": " << r.queue_capacity << ",\n";
  os << "  \"payload_bytes\": " << r.payload_bytes << ",\n";
  os << "  \"duration_ms\": " << r.duration_ms << ",\n";
  os << "  \"throughput_tps\": " << r.throughput_tps << ",\n";
  os << "  \"latency_us\": {\n";
  os << "    \"p99\": " << r.p99_latency_us << ",\n";
  os << "    \"p999\": " << r.p999_latency_us << "\n";
  os << "  },\n";
  os << "  \"efficiency\": {\n";
  os << "    \"cas_retry_rate\": " << r.cas_retry_rate << ",\n";
  os << "    \"worker_empty_poll_ratio\": " << r.worker_empty_poll_ratio << "\n";
  os << "  },\n";
  os << "  \"queue_metrics\": {\n";
  os << "    \"enqueue_success\": " << r.queue_metrics.enqueue_success << ",\n";
  os << "    \"dequeue_success\": " << r.queue_metrics.dequeue_success << ",\n";
  os << "    \"dequeue_empty\": " << r.queue_metrics.dequeue_empty << ",\n";
  os << "    \"cas_retry\": " << r.queue_metrics.cas_retry << "\n";
  os << "  },\n";
  os << "  \"thread_pool_metrics\": {\n";
  os << "    \"executed_tasks\": " << r.thread_pool_metrics.executed_tasks << ",\n";
  os << "    \"worker_empty_poll\": " << r.thread_pool_metrics.worker_empty_poll << ",\n";
  os << "    \"worker_block_wait\": " << r.thread_pool_metrics.worker_block_wait << "\n";
  os << "  },\n";
  os << "  \"capacity_executor_metrics\": {\n";
  os << "    \"executed_tasks\": " << r.executor_metrics.executed_tasks << ",\n";
  os << "    \"invalid_payload_tasks\": " << r.executor_metrics.invalid_payload_tasks << ",\n";
  os << "    \"parsed_header_tasks\": " << r.executor_metrics.parsed_header_tasks << ",\n";
  os << "    \"total_payload_bytes\": " << r.executor_metrics.total_payload_bytes << ",\n";
  os << "    \"checksum_accumulator\": " << r.executor_metrics.checksum_accumulator << "\n";
  os << "  }\n";
  os << "}\n";
}

void PrintCsv(const BenchmarkResult& r, std::ostream& os) {
  os << "task_count,worker_threads,queue_capacity,payload_bytes,duration_ms,throughput_tps,p99_latency_us,p999_latency_us,cas_retry_rate,worker_empty_poll_ratio\n";
  os << std::fixed << std::setprecision(3);
  os << r.task_count << ","
     << r.worker_threads << ","
     << r.queue_capacity << ","
     << r.payload_bytes << ","
     << r.duration_ms << ","
     << r.throughput_tps << ","
     << r.p99_latency_us << ","
     << r.p999_latency_us << ","
     << r.cas_retry_rate << ","
     << r.worker_empty_poll_ratio << "\n";
}

}  // namespace
}  // namespace robotaxi

int main(int argc, char** argv) {
  using namespace robotaxi;

  BenchmarkConfig cfg;
  const Status parse_status = ParseArgs(argc, argv, &cfg);
  if (!parse_status.Ok()) {
    if (parse_status.message != nullptr && std::string(parse_status.message) != "help requested") {
      std::cerr << "参数错误: " << parse_status.message << "\n";
      PrintUsage();
    }
    return 2;
  }

  try {
    const BenchmarkResult result = RunBenchmark(cfg);

    if (!cfg.output_file.empty()) {
      std::ofstream ofs(cfg.output_file, std::ios::out | std::ios::trunc);
      if (!ofs.is_open()) {
        std::cerr << "无法打开输出文件: " << cfg.output_file << "\n";
        return 3;
      }
      if (cfg.output_format == "csv") {
        PrintCsv(result, ofs);
      } else {
        PrintJson(result, ofs);
      }
    }

    if (cfg.output_format == "csv") {
      PrintCsv(result, std::cout);
    } else {
      PrintJson(result, std::cout);
    }
  } catch (const std::exception& ex) {
    std::cerr << "压测失败: " << ex.what() << "\n";
    return 1;
  }

  return 0;
}
