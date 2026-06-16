#ifndef ROBOTAXI_TYPES_H_
#define ROBOTAXI_TYPES_H_

#include <cstdint>

namespace robotaxi {

struct Task;

// 网络接入后端类型。
enum class IngressBackendType {
  kIoUring = 0,
  kEpoll = 1,
};

// 网络接入运行时配置。
struct IngressConfig {
  IngressBackendType backend_type;
  const char* bind_ip;
  std::uint16_t bind_port;
  std::uint32_t listen_backlog;
  std::uint32_t recv_buffer_size;
  std::uint32_t send_buffer_size;
  std::uint32_t max_connections;
  std::uint32_t max_events_per_poll;
};

// 固定长度前缀帧切分配置。
// 协议约束：
// 1) length_field_bytes 固定为 4。
// 2) max_frame_length 默认 1 MiB，且不得超过 16 MiB。
// 3) network_byte_order 固定为 true，表示长度字段按大端解析。
struct FrameConfig {
  std::uint32_t length_field_bytes;
  std::uint32_t max_frame_length;
  bool network_byte_order;
};

// 任务执行器函数签名。
// 调用语义：由线程池工作线程调用；函数内应保证异常自行处理，避免跨线程边界抛出未捕获异常。
using TaskExecutor = void (*)(const Task& task);

// 任务描述对象（生产者 -> 队列 -> 消费者）。
// 所有权与生命周期约束：
// 1) Task 对象本身可按值拷贝进队列槽位。
// 2) payload/response_handle 为外部资源句柄，队列不拥有其内存，也不负责释放。
// 3) 调用方必须保证 payload 在任务被消费并执行前保持有效。
struct Task {
  // 全局或局部唯一任务标识，用于追踪与排障。
  std::uint64_t task_id;
  // 业务负载只读指针，非 owning。
  const void* payload;
  // payload 字节长度。
  std::uint32_t payload_size;
  // 响应句柄，语义由上层协议栈定义，非 owning。
  void* response_handle;
  // 执行入口；为空时任务无效。
  TaskExecutor executor;
};

// 队列运行指标快照。
// 线程安全：调用方读取的是原子计数聚合快照，允许轻微读时偏差，但不破坏单调性。
struct QueueMetrics {
  // 队列总容量（槽位数）。
  std::uint32_t capacity;
  // 当前估算深度（tail - head）。
  std::uint32_t size;
  // 入队成功总次数。
  std::uint64_t enqueue_success;
  // 入队超时总次数。
  std::uint64_t enqueue_timeout;
  // 出队成功总次数。
  std::uint64_t dequeue_success;
  // 出队空队列总次数（含即时模式与超时路径统计）。
  std::uint64_t dequeue_empty;
  // CAS 冲突重试总次数。
  std::uint64_t cas_retry;
  // 背压触发计数（预留字段，后续 ingress 联动使用）。
  std::uint64_t backpressure_on;
  // 背压解除计数（预留字段，后续 ingress 联动使用）。
  std::uint64_t backpressure_off;
};

// 线程池运行指标快照。
// 线程安全：字段由线程池内部原子变量聚合，适用于趋势观测与健康检查。
struct ThreadPoolMetrics {
  // 配置的工作线程数。
  std::uint32_t configured_threads;
  // 当前仍在循环中的工作线程数。
  std::uint32_t active_threads;
  // 成功执行的任务总数。
  std::uint64_t executed_tasks;
  // 工作线程空轮询次数（未取到任务）。
  std::uint64_t worker_empty_poll;
  // 工作线程阻塞等待次数（通过短超时出队实现）。
  std::uint64_t worker_block_wait;
};

// 网络接入层指标快照。
struct IngressMetrics {
  std::uint64_t accepted_connections;
  std::uint64_t active_connections;
  std::uint64_t recv_bytes;
  std::uint64_t sent_bytes;
  std::uint64_t completed_frames;
  std::uint64_t frame_decode_error;
  std::uint64_t dropped_on_backpressure;
};

}  // namespace robotaxi

#endif  // ROBOTAXI_TYPES_H_
