# 规格说明（SPEC）

本文件为高并发服务端并发处理模块的唯一权威规格说明，定义网络接入、消息边界切分、CAS 队列、线程池执行、可观测与验收标准。

## 1. 范围与目标

### 1.1 目标

- 构建可复用的高并发请求处理模块，覆盖网络接入、消息切分、任务入队与线程池执行全链路。
- 网络接入层保留 io_uring + epoll 双后端抽象能力；当前阶段以 epoll 完成端到端闭环为硬目标，io_uring 暂不纳入本阶段硬验收。
- 使用固定长度前缀帧（length-prefixed）完成消息边界切分，并将消息投递到 CAS 无锁环形队列。
- 在 C++20 环境下提供稳定接口，支持 CMake 构建和 GoogleTest 验证。
- 在高竞争场景下保证任务处理正确性、无死锁与可观测。

### 1.2 非目标

- 不实现业务协议语义解析（如 HTTP 路由、gRPC 方法映射、业务字段验证）。
- 不实现 TLS 终止、证书管理、密钥轮换。
- 不提供 C 兼容 ABI，仅提供 C++ 接口。

### 1.3 业务场景与客户端模型（冻结草案）

- 本模块服务 Robotaxi 场景，客户端分为车端与乘客端。
- 车端规模按峰值 30 万并发在线进行容量规划。
- 车端负责持续状态同步（位置、车况等）与关键事件即时上报。
- 乘客端通过 HTTPS REST 发起约车与订单操作，通过 WebSocket 接收实时状态推送。
- 本阶段冻结协议路线：车端主链路为 gRPC Streaming（HTTP/2），HTTP/3 作为后续演进项，不纳入本阶段硬验收。

## 2. 术语与约束

### 2.1 术语定义

- 网络接入层（Network Ingress）：负责连接管理、读写事件处理和字节流收发。
- 帧切分层（Frame Decoder）：根据固定长度前缀规则切分完整消息边界，不解析业务字段。
- 任务（Task）：线程池可执行的工作单元，包含任务标识、负载指针与回调上下文。
- CAS 队列（CAS Ring Queue）：基于原子比较交换操作的多生产者多消费者环形缓冲区。
- 入队超时（Enqueue Timeout）：任务在指定时间内未成功入队即失败。
- 空轮询（Empty Poll）：工作线程出队失败且返回队列空。
- 至少一次投递（At-least-once）：消息可能重复到达但不应静默丢失，需通过幂等机制去重。
- 幂等窗口（Dedup Window）：对同一 message_id 进行去重的时间窗口，本阶段固定为 5 分钟。

### 2.2 运行与工具链约束

- 语言标准：C++20。
- 构建系统：CMake（>= 3.20）。
- 测试框架：GoogleTest。
- 目标平台：Linux x86_64 与 Linux aarch64。

## 3. 架构与职责边界

### 3.1 分层架构

1. 网络接入层（Network Ingress Layer）
  - 负责 accept/read/write/close 和事件循环。
  - 通过后端抽象支持 io_uring 与 epoll；当前阶段优先交付 epoll 闭环。
2. 帧切分层（Frame Decoder Layer）
  - 按固定长度前缀协议切分完整消息。
  - 生成待处理消息并交给任务构造逻辑。
3. 队列层（CAS Ring Queue Layer）
  - 负责无锁入队与出队。
  - 提供容量、深度与统计信息。
4. 执行层（Thread Pool Layer）
   - 工作线程从队列取任务并执行。
   - 管理线程生命周期、空闲等待与停止流程。
5. 可观测层（Observability Layer）
   - 暴露指标快照与关键日志事件。

### 3.2 职责边界

- 网络接入层负责字节流接入与连接生命周期管理，不处理业务语义。
- 帧切分层只做消息边界切分，不做业务字段解析。
- 队列层只做任务存储与并发访问控制，不执行任务。
- 线程池只做任务调度与执行，不解析业务负载。
- 模块边界外的业务协议语义解析与业务处理由上层系统负责。

## 4. API 契约

### 4.1 统一错误码

```cpp
enum class ErrorCode {
  Ok = 0,
  InvalidArgument = 1,
  IoError = 2,
  FrameError = 3,
  Backpressure = 4,
  QueueFull = 5,
  QueueEmpty = 6,
  Timeout = 7,
  NotRunning = 8,
  AlreadyRunning = 9,
  Internal = 10,
};

struct Status {
  ErrorCode code;
  const char* message;
};
```

### 4.2 基础类型

```cpp
enum class IngressBackendType {
  IoUring = 0,
  Epoll = 1,
};

struct IngressConfig {
  IngressBackendType backend_type;
  const char* bind_ip;
  uint16_t bind_port;
  uint32_t listen_backlog;
  uint32_t recv_buffer_size;
  uint32_t send_buffer_size;
  uint32_t listener_count;
  bool enable_reuseport;
  uint32_t max_connections;
  uint32_t max_events_per_poll;
};

struct FrameConfig {
  uint32_t length_field_bytes;   // 固定为 4
  uint32_t max_frame_length;     // 默认 1 MiB
  bool network_byte_order;       // 固定为 true（大端）
};

struct FrameView {
  const uint8_t* payload;
  uint32_t payload_size;
};

struct FrameDecodeResult {
  size_t bytes_consumed;
  std::vector<FrameView> completed_frames;
};

using TaskExecutor = void (*)(const struct Task& task);

struct Task {
  uint64_t task_id;
  const void* payload;
  uint32_t payload_size;
  void* response_handle;
  TaskExecutor executor;
};

struct QueueMetrics {
  uint32_t capacity;
  uint32_t size;
  uint64_t enqueue_success;
  uint64_t enqueue_timeout;
  uint64_t dequeue_success;
  uint64_t dequeue_empty;
  uint64_t cas_retry;
  uint64_t backpressure_on;
  uint64_t backpressure_off;
};

struct ThreadPoolMetrics {
  uint32_t configured_threads;
  uint32_t active_threads;
  uint64_t executed_tasks;
  uint64_t worker_empty_poll;
  uint64_t worker_block_wait;
};

struct IngressMetrics {
  uint64_t accepted_connections;
  uint64_t active_connections;
  uint64_t recv_bytes;
  uint64_t sent_bytes;
  uint64_t completed_frames;
  uint64_t frame_decode_error;
  uint64_t dropped_on_backpressure;
};
```

### 4.3 资源所有权与生命周期

#### 4.3.1 Payload 所有权与缓冲管理

- **网络接入层负责 payload 缓冲分配与释放**。每个入队的 Task 都伴随一个由 ingress 层分配的 `std::vector<uint8_t>` 缓冲，其中存储帧的完整 payload 数据。
- **缓冲生命周期覆盖出队后执行**：缓冲在 `Enqueue()` 成功时分配，在任务 executor 返回后立即回收；`Stop()` 仅负责兜底清理未执行任务残留缓冲。
- **Task.response_handle 用于缓冲追踪**：指向 ingress 内部缓冲句柄；执行器包装器在任务执行完成后回调 ingress 完成缓冲回收。
- **executor 无需显式管理 payload 所有权**：只读取 `Task.payload` 数据，无需释放。

#### 4.3.2 其他资源所有权

- Task 对象本体由调用方创建并管理。
- executor 不得抛出未捕获异常；若抛出，线程池必须捕获并记录错误。
- 网络层连接缓冲区与帧切分中间缓存由模块内部管理，不暴露跨层共享裸指针。

#### 4.3.3 当前阶段执行器策略（容量模式）

- 当前阶段默认执行器采用“容量模式”，目标是评估接入模块承载上限，而非模拟完整业务逻辑。
- 容量模式执行器仅允许：payload 读取、头字段轻量解析、基础合法性校验、聚合计数与轻量校验计算。
- 容量模式执行器禁止：外部网络调用、磁盘 IO、长时间锁等待、回包发送。
- 容量模式执行器应保持近似常量时间或线性读取复杂度（O(payload_size)），避免引入与接入框架无关的瓶颈。

### 4.4 帧切分接口

```cpp
class FrameDecoder {
 public:
  virtual ~FrameDecoder() = default;

  virtual Status Decode(const uint8_t* buffer,
                        size_t buffer_size,
                        FrameDecodeResult* out_result) const = 0;
  virtual FrameConfig Config() const = 0;
};
```

- Decode: 仅负责按固定长度前缀协议切出完整 payload，不解析业务字段，不创建 Task。
- 当 buffer 剩余字节不足以组成完整帧时，返回 Ok，且 out_result.bytes_consumed 仅覆盖已切出的完整帧。
- 当长度字段为 0 或超过 max_frame_length 时，返回 FrameError，且不得消费当前错误帧。
- out_result.completed_frames 中的 payload 为借用视图，仅在输入 buffer 保持有效期间可读。

### 4.5 网络接入接口

```cpp
class NetworkIngress {
 public:
  virtual ~NetworkIngress() = default;

  virtual Status Start(const IngressConfig& ingress_config,
                       const FrameConfig& frame_config) = 0;
  virtual Status Stop(uint32_t wait_timeout_ms) = 0;
  virtual Status PollOnce(uint32_t timeout_ms) = 0;
  virtual IngressBackendType BackendType() const = 0;
  virtual IngressMetrics Metrics() const = 0;
};
```

- Start: 初始化监听与事件循环；配置非法返回 InvalidArgument。
- PollOnce: 处理一次事件批次，包含连接处理、收包、帧切分和任务投递。
- 当队列处于背压态时，PollOnce 可返回 Backpressure，并执行降速读或暂停读事件。
- 对于已收到 FIN/RDHUP 的连接，必须先消费并切分当前缓冲中已到达的完整帧，再执行连接关闭；不得因为对端已关闭就跳过已接收数据的 decode。

### 4.5.1 NetworkIngress 实现基线决策（2026-06-16 冻结）

- 接收缓冲策略：采用“每连接独立接收缓冲区”。每个连接维护独立字节缓存，用于处理半包拼接与粘包拆分；不使用跨连接共享缓冲。
- 接收缓冲压缩策略：采用“消费偏移 + 按阈值压缩”。
  - `Decode()` 基于连接缓冲的未消费区间执行，不对每次消费执行线性 `erase(begin, begin + consumed)`。
  - 当已消费偏移达到压缩阈值且剩余数据占比低于阈值时，执行一次内存压缩；缓冲完全消费时直接 `clear + offset reset`。
- PollOnce 语义：采用工业常见的“阻塞式轮询 + 超时参数”模型。
  - timeout_ms == 0：非阻塞探测一次。
  - timeout_ms > 0：最多阻塞 timeout_ms 等待事件。
  - 超时无事件返回 Ok（非错误）。
- epoll 触发模式（第 2 步单变量实验）：
  - `listen_fd` 保持 LT（默认 `EPOLLIN`），优先保证 accept 行为可观测与稳定性。
  - `conn_fd` 采用 ET（`EPOLLIN | EPOLLET`），并保持“读到 `EAGAIN` 为止”的 drain 语义。
- 监听接入策略（当前阶段默认）：
  - 默认采用“单 ingress 实例 + 单 listen socket”。
  - `listen_fd` 默认仅启用 `SO_REUSEADDR`；`SO_REUSEPORT` 为可选能力，不作为默认路径。
  - 多 acceptor 改为显式配置能力：
    - `listener_count`: 监听 socket 数量，默认 1。
    - `enable_reuseport`: 是否启用 `SO_REUSEPORT`，默认 false。
    - 约束：当 `listener_count > 1` 时，`enable_reuseport` 必须为 true。
  - 仅当 `listener_count > 1` 且通过压测证据证明收益稳定时，才建议在生产环境启用。
- 背压策略：采用“高低水位迟滞 + 暂停读事件”。
  - 队列占用达到 HIGH_WATERMARK（85%）时，暂停连接读事件（必要时含监听 fd 读事件）。
  - 占用回落到 LOW_WATERMARK（60%）时恢复读事件。
  - PollOnce 在背压态可返回 Backpressure，供上层做降速与观测。

### 4.6 队列接口

```cpp
class RingQueue {
 public:
  virtual ~RingQueue() = default;

  virtual Status Enqueue(const Task& task, uint32_t timeout_ms) = 0;
  virtual Status Dequeue(Task* out_task, uint32_t timeout_ms) = 0;
  virtual uint32_t Capacity() const = 0;
  virtual uint32_t Size() const = 0;
  virtual QueueMetrics Metrics() const = 0;
};
```

- Enqueue: timeout_ms 内成功写入返回 Ok；超时返回 Timeout，且任务不入队。
- Dequeue: timeout_ms 内取到任务返回 Ok；超时返回 Timeout；即时模式下空队列返回 QueueEmpty。
- timeout_ms 为 0 表示即时模式，不阻塞等待。

### 4.7 线程池接口

```cpp
class ThreadPool {
 public:
  virtual ~ThreadPool() = default;

  virtual Status Start(uint32_t thread_count) = 0;
  virtual Status Stop(uint32_t wait_timeout_ms) = 0;
  virtual Status Submit(const Task& task, uint32_t timeout_ms) = 0;
  virtual uint32_t ActiveThreads() const = 0;
  virtual ThreadPoolMetrics Metrics() const = 0;
};
```

- Start: thread_count 必须大于 0，重复启动返回 AlreadyRunning。
- Stop: 在 wait_timeout_ms 内完成线程退出返回 Ok，否则返回 Timeout。
- Submit: 线程池未启动返回 NotRunning；其余语义与队列 Enqueue 一致。

### 4.8 固定长度前缀帧协议契约

- 长度字段固定 4 字节，采用网络字节序（大端）。
- 长度字段表示 payload 字节长度，不包含长度字段自身。
- 默认最大帧长度为 1 MiB，可配置但不得超过 16 MiB。
- 长度字段为 0 或超过 max_frame_length 判定为 FrameError。
- 发生 FrameError 时必须关闭对应连接，并递增 frame_decode_error 指标。

### 4.9 Robotaxi 通信契约（冻结草案）

- 车端通信：gRPC Streaming（HTTP/2）主链路；连接断开后必须自动重连。
- 乘客端通信：HTTPS REST 为主，WebSocket 用于实时推送。
- 车端常态上报频率：1Hz；关键事件即时上报。
- 关键事件集合：急刹、碰撞、故障、订单状态变更、偏航告警。
- 车端状态包估算：平均 1 KiB，P99 2 KiB（用于容量预算，后续可由实测回填）。
- 投递语义：至少一次；每条车端消息必须携带 message_id。
- 去重规则：服务端必须在 5 分钟窗口内对重复 message_id 去重。
- 弱网策略：支持断线重连 + 本地缓存 + 60 秒补传窗口。
- 补传规则：关键事件在 60 秒窗口内全量补传；常态状态仅上传最新快照，不执行全量补传。
- 乘客端推送频率：常态 1Hz；接驾阶段可提升到 2Hz。

## 5. 并发算法与线程安全

### 5.1 CAS 环形队列模型

- 使用固定容量环形数组，容量必须为 2 的幂。
- 使用 64 位游标表示 head 与 tail：高 32 位为轮次（epoch），低 32 位为槽位索引。
- 判空条件：head == tail。
- 判满条件：tail_index + 1 == head_index 且 tail_epoch == head_epoch；或 tail_index + 1 溢出并使 epoch 差为 1。

### 5.2 原子语义与内存序

- 读取 head/tail 使用 memory_order_acquire。
- CAS 更新使用 compare_exchange_weak 循环，成功路径使用 memory_order_acq_rel，失败路径使用 memory_order_acquire。
- 槽位写入在 tail CAS 成功后进行，槽位读取在 head CAS 成功后进行。
- 禁止使用非原子共享变量参与队列状态判断。

### 5.3 CAS 失败重试与防饥饿

- 默认 SPIN_THRESHOLD = 1024。
- 每次 CAS 失败累加 retry 计数；每 32 次失败执行一次 std::this_thread::yield()。
- 达到 SPIN_THRESHOLD 后进入短阻塞等待（默认 1ms），避免自旋风暴。
- 任一工作线程连续失败超过 MAX_CONSECUTIVE_FAIL = 10000 视为异常，必须记录 ERROR 级日志。

### 5.4 工作线程空闲策略

- 工作线程优先执行 Dequeue(out_task, 0) 快速拉取。
- 连续空轮询达到 EMPTY_POLL_THRESHOLD = 256 后，切换到 Dequeue(out_task, 1)。
- Stop 触发时必须广播唤醒所有阻塞线程，确保可控退出。

### 5.5 超时语义

- 入队超时语义：从接口调用开始计时，到成功完成 CAS 写入为止。
- 超时返回 Timeout 时，任务状态必须保持“未入队”。
- 出队超时语义：在 timeout_ms 窗口内未取到任务即超时，不改变队列状态。

### 5.6 背压策略

- 队列高水位阈值：HIGH_WATERMARK = 85%。
- 队列低水位阈值：LOW_WATERMARK = 60%。
- 队列占用率达到高水位后进入背压态：网络层暂停读事件或降低每轮读取上限。
- 占用率回落到低水位后退出背压态，恢复正常读事件。
- 背压状态切换必须更新 backpressure_on/backpressure_off 指标并记录 WARN 日志。
- 当前阶段联动判定只使用现有公开指标与返回码，不新增公开结构体字段：
  - 判定输入：`QueueMetrics.capacity`、`QueueMetrics.size`。
  - 判定输出：`PollOnce()` 返回 `Backpressure` 或 `Ok`。
  - 观测补充：`IngressMetrics.dropped_on_backpressure` 仅表示入队失败导致的丢弃，不等价于“是否处于背压态”。
- 占用率口径：`usage_percent = floor(size * 100 / capacity)`，按每次 `PollOnce()` 的队列快照计算。
- 迟滞联动约束（强制）：
  - 当 `usage_percent >= 85` 时，系统必须进入背压态，`PollOnce()` 允许返回 `Backpressure`。
  - 当 `usage_percent > 60` 且此前已进入背压态时，系统必须保持背压态，不得提前恢复。
  - 当 `usage_percent <= 60` 时，系统必须退出背压态，`PollOnce()` 应恢复返回 `Ok`（无其他错误前提下）。

## 6. 配置与可观测

### 6.1 运行时参数

| 参数名 | 类型 | 范围 | 默认值 | 生效方式 |
| --- | --- | --- | --- | --- |
| peak_online_vehicles | uint32_t | [100000, 500000] | 300000 | 容量规划固定 |
| ingress_backend | enum | io_uring / epoll | epoll | Start 时固定 |
| ingress_max_connections | uint32_t | [1024, 1,000,000] | 100000 | Start 时固定 |
| ingress_max_events_per_poll | uint32_t | [64, 8192] | 1024 | Start 时固定 |
| frame_max_length | uint32_t | [1024, 16 MiB] | 1 MiB | Start 时固定 |
| vehicle_status_payload_avg_bytes | uint32_t | [256, 4096] | 1024 | 容量规划固定 |
| vehicle_status_payload_p99_bytes | uint32_t | [512, 8192] | 2048 | 容量规划固定 |
| vehicle_report_hz | uint32_t | [1, 10] | 1 | 协议策略固定 |
| passenger_push_hz_normal | uint32_t | [1, 5] | 1 | 协议策略固定 |
| passenger_push_hz_pickup | uint32_t | [1, 10] | 2 | 协议策略固定 |
| replay_window_sec | uint32_t | [10, 300] | 60 | 协议策略固定 |
| dedup_window_sec | uint32_t | [60, 900] | 300 | 协议策略固定 |
| thread_count | uint32_t | [1, 4 * CPU 核数] | CPU 核数 | Start 时固定 |
| queue_capacity | uint32_t | [256, 1,048,576] 且为 2 的幂 | 65536 | 初始化固定 |
| submit_timeout_ms | uint32_t | [0, 60000] | 1000 | 调用时传入 |
| dequeue_timeout_ms | uint32_t | [0, 1000] | 0 | 调用时传入 |
| spin_threshold | uint32_t | [64, 65536] | 1024 | 初始化固定 |

### 6.2 指标契约

- 必须提供网络接入、队列与线程池三类指标快照接口，支持周期采样。
- 网络指标最小集合：活跃连接数、收包字节速率、完成帧速率、帧错误率、背压丢弃计数。
- 内部链路指标最小集合：任务吞吐、P50/P99 延迟、队列深度、CAS 重试次数、工作线程阻塞次数、失败率。
- 指标时间窗口默认 10 秒，支持外部采集系统轮询。

### 6.3 日志契约

- ERROR：监听启动失败、后端初始化失败、线程退出超时、任务执行异常、连续 CAS 失败超过阈值。
- WARN：队列背压触发、任务提交超时率超过 1%、帧错误率超过 0.1%。
- INFO：网络层启动、线程池启动/停止、配置摘要、背压状态切换。

## 7. 测试与验收标准

### 7.1 单元测试

- 固定长度前缀帧切分正确性（半包、粘包、多帧拼接）。
- 非法长度字段处理正确性（0 长度、超限长度、字节序错误）。
- 队列满/空边界正确性。
- 单生产者单消费者顺序正确性。
- 多生产者多消费者下任务不重复、不丢失。
- 入队与出队超时语义正确。
- 背压迟滞联动正确：高水位触发、低水位恢复、迟滞区间保持背压。

### 7.2 并发与竞态测试

- 使用 ThreadSanitizer 运行全量单元测试，结果为 0 data race。
- 32 线程并发压测 10 分钟，无死锁、无崩溃、无任务重复执行。
- epoll 后端一致性测试：同一测试集多轮执行结果一致。

### 7.3 性能验收口径（网络接入 + 内部链路）

- 测试环境：4 vCPU / 15GiB 内存 / Linux 6.x（Codespace 共享资源）。
- 工作负载：客户端发送固定长度前缀帧，payload 256B，executor 为常量时间操作。
- 接入吞吐目标：>= 200k frames/s。
- 内部吞吐目标：>= 200k tasks/s。
- 延迟目标（从 Submit 进入到 executor 执行开始）：P99 <= 5ms，P999 <= 20ms。
- 资源目标：CPU 平均使用率 <= 75%，内存水位 <= 70%。
- 稳定性目标：300k 活跃连接下持续 30 分钟无崩溃、无死锁、无持续性背压失控。
- 关键事件补传成功率：>= 99.99%（60 秒补传窗口）。
- 去重正确性：重复 message_id 不重复入队，误判率 <= 0.01%。
- 乘客端推送约束：常态 1Hz，接驾阶段 2Hz，推送延迟满足 7.3 时延目标。

### 7.3.1 端到端压测程序与输出契约（强制）

- 当前仓库的基线压测必须覆盖完整端到端链路：客户端 TCP 发包 -> NetworkIngress(epoll) 收包 -> FrameDecoder 切帧 -> RingQueue 入队/出队 -> ThreadPool 调度 -> CapacityModeExecutor 执行完成。
- 端到端压测结论在任何运行环境都必须可获得，包括本地 Linux、CI、Codespace；禁止仅以内部链路基准替代端到端结论。
- 基线压测程序应包含 server 与 client 两侧：
  - server 侧负责启动 ingress 与事件循环，并将帧投递到执行层。
  - client 侧负责并发建连、发送长度前缀帧、控制发送速率与总请求量。
- 在 Codespace 等共享环境中，端到端程序默认应自动选择本地空闲端口，避免使用固定热点端口导致的外部探测流量污染结果；若显式指定端口，则由调用方承担端口占用与外部噪声风险。
- 若保留内部链路微基准，仅允许作为诊断辅助，不得作为 7.3 验收主结论来源。
- 命令示例应使用端到端程序入口（包含 server/client 参数），并输出统一指标。

输出指标字段（统一口径）：

- e2e 吞吐：throughput_fps（frames/s）与 throughput_tps（tasks/s）。
- e2e 延迟：latency_us.p50、latency_us.p99、latency_us.p999。
- 口径定义：默认延迟口径为“客户端发送时刻 -> executor 执行完成时刻”；若采用其他口径，必须在结果中显式标注且不可混用。
- 稳定性指标：连接成功率、发送失败率、frame_decode_error、dropped_on_backpressure。
- 内部快照：queue_metrics、thread_pool_metrics、capacity_executor_metrics、ingress_metrics。

结果解释规则：

- 端到端吞吐与端到端尾延迟是主判据。
- 内部指标仅用于定位瓶颈来源，不可替代端到端主判据。
- 任何结论必须附带完整参数、环境信息与原始采样文件。

### 7.3.2 基线采样流程契约（用于调优对比）

- 采样流程固定为：`预热 1 轮（不计入统计） + 正式采样 N 轮（默认 N=5）`。
- 正式采样各轮必须使用相同参数，且在同一运行环境内完成（同一容器、同一 CPU 配额、同一构建产物）。
- 每轮输出必须保留原始记录，至少包含：`throughput_tps`、`p99_latency_us`、`p999_latency_us`、`cas_retry_rate`、`worker_empty_poll_ratio`。
- 汇总必须同时给出均值、标准差、最小值、最大值，避免仅报告单次最优值。
- 单变量调优规则：每次仅改变 1 个变量（线程数/队列容量/payload 大小/网络参数之一），其余参数保持不变。
- 推荐执行入口：`benchmarks/run_capacity_sampling.sh`，脚本负责预热、重复执行、生成逐轮明细与汇总文件。

结果判读补充规则：

- 若吞吐提升但 `p999_latency_us` 明显恶化（建议阈值 20%），判定为不可接受优化。
- 若 `cas_retry_rate` 与 `worker_empty_poll_ratio` 同时上升，优先排查线程数与队列容量匹配关系，而非立即替换底层算法。
- 任何参数组合的结论至少基于 5 轮样本，不得用单轮结果直接下优化结论。

### 7.4 当前阶段交付标准（2A-epoll）

- epoll 后端必须通过 7.1、7.2、7.3 的全部门槛。
- io_uring 后端在当前阶段标记为“暂缓实现”，不作为阶段通过前置条件。
- 当前阶段若 epoll 后端未达标，则整体阶段验收不通过。

### 7.5 通过/失败判定

- 任一核心指标未达标即判定失败。
- 出现死锁、崩溃、数据竞态、任务丢失或重复执行即判定失败。
- 验收报告必须包含测试命令、参数、原始结果与环境信息。
- 背压联动验收判定（当前阶段最小口径）：
  - 队列占用率达到 `>=85%` 时，`PollOnce()` 不能持续返回 `Ok`。
  - 队列占用率回落但仍 `>60%` 时，`PollOnce()` 仍应保持 `Backpressure`。
  - 队列占用率回落到 `<=60%` 时，`PollOnce()` 应恢复为 `Ok`。

## 8. 后续扩展边界

- 未来可在保持现有 Ingress 契约前提下扩展 TLS 与业务协议适配层。
- 扩展模块不得破坏本 SPEC 的网络接入、帧切分、队列与线程池接口契约。

## 9. 实现骨架清单（编码输入）

### 9.1 目录与模块划分

- src/ingress：网络接入抽象层与后端实现。
- src/ingress/epoll：epoll 后端实现（当前阶段主路径）。
- src/ingress/iouring：io_uring 后端实现（后续阶段）。
- src/frame：固定长度前缀帧切分器。
- src/queue：CAS 环形队列实现。
- src/thread_pool：线程池与调度循环实现。
- src/observability：指标聚合与日志封装。
- include/robotaxi：对外接口头文件。
- tests/unit：单元测试。
- tests/integration：epoll 链路与端到端集成测试。
- tests/perf：吞吐、延迟、连接规模压测脚本。

### 9.2 对外头文件骨架

- include/robotaxi/status.h：ErrorCode、Status。
- include/robotaxi/types.h：Task、配置结构体、指标结构体。
- include/robotaxi/frame_decoder.h：FrameDecoder 接口。
- include/robotaxi/network_ingress.h：NetworkIngress 接口。
- include/robotaxi/ring_queue.h：RingQueue 接口。
- include/robotaxi/thread_pool.h：ThreadPool 接口。
- include/robotaxi/factory.h：按后端类型构造 ingress、queue、thread_pool 的工厂接口。

### 9.3 最小可运行链路（MVP）

- 阶段 1：实现固定长度前缀帧切分器与单元测试。
- 阶段 2：实现 CAS 队列基础路径（Enqueue/Dequeue/超时）与单元测试。
- 阶段 3：实现线程池工作循环（Submit/Stop/空闲策略）与单元测试。
- 阶段 4：接入 epoll 后端，实现端到端收包-切帧-入队-执行闭环。
- 阶段 5：补齐可观测与背压，完成 2A-epoll 验收测试。
- 阶段 6（后续）：接入 io_uring 后端并开展一致性回归。

### 9.4 工程约束

- 所有公共接口遵循第 4 章契约，不得擅自新增未定义行为。
- 任何异常路径必须返回 Status，不使用静默失败。
- 队列、线程池、接入层关键路径必须具备指标埋点。
- 新增代码必须配套最小可复现实验或测试用例。

## 10. 测试分解清单（执行顺序）

### 10.1 单元测试（第一优先级）

- frame_decoder：半包、粘包、多帧拼接、非法长度、超限长度。
- ring_queue：满/空边界、顺序正确性、超时语义、CAS 冲突重试。
- thread_pool：Start/Stop 生命周期、提交语义、空闲切换、异常任务处理。
- dedup_window：5 分钟窗口去重命中与过期淘汰。

### 10.2 集成测试（第二优先级）

- epoll 链路：收包到执行全链路正确性。
- epoll 稳定性回归：同一输入集多轮执行行为一致。
- 背压联动：高低水位触发/恢复行为正确，指标与日志一致。
- 连接生命周期：接入、断开、异常关闭路径与资源回收正确。

### 10.3 性能与稳定性测试（第三优先级）

- 300k 活跃连接稳定性测试（30 分钟）。
- 200k frames/s 接入吞吐测试。
- 200k tasks/s 内部吞吐测试。
- 关键事件补传成功率测试（目标 >= 99.99%）。
- 去重误判率测试（目标 <= 0.01%）。

### 10.4 验收判定

- 任一单元测试或集成测试失败即阻断合并。
- 任一性能门槛未达标即验收失败。
- 当前阶段以 epoll 为唯一硬验收后端；io_uring 不作为 2A-epoll 通过前置条件。

- 每轮采样前必须完成 server 就绪检查与 client 连通性检查；任一失败则该轮判定无效并重试。
- 采样结果必须同时保留端到端指标与 ingress/queue/thread_pool 内部快照，确保可做瓶颈归因。
- 若未提供端到端链路结果，仅提供内部链路结果，则验收判定为失败。
