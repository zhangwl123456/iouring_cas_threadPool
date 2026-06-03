# 规格说明（SPEC）

本文件作为本模块唯一权威规格说明，涵盖架构、接口契约、线程安全与算法设计、非功能性指标与验收标准。

## 1. 范围与目标

### 1.1 目标

- 构建面向高并发请求的服务器端并发处理模块。
- 采用 `io_uring` 作为高性能 IO 接入手段，将请求转化为任务并投递至无锁队列。
- 使用基于 CAS 的环形缓冲区作为核心任务队列。
- 线程池通过无锁方式获取任务并执行，提升吞吐与降低延迟。

### 1.2 非目标

- 不实现具体业务逻辑，仅提供通用并发处理与调度框架。
- 不绑定具体协议实现（HTTP、gRPC、自定义二进制等），仅预留可插拔协议适配层。
- 不提供 C 兼容接口，仅提供 C++ 接口。

## 2. 术语定义

- 请求（Request）：客户端发送的单位业务调用，经协议层解析后形成任务。
- 任务（Task）：可被线程池执行的工作单元，包含请求上下文与回包句柄。
- 协议适配层（Protocol Adapter）：负责协议解析与编解码的可插拔抽象层。
- 队列（Ring Queue）：基于 CAS 管理头尾指针的无锁环形缓冲区。
- IO 接入层（IO Ingress）：使用 `io_uring` 接收并分发请求的模块。

## 3. 分层架构与模块职责

### 3.1 分层架构

1. IO 接入层（IO Ingress Layer）
	 - 负责 `io_uring` 初始化、事件轮询、请求接入与初步解析。
	 - 将请求转换为标准任务并投递至队列。
2. 协议适配层（Protocol Adapter Layer）
	 - 提供协议解析与编解码的抽象接口。
	 - 支持 TLS 终止与证书管理边界配置（见 7.3）。
3. 队列层（Ring Queue Layer）
	 - CAS 无锁环形缓冲区，负责任务入队与出队。
	 - 支持背压与超时策略。
4. 线程池与调度层（Thread Pool & Scheduler Layer）
	 - 线程池拉取任务并执行。
	 - 支持动态线程数调整与任务执行统计。
5. 可观测与运维层（Observability & Ops Layer）
	 - 提供指标采集、日志与运行时配置能力。

### 3.2 模块职责边界

- IO 接入层只负责 IO 与任务构造，不含业务处理。
- 协议适配层只做协议编解码与 TLS 处理边界，不做调度。
- 队列层只管理任务缓存与并发访问，不执行任务。
- 线程池只执行任务，不直接操作 `io_uring`。

## 4. API 契约（跨模块调用）

### 4.1 统一错误码

- `Ok`：成功
- `InvalidArgument`：参数非法
- `QueueFull`：队列满
- `QueueEmpty`：队列空
- `Timeout`：超时
- `IoError`：IO 错误
- `TlsError`：TLS 相关错误
- `Internal`：内部错误

### 4.2 基础类型

```cpp
enum class ErrorCode {
	Ok = 0,
	InvalidArgument = 1,
	QueueFull = 2,
	QueueEmpty = 3,
	Timeout = 4,
	IoError = 5,
	TlsError = 6,
	Internal = 7,
};

struct Status {
	ErrorCode code;
	const char* message;
};

struct Task {
	uint64_t taskId;
	void* payload;
	uint32_t payloadSize;
	void* responseHandle;
};
```

### 4.3 协议适配层接口

```cpp
class ProtocolAdapter {
 public:
	virtual ~ProtocolAdapter() = default;

	virtual Status DecodeRequest(const void* input, uint32_t size, Task* outTask) = 0;
	virtual Status EncodeResponse(const Task& task, const void* result, uint32_t size,
																void* outBuffer, uint32_t* outSize) = 0;

	virtual Status ConfigureTls(const void* tlsConfig) = 0;
};
```

- `DecodeRequest`：协议解析失败返回 `InvalidArgument` 或 `Internal`。
- `EncodeResponse`：回包编码失败返回 `Internal`。
- `ConfigureTls`：配置 TLS 失败返回 `TlsError`。

### 4.4 队列接口

```cpp
class RingQueue {
 public:
	virtual ~RingQueue() = default;

	virtual Status Enqueue(const Task& task) = 0;
	virtual Status Dequeue(Task* outTask) = 0;
	virtual uint32_t Capacity() const = 0;
	virtual uint32_t Size() const = 0;
};
```

- `Enqueue`：队列满返回 `QueueFull`。
- `Dequeue`：队列空返回 `QueueEmpty`。

### 4.5 IO 接入层接口

```cpp
class IoIngress {
 public:
	virtual ~IoIngress() = default;

	virtual Status Start() = 0;
	virtual Status Stop() = 0;
	virtual Status PollOnce(uint32_t maxEvents) = 0;
};
```

- `Start` 初始化 `io_uring` 资源。
- `PollOnce` 处理一次事件轮询，内部将请求入队。

### 4.6 线程池接口

```cpp
class ThreadPool {
 public:
	virtual ~ThreadPool() = default;

	virtual Status Start(uint32_t threadCount) = 0;
	virtual Status Stop() = 0;
	virtual Status Submit(const Task& task) = 0;
	virtual uint32_t ActiveThreads() const = 0;
};
```

- `Submit`：用于直接投递任务的扩展能力，可选实现。

## 5. 核心算法与线程安全设计

### 5.1 CAS 环形缓冲区

- 使用无锁环形缓冲区存储任务，头尾指针通过 CAS 原子更新。
- 入队步骤：
	1. 读取 `tail` 与 `head`，计算剩余容量。
	2. 使用 CAS 更新 `tail`，成功后写入任务。
	3. 若容量不足返回 `QueueFull`。
- 出队步骤：
	1. 读取 `head` 与 `tail`，判断是否为空。
	2. 使用 CAS 更新 `head`，成功后读取任务。
	3. 若为空返回 `QueueEmpty`。

### 5.2 线程池任务获取

- 每个工作线程以无锁方式从队列获取任务。
- 若连续空队列达到阈值，进入短暂自旋或阻塞等待策略（可配置）。

### 5.3 背压与超时策略

- 当队列接近满载时，IO 接入层应触发背压信号，降低接入速率。
- 任务入队支持超时参数，超时返回 `Timeout`。

## 6. 非功能性需求

### 6.1 性能目标（默认基线）

- 环境：16 核 CPU / 64GB 内存 / SSD / Linux 6.x。
- 吞吐：200k 请求/秒（短任务）。
- 延迟：P99 ≤ 5ms，P999 ≤ 20ms。
- 并发连接：100k 活跃连接。
- 资源：CPU 平均使用率 ≤ 75%，内存水位 ≤ 70%。

### 6.2 可靠性与容错

- 不允许丢请求为默认目标。
- 支持超时、重试、背压、限流与熔断策略配置。
- 关键错误必须可观测且可追踪。

### 6.3 可观测性与运维

- 指标：吞吐、延迟分位数、队列深度、线程池饱和度、失败率。
- 日志：关键错误、超时与 IO 异常。
- 运维：运行时可动态调整线程数、队列容量与限流阈值。

## 7. TLS 边界定义

### 7.1 TLS 终止位置

- TLS 终止在协议适配层，由协议适配层负责握手与加解密。

### 7.2 证书管理边界

- 证书加载与更新由外部运维系统提供，模块仅提供配置接口。
- 模块不负责证书自动轮换与密钥生成。

### 7.3 TLS 错误处理

- TLS 相关错误统一返回 `TlsError`，并记录可观测日志。

## 8. 验收标准

1. 在基准环境下达到 6.1 节性能目标。
2. 队列满载时仍能稳定响应背压与限流策略。
3. TLS 配置错误可被准确捕获并返回 `TlsError`。
4. 线程池在高并发下无死锁、无崩溃，任务处理正确完成。
5. 关键指标可通过监控接口获取，日志包含关键错误与超时信息。
