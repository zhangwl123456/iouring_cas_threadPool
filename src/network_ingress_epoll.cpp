#include "robotaxi/network_ingress.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "robotaxi/capacity_executor.h"

namespace robotaxi {
namespace {

constexpr std::uint32_t kReadChunkSize = 4096;
constexpr std::uint32_t kQueueHighWatermarkPercent = 85;
constexpr std::uint32_t kQueueLowWatermarkPercent = 60;
constexpr std::size_t kReadCompactThresholdBytes = 16U * 1024U;

std::uint32_t MakeConnEvents(const bool enable_read) {
  return EPOLLRDHUP | EPOLLET | (enable_read ? EPOLLIN : 0U);
}

class EpollNetworkIngress;

struct PayloadHandle {
  EpollNetworkIngress* owner = nullptr;
  std::uint64_t task_id = 0;
};

void CloseFdIfValid(int* fd) {
  if (fd != nullptr && *fd >= 0) {
    (void)::close(*fd);
    *fd = -1;
  }
}

// EpollNetworkIngress：epoll 后端的网络接入实现（当前主交付路径）。
//
// 设计思想：
// 1) 在“长连接低频建连 + 高频消息处理”场景下，优先保证稳态吞吐与可预测延迟；
// 2) 明确职责边界：本模块只负责接入、切帧、投递，不承担业务语义解析；
// 3) 通过迟滞背压（高/低水位）避免队列长期满载导致的系统失稳与级联抖动。
//
// 并发语义：
// - 控制面（Start/Stop/PollOnce）按单线程驱动模型设计；
// - 执行面（ReleasePayload）由 worker 线程回调，需与接入线程并发访问 payload 池；
// - payload_buffers_/payload_handles_ 统一受 payload_mu_ 保护，避免悬垂指针与双重释放。
class EpollNetworkIngress final : public NetworkIngress {
 public:
  EpollNetworkIngress(std::shared_ptr<RingQueue> queue, std::unique_ptr<FrameDecoder> decoder,
                      TaskExecutor custom_executor = nullptr)
      : queue_(std::move(queue)),
        decoder_(std::move(decoder)),
        custom_executor_(custom_executor),
        running_(false),
        backpressure_active_(false),
        epoll_fd_(-1),
        accepted_connections_(0),
        active_connections_(0),
        recv_bytes_(0),
        sent_bytes_(0),
        completed_frames_(0),
        frame_decode_error_(0),
        dropped_on_backpressure_(0),
        next_task_id_(1) {}

  ~EpollNetworkIngress() override {
    (void)Stop(0);
  }

  static void IngressCapacityExecutor(const Task& task) {
    CapacityModeExecutor(task);

    auto* handle = static_cast<PayloadHandle*>(task.response_handle);
    if (handle != nullptr && handle->owner != nullptr) {
      handle->owner->ReleasePayload(handle->task_id);
    }
  }

  Status Start(const IngressConfig& ingress_config, const FrameConfig&) override {
    // 运行态检查使用 acquire：确保读到最新 running_，避免重复启动破坏 fd 所有权。
    if (running_.load(std::memory_order_acquire)) {
      return {ErrorCode::kAlreadyRunning, "ingress already running"};
    }
    if (queue_ == nullptr || decoder_ == nullptr) {
      return {ErrorCode::kInvalidArgument, "queue/decoder is null"};
    }
    if (ingress_config.backend_type != IngressBackendType::kEpoll) {
      return {ErrorCode::kInvalidArgument, "backend_type must be epoll"};
    }
    if (ingress_config.bind_ip == nullptr || std::strlen(ingress_config.bind_ip) == 0U) {
      return {ErrorCode::kInvalidArgument, "bind_ip is invalid"};
    }
    if (ingress_config.listen_backlog == 0U || ingress_config.max_connections == 0U ||
        ingress_config.max_events_per_poll == 0U) {
      return {ErrorCode::kInvalidArgument, "ingress config has zero fields"};
    }
    if (ingress_config.listener_count == 0U) {
      return {ErrorCode::kInvalidArgument, "listener_count must be > 0"};
    }
    if (ingress_config.listener_count > 1U && !ingress_config.enable_reuseport) {
      // 多 listen socket 若不启用 reuseport，会导致绑定/分流语义不明确，直接拒绝。
      return {ErrorCode::kInvalidArgument,
              "enable_reuseport must be true when listener_count > 1"};
    }

    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
      return {ErrorCode::kIoError, "epoll_create1 failed"};
    }

    for (std::uint32_t i = 0; i < ingress_config.listener_count; ++i) {
      const int listen_fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
      if (listen_fd < 0) {
        CleanupAllFds();
        return {ErrorCode::kIoError, "socket failed"};
      }

      int reuse = 1;
      if (::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        (void)::close(listen_fd);
        CleanupAllFds();
        return {ErrorCode::kIoError, "setsockopt(SO_REUSEADDR) failed"};
      }
      if (ingress_config.enable_reuseport) {
        // 仅在显式配置下启用 reuseport：默认路径保持单 listen，降低行为复杂度。
        if (::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse)) < 0) {
          (void)::close(listen_fd);
          CleanupAllFds();
          return {ErrorCode::kIoError, "setsockopt(SO_REUSEPORT) failed"};
        }
      }

      sockaddr_in addr{};
      addr.sin_family = AF_INET;
      addr.sin_port = htons(ingress_config.bind_port);
      if (::inet_pton(AF_INET, ingress_config.bind_ip, &addr.sin_addr) != 1) {
        (void)::close(listen_fd);
        CleanupAllFds();
        return {ErrorCode::kInvalidArgument, "bind_ip is not valid ipv4"};
      }

      if (::bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        (void)::close(listen_fd);
        CleanupAllFds();
        return {ErrorCode::kIoError, "bind failed"};
      }

      if (::listen(listen_fd, static_cast<int>(ingress_config.listen_backlog)) < 0) {
        (void)::close(listen_fd);
        CleanupAllFds();
        return {ErrorCode::kIoError, "listen failed"};
      }

      epoll_event ev{};
      ev.events = EPOLLIN | EPOLLRDHUP;
      ev.data.fd = listen_fd;
      if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd, &ev) < 0) {
        (void)::close(listen_fd);
        CleanupAllFds();
        return {ErrorCode::kIoError, "epoll_ctl add listen fd failed"};
      }

      listen_fds_.push_back(listen_fd);
      listen_fds_set_.insert(listen_fd);
    }

    ingress_config_ = ingress_config;
    // release 保证：监听 fd/epoll 注册完成后，再对外可见 running=true。
    running_.store(true, std::memory_order_release);
    return Status::Success();
  }

  Status Stop(std::uint32_t) override {
    if (!running_.load(std::memory_order_acquire)) {
      return {ErrorCode::kNotRunning, "ingress not running"};
    }

    // 先发布停止信号，再执行资源回收；避免并发 PollOnce 在回收期间继续处理事件。
    running_.store(false, std::memory_order_release);
    for (auto& [fd, _] : conn_buffers_) {
      (void)::close(fd);
    }
    conn_buffers_.clear();
    CleanupAllFds();

    // 清理所有未被消费的帧缓冲。该区域与 ReleasePayload 并发，需互斥保护。
    {
      std::lock_guard<std::mutex> lock(payload_mu_);
      payload_buffers_.clear();
      payload_handles_.clear();
    }

    active_connections_.store(0, std::memory_order_relaxed);
    backpressure_active_.store(false, std::memory_order_release);
    return Status::Success();
  }

  Status PollOnce(const std::uint32_t timeout_ms) override {
    if (!running_.load(std::memory_order_acquire)) {
      return {ErrorCode::kNotRunning, "ingress not running"};
    }

    // 先刷新背压态再 wait：确保本轮事件处理使用最新读开关，减少过量读入。
    const bool in_backpressure = RefreshBackpressureState();

    std::vector<epoll_event> events(ingress_config_.max_events_per_poll);
    const int wait_timeout = (timeout_ms == 0U) ? 0 : static_cast<int>(timeout_ms);
    const int n = ::epoll_wait(epoll_fd_, events.data(), static_cast<int>(events.size()), wait_timeout);
    if (n < 0) {
      if (errno == EINTR) {
        return Status::Success();
      }
      return {ErrorCode::kIoError, "epoll_wait failed"};
    }

    for (int i = 0; i < n; ++i) {
      const int fd = events[static_cast<std::size_t>(i)].data.fd;
      const std::uint32_t ev = events[static_cast<std::size_t>(i)].events;

      if (listen_fds_set_.find(fd) != listen_fds_set_.end()) {
        HandleAccept(fd);
        continue;
      }

      if ((ev & EPOLLIN) != 0U) {
        // 先处理读事件，确保 RDHUP 场景下已到达缓冲的数据不被跳过。
        const Status s = HandleRead(fd);
        if (!s.Ok()) {
          return s;
        }
      }

      if ((ev & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) != 0U) {
        // 读路径已尽量消费可读数据；此处分支仅负责收尾关闭，避免重复 decode。
        CloseConnection(fd);
        continue;
      }
    }

    if (in_backpressure) {
      return {ErrorCode::kBackpressure, "ingress is in backpressure state"};
    }
    return Status::Success();
  }

  [[nodiscard]] IngressBackendType BackendType() const override {
    return IngressBackendType::kEpoll;
  }

  [[nodiscard]] IngressMetrics Metrics() const override {
    return IngressMetrics{
        .accepted_connections = accepted_connections_.load(std::memory_order_relaxed),
        .active_connections = active_connections_.load(std::memory_order_relaxed),
        .recv_bytes = recv_bytes_.load(std::memory_order_relaxed),
        .sent_bytes = sent_bytes_.load(std::memory_order_relaxed),
        .completed_frames = completed_frames_.load(std::memory_order_relaxed),
        .frame_decode_error = frame_decode_error_.load(std::memory_order_relaxed),
        .dropped_on_backpressure = dropped_on_backpressure_.load(std::memory_order_relaxed),
    };
  }

 private:
  bool RefreshBackpressureState() {
    const QueueMetrics qm = queue_->Metrics();
    if (qm.capacity == 0U) {
      return false;
    }

    const std::uint32_t usage_percent = static_cast<std::uint32_t>((qm.size * 100U) / qm.capacity);
    const bool current = backpressure_active_.load(std::memory_order_acquire);

    if (!current && usage_percent >= kQueueHighWatermarkPercent) {
      // 高水位触发：立即停读，把压力限制在 ingress 前段，防止队列持续冲高。
      backpressure_active_.store(true, std::memory_order_release);
      UpdateReadEvents(false);
      return true;
    }
    if (current && usage_percent <= kQueueLowWatermarkPercent) {
      // 低水位恢复：使用迟滞区间避免高低阈值附近的频繁抖动。
      backpressure_active_.store(false, std::memory_order_release);
      UpdateReadEvents(true);
      return false;
    }
    return current;
  }

  void UpdateReadEvents(const bool enable_read) {
    if (epoll_fd_ < 0) {
      return;
    }

    epoll_event listen_ev{};
    listen_ev.events = EPOLLRDHUP | (enable_read ? EPOLLIN : 0U);
    for (const int listen_fd : listen_fds_) {
      listen_ev.data.fd = listen_fd;
      (void)::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, listen_fd, &listen_ev);
    }

    for (const auto& [fd, _] : conn_buffers_) {
      // 统一批量切换 conn_fd 的读事件，确保背压语义对所有活跃连接一致生效。
      epoll_event conn_ev{};
      conn_ev.events = MakeConnEvents(enable_read);
      conn_ev.data.fd = fd;
      (void)::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &conn_ev);
    }
  }

  void HandleAccept(const int listen_fd) {
    for (;;) {
      sockaddr_in peer{};
      socklen_t peer_len = sizeof(peer);
        const int conn_fd =
          ::accept4(listen_fd, reinterpret_cast<sockaddr*>(&peer), &peer_len,
                    SOCK_NONBLOCK | SOCK_CLOEXEC);
      if (conn_fd < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          // LT/ET 下 accept drain 结束条件：表示本轮已接尽可接连接。
          return;
        }
        return;
      }

      if (conn_buffers_.size() >= ingress_config_.max_connections) {
        // 连接数达到上限时主动拒绝，优先保护既有连接与处理链路稳定。
        (void)::close(conn_fd);
        continue;
      }

      epoll_event ev{};
      ev.events = MakeConnEvents(true);
      ev.data.fd = conn_fd;
      if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, conn_fd, &ev) < 0) {
        (void)::close(conn_fd);
        continue;
      }

      conn_buffers_.emplace(conn_fd, ConnBuffer{});
      accepted_connections_.fetch_add(1, std::memory_order_relaxed);
      active_connections_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  struct ConnBuffer {
    std::vector<std::uint8_t> bytes;
    std::size_t consumed = 0;
  };

  Status HandleRead(const int conn_fd) {
    // connection buffer 必须存在，否则说明 epoll 事件与 conn_buffers_ 状态不一致。
    auto it = conn_buffers_.find(conn_fd);
    // 如果出现 conn_buffers_ 中缺失的 fd，说明 epoll 事件与 conn_buffers_ 状态不一致，
    // 可能是由于连接已被关闭或未正确注册。此时返回 IO 错误状态，提示连接状态缺失。
    if (it == conn_buffers_.end()) {
      return {ErrorCode::kIoError, "connection state missing"};
    }
    // 处理读事件时，先尽量 drain 可读数据，再执行切帧入队，确保已到达数据不被丢弃。
    ConnBuffer& conn_buf = it->second;
    // kReadChunkSize 的大小是一个经验值，既能保证每次 recv 有足够的缓冲空间，又不会占用过多内存。
    std::uint8_t tmp[kReadChunkSize];
    bool peer_closed = false;
    // ET 模式下，必须循环 drain 直到 EAGAIN/EWOULDBLOCK 才能保证本轮读事件处理完毕。
    for (;;) {
      const ssize_t n = ::recv(conn_fd, tmp, sizeof(tmp), 0);
      if (n == 0) {
        // 对端半关闭：先标记，后续仍要消费本端已接收缓冲，避免丢帧。
        peer_closed = true;
        break;
      }
      if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          // ET 下 drain 结束条件；非错误，表示当前已无可读数据。
          break;
        }
        return {ErrorCode::kIoError, "recv failed"};
      }
      // 累计接收字节数，供指标统计使用。
      recv_bytes_.fetch_add(static_cast<std::uint64_t>(n), std::memory_order_relaxed);
      // 将新接收数据追加到 connection buffer 尾部，供后续切帧使用。
      conn_buf.bytes.insert(conn_buf.bytes.end(), tmp, tmp + n);
    }

    for (;;) {
      FrameDecodeResult result{};
      // 如果 consumed 超过 bytes.size()，说明切帧逻辑出现异常，可能导致访问越界。
      // consumed在EnqueueFrames中，处理完每个完整帧后，会将 consumed 增加已消费的字节数。
      if (conn_buf.consumed > conn_buf.bytes.size()) {
        return {ErrorCode::kIoError, "connection buffer offset invalid"};
      }
      // conn_buf.consumed 表示已消费的字节数，conn_buf.bytes.size() 表示当前缓冲区的总字节数。
      const std::size_t available = conn_buf.bytes.size() - conn_buf.consumed;
      const std::uint8_t* decode_ptr =
          (available == 0U) ? nullptr : (conn_buf.bytes.data() + conn_buf.consumed);
      const Status decode_status = decoder_->Decode(decode_ptr, available, &result);
      if (!decode_status.Ok()) {
        if (decode_status.code == ErrorCode::kFrameError) {
          // 帧级协议错误按连接维度隔离：关闭当前连接，避免污染全局处理链路。
          frame_decode_error_.fetch_add(1, std::memory_order_relaxed);
          CloseConnection(conn_fd);
          return Status::Success();
        }
        return decode_status;
      }

      if (!EnqueueFrames(result)) {
        // 入队失败立即回退为背压：优先保护内存与队列，不继续吞入新数据。
        dropped_on_backpressure_.fetch_add(1, std::memory_order_relaxed);
        return {ErrorCode::kBackpressure, "queue full during frame enqueue"};
      }

      if (result.bytes_consumed == 0U) {
        break;
      }

      conn_buf.consumed += result.bytes_consumed;
      // 使用 consumed 偏移替代逐帧 erase，降低热路径线性搬移成本。
      CompactConnBuffer(&conn_buf);
      if (conn_buf.bytes.empty()) {
        break;
      }
    }

    if (peer_closed) {
      // 仅在 decode 循环结束后关闭连接，确保已到达数据有机会被切帧入队。
      CloseConnection(conn_fd);
    }

    return Status::Success();
  }

  bool EnqueueFrames(const FrameDecodeResult& result) {
    for (const FrameView& frame : result.completed_frames) {
      // 为每个帧分配持久缓冲，确保 payload 生命周期覆盖任务执行周期。
      // 缓冲由 ingress 层管理；response_handle 存储缓冲指针便于查询与清理。
      const std::uint64_t task_id = next_task_id_.fetch_add(1, std::memory_order_relaxed);
      auto payload_buffer = std::make_unique<std::vector<std::uint8_t>>(
          frame.payload, frame.payload + frame.payload_size);

      auto payload_handle = std::make_unique<PayloadHandle>();
      payload_handle->owner = this;
      payload_handle->task_id = task_id;

      const void* payload_ptr = payload_buffer->data();
      void* buffer_handle = payload_handle.get();

      // 先入缓冲池，再入队列（确保缓冲有效）。
      {
        // 与 ReleasePayload 并发，必须在同一把锁下维护双 map 一致性。
        std::lock_guard<std::mutex> lock(payload_mu_);
        payload_buffers_[task_id] = std::move(payload_buffer);
        payload_handles_[task_id] = std::move(payload_handle);
      }

      Task task{
          .task_id = task_id,
          .payload = payload_ptr,
          .payload_size = frame.payload_size,
          .response_handle = buffer_handle,
          .executor = custom_executor_ != nullptr ? custom_executor_ : &EpollNetworkIngress::IngressCapacityExecutor,
      };

      const Status s = queue_->Enqueue(task, 0);
      if (!s.Ok()) {
        // 入队失败时，从缓冲池移除该任务的缓冲（自动析构）。
        // 必须回滚，避免“未入队却持有缓冲”的孤儿对象。
        std::lock_guard<std::mutex> lock(payload_mu_);
        payload_buffers_.erase(task_id);
        payload_handles_.erase(task_id);
        return false;
      }
      completed_frames_.fetch_add(1, std::memory_order_relaxed);
    }
    return true;
  }

  void CloseConnection(const int conn_fd) {
    if (conn_buffers_.find(conn_fd) == conn_buffers_.end()) {
      return;
    }
    // 先从 epoll 删除再 close，防止 fd 重用后出现幽灵事件。
    (void)::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, conn_fd, nullptr);
    (void)::close(conn_fd);
    conn_buffers_.erase(conn_fd);
    active_connections_.fetch_sub(1, std::memory_order_relaxed);
  }

  void CleanupAllFds() {
    for (const int listen_fd : listen_fds_) {
      if (listen_fd >= 0) {
        (void)::close(listen_fd);
      }
    }
    listen_fds_.clear();
    listen_fds_set_.clear();
    CloseFdIfValid(&epoll_fd_);
  }

  void CompactConnBuffer(ConnBuffer* conn_buf) {
    if (conn_buf == nullptr || conn_buf->consumed == 0U) {
      return;
    }

    if (conn_buf->consumed >= conn_buf->bytes.size()) {
      conn_buf->bytes.clear();
      conn_buf->consumed = 0U;
      return;
    }

    const std::size_t remaining = conn_buf->bytes.size() - conn_buf->consumed;
    const bool should_compact =
        conn_buf->consumed >= kReadCompactThresholdBytes &&
        conn_buf->consumed >= remaining;
    if (!should_compact) {
      // 压缩是线性搬移，仅在“已消费足够多且收益明显”时触发，控制 CPU 抖动。
      return;
    }

    std::memmove(conn_buf->bytes.data(), conn_buf->bytes.data() + conn_buf->consumed, remaining);
    conn_buf->bytes.resize(remaining);
    conn_buf->consumed = 0U;
  }

  void ReleasePayload(const std::uint64_t task_id) {
    // worker 线程回收入口：通过 task_id 精确删除，避免跨线程释放悬垂引用。
    std::lock_guard<std::mutex> lock(payload_mu_);
    payload_buffers_.erase(task_id);
    payload_handles_.erase(task_id);
  }

  std::shared_ptr<RingQueue> queue_;
  std::unique_ptr<FrameDecoder> decoder_;
  TaskExecutor custom_executor_;  // 自定义 executor，若为 nullptr 则使用默认 IngressCapacityExecutor
  IngressConfig ingress_config_{};

  std::atomic<bool> running_;
  std::atomic<bool> backpressure_active_;

  int epoll_fd_;
  std::vector<int> listen_fds_;
  std::unordered_set<int> listen_fds_set_;
  std::unordered_map<int, ConnBuffer> conn_buffers_;
  
  // 帧缓冲池：task_id -> payload 缓冲。
  // 每个入队的 Task 都有对应的缓冲，确保 payload 指针有效期覆盖任务执行周期。
  // 当线程池或上层应用处理完任务后，可通过 response_handle 查询并释放缓冲。
  // 在 Stop() 时统一清理所有遗留缓冲。
  std::mutex payload_mu_;
  std::unordered_map<std::uint64_t, std::unique_ptr<std::vector<std::uint8_t>>> payload_buffers_;
  std::unordered_map<std::uint64_t, std::unique_ptr<PayloadHandle>> payload_handles_;

  std::atomic<std::uint64_t> accepted_connections_;
  std::atomic<std::uint64_t> active_connections_;
  std::atomic<std::uint64_t> recv_bytes_;
  std::atomic<std::uint64_t> sent_bytes_;
  std::atomic<std::uint64_t> completed_frames_;
  std::atomic<std::uint64_t> frame_decode_error_;
  std::atomic<std::uint64_t> dropped_on_backpressure_;
  std::atomic<std::uint64_t> next_task_id_;
};

}  // namespace

std::unique_ptr<NetworkIngress> MakeEpollNetworkIngress(std::shared_ptr<RingQueue> queue,
                                                        std::unique_ptr<FrameDecoder> decoder) {
  return std::make_unique<EpollNetworkIngress>(std::move(queue), std::move(decoder), nullptr);
}

std::unique_ptr<NetworkIngress> MakeEpollNetworkIngressWithExecutor(std::shared_ptr<RingQueue> queue,
                                                                     std::unique_ptr<FrameDecoder> decoder,
                                                                     TaskExecutor executor) {
  return std::make_unique<EpollNetworkIngress>(std::move(queue), std::move(decoder), executor);
}

}  // namespace robotaxi
