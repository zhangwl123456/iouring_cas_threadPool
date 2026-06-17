#include "robotaxi/network_ingress.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "robotaxi/capacity_executor.h"

namespace robotaxi {
namespace {

constexpr std::uint32_t kReadChunkSize = 4096;
constexpr std::uint32_t kQueueHighWatermarkPercent = 85;
constexpr std::uint32_t kQueueLowWatermarkPercent = 60;

void CloseFdIfValid(int* fd) {
  if (fd != nullptr && *fd >= 0) {
    (void)::close(*fd);
    *fd = -1;
  }
}

// EpollNetworkIngress：epoll 后端的网络接入实现。
// 关键职责：
// 1) 管理监听与连接生命周期（accept/read/close）；
// 2) 将字节流切分为帧并封装为 Task 入队；
// 3) 通过高低水位背压控制读事件，避免队列被持续写满。
class EpollNetworkIngress final : public NetworkIngress {
 public:
  EpollNetworkIngress(std::shared_ptr<RingQueue> queue, std::unique_ptr<FrameDecoder> decoder)
      : queue_(std::move(queue)),
        decoder_(std::move(decoder)),
        running_(false),
        backpressure_active_(false),
        epoll_fd_(-1),
        listen_fd_(-1),
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

  Status Start(const IngressConfig& ingress_config, const FrameConfig&) override {
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

    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
      return {ErrorCode::kIoError, "epoll_create1 failed"};
    }

    listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_fd_ < 0) {
      CloseFdIfValid(&epoll_fd_);
      return {ErrorCode::kIoError, "socket failed"};
    }

    int reuse = 1;
    if (::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
      CleanupAllFds();
      return {ErrorCode::kIoError, "setsockopt(SO_REUSEADDR) failed"};
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(ingress_config.bind_port);
    if (::inet_pton(AF_INET, ingress_config.bind_ip, &addr.sin_addr) != 1) {
      CleanupAllFds();
      return {ErrorCode::kInvalidArgument, "bind_ip is not valid ipv4"};
    }

    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
      CleanupAllFds();
      return {ErrorCode::kIoError, "bind failed"};
    }

    if (::listen(listen_fd_, static_cast<int>(ingress_config.listen_backlog)) < 0) {
      CleanupAllFds();
      return {ErrorCode::kIoError, "listen failed"};
    }

    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLRDHUP;
    ev.data.fd = listen_fd_;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev) < 0) {
      CleanupAllFds();
      return {ErrorCode::kIoError, "epoll_ctl add listen fd failed"};
    }

    ingress_config_ = ingress_config;
    running_.store(true, std::memory_order_release);
    return Status::Success();
  }

  Status Stop(std::uint32_t) override {
    if (!running_.load(std::memory_order_acquire)) {
      return {ErrorCode::kNotRunning, "ingress not running"};
    }

    running_.store(false, std::memory_order_release);
    for (auto& [fd, _] : conn_buffers_) {
      (void)::close(fd);
    }
    conn_buffers_.clear();
    CleanupAllFds();

    // 清理所有未被消费的帧缓冲。若线程池已完成执行，缓冲的管理责任回归 ingress 层。
    payload_buffers_.clear();

    active_connections_.store(0, std::memory_order_relaxed);
    backpressure_active_.store(false, std::memory_order_release);
    return Status::Success();
  }

  Status PollOnce(const std::uint32_t timeout_ms) override {
    if (!running_.load(std::memory_order_acquire)) {
      return {ErrorCode::kNotRunning, "ingress not running"};
    }

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

      if (fd == listen_fd_) {
        HandleAccept();
        continue;
      }

      if ((ev & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) != 0U) {
        CloseConnection(fd);
        continue;
      }

      if ((ev & EPOLLIN) != 0U) {
        const Status s = HandleRead(fd);
        if (!s.Ok()) {
          return s;
        }
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
      backpressure_active_.store(true, std::memory_order_release);
      UpdateReadEvents(false);
      return true;
    }
    if (current && usage_percent <= kQueueLowWatermarkPercent) {
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
    listen_ev.data.fd = listen_fd_;
    if (listen_fd_ >= 0) {
      (void)::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, listen_fd_, &listen_ev);
    }

    for (const auto& [fd, _] : conn_buffers_) {
      epoll_event conn_ev{};
      conn_ev.events = EPOLLRDHUP | (enable_read ? EPOLLIN : 0U);
      conn_ev.data.fd = fd;
      (void)::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &conn_ev);
    }
  }

  void HandleAccept() {
    for (;;) {
      sockaddr_in peer{};
      socklen_t peer_len = sizeof(peer);
      const int conn_fd =
          ::accept4(listen_fd_, reinterpret_cast<sockaddr*>(&peer), &peer_len,
                    SOCK_NONBLOCK | SOCK_CLOEXEC);
      if (conn_fd < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          return;
        }
        return;
      }

      if (conn_buffers_.size() >= ingress_config_.max_connections) {
        (void)::close(conn_fd);
        continue;
      }

      epoll_event ev{};
      ev.events = EPOLLIN | EPOLLRDHUP;
      ev.data.fd = conn_fd;
      if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, conn_fd, &ev) < 0) {
        (void)::close(conn_fd);
        continue;
      }

      conn_buffers_.emplace(conn_fd, std::vector<std::uint8_t>{});
      accepted_connections_.fetch_add(1, std::memory_order_relaxed);
      active_connections_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  Status HandleRead(const int conn_fd) {
    auto it = conn_buffers_.find(conn_fd);
    if (it == conn_buffers_.end()) {
      return {ErrorCode::kIoError, "connection state missing"};
    }

    std::vector<std::uint8_t>& buf = it->second;
    std::uint8_t tmp[kReadChunkSize];

    for (;;) {
      const ssize_t n = ::recv(conn_fd, tmp, sizeof(tmp), 0);
      if (n == 0) {
        CloseConnection(conn_fd);
        return Status::Success();
      }
      if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          break;
        }
        return {ErrorCode::kIoError, "recv failed"};
      }

      recv_bytes_.fetch_add(static_cast<std::uint64_t>(n), std::memory_order_relaxed);
      buf.insert(buf.end(), tmp, tmp + n);
    }

    for (;;) {
      FrameDecodeResult result{};
      const Status decode_status = decoder_->Decode(buf.data(), buf.size(), &result);
      if (!decode_status.Ok()) {
        if (decode_status.code == ErrorCode::kFrameError) {
          frame_decode_error_.fetch_add(1, std::memory_order_relaxed);
          CloseConnection(conn_fd);
          return Status::Success();
        }
        return decode_status;
      }

      if (!EnqueueFrames(result)) {
        dropped_on_backpressure_.fetch_add(1, std::memory_order_relaxed);
        return {ErrorCode::kBackpressure, "queue full during frame enqueue"};
      }

      if (result.bytes_consumed == 0U) {
        break;
      }

      buf.erase(buf.begin(), buf.begin() + static_cast<std::ptrdiff_t>(result.bytes_consumed));
      if (buf.empty()) {
        break;
      }
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
      
      const void* payload_ptr = payload_buffer->data();
      void* buffer_handle = payload_buffer.get();
      
      // 先入缓冲池，再入队列（确保缓冲有效）。
      payload_buffers_[task_id] = std::move(payload_buffer);

      Task task{
          .task_id = task_id,
          .payload = payload_ptr,
          .payload_size = frame.payload_size,
          .response_handle = buffer_handle,
          .executor = &CapacityModeExecutor,
      };

      const Status s = queue_->Enqueue(task, 0);
      if (!s.Ok()) {
        // 入队失败时，从缓冲池移除该任务的缓冲（自动析构）。
        payload_buffers_.erase(task_id);
        return false;
      }
      completed_frames_.fetch_add(1, std::memory_order_relaxed);
    }
    return true;
  }

  void CloseConnection(const int conn_fd) {
    (void)::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, conn_fd, nullptr);
    (void)::close(conn_fd);
    conn_buffers_.erase(conn_fd);
    active_connections_.fetch_sub(1, std::memory_order_relaxed);
  }

  void CleanupAllFds() {
    CloseFdIfValid(&listen_fd_);
    CloseFdIfValid(&epoll_fd_);
  }

  std::shared_ptr<RingQueue> queue_;
  std::unique_ptr<FrameDecoder> decoder_;
  IngressConfig ingress_config_{};

  std::atomic<bool> running_;
  std::atomic<bool> backpressure_active_;

  int epoll_fd_;
  int listen_fd_;
  std::unordered_map<int, std::vector<std::uint8_t>> conn_buffers_;
  
  // 帧缓冲池：task_id -> payload 缓冲。
  // 每个入队的 Task 都有对应的缓冲，确保 payload 指针有效期覆盖任务执行周期。
  // 当线程池或上层应用处理完任务后，可通过 response_handle 查询并释放缓冲。
  // 在 Stop() 时统一清理所有遗留缓冲。
  std::unordered_map<std::uint64_t, std::unique_ptr<std::vector<std::uint8_t>>> payload_buffers_;

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
  return std::make_unique<EpollNetworkIngress>(std::move(queue), std::move(decoder));
}

}  // namespace robotaxi
