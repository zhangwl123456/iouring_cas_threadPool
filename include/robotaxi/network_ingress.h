#ifndef ROBOTAXI_NETWORK_INGRESS_H_
#define ROBOTAXI_NETWORK_INGRESS_H_

#include <cstdint>
#include <memory>

#include "robotaxi/frame_decoder.h"
#include "robotaxi/ring_queue.h"
#include "robotaxi/status.h"
#include "robotaxi/types.h"

namespace robotaxi {

// 网络接入层抽象接口。
// 设计目标：负责连接管理、字节流收发与帧切分投递，不解析业务语义。
class NetworkIngress {
 public:
  virtual ~NetworkIngress() = default;

  // 启动网络接入层。
  // 返回：
  // - kOk: 启动成功。
  // - kInvalidArgument: 配置非法。
  // - kAlreadyRunning: 重复启动。
  // - kIoError: 底层 socket/epoll 初始化失败。
  virtual Status Start(const IngressConfig& ingress_config,
                       const FrameConfig& frame_config) = 0;

  // 停止网络接入层并回收资源。
  // 返回：
  // - kOk: 停止成功。
  // - kNotRunning: 尚未启动。
  virtual Status Stop(std::uint32_t wait_timeout_ms) = 0;

  // 处理一次事件批次。
  // timeout_ms 语义：
  // - 0: 非阻塞探测。
  // - >0: 最多阻塞 timeout_ms 等待事件。
  // 返回：
  // - kOk: 正常处理完成（含超时无事件）。
  // - kBackpressure: 当前处于背压态，已暂停读事件。
  // - kNotRunning: 尚未启动。
  // - kIoError: epoll_wait/recv/accept 等系统调用错误。
  virtual Status PollOnce(std::uint32_t timeout_ms) = 0;

  // 返回当前后端类型。
  [[nodiscard]] virtual IngressBackendType BackendType() const = 0;

  // 返回指标快照。
  [[nodiscard]] virtual IngressMetrics Metrics() const = 0;
};

// 工厂函数：创建 epoll 后端 NetworkIngress。
std::unique_ptr<NetworkIngress> MakeEpollNetworkIngress(std::shared_ptr<RingQueue> queue,
                                                        std::unique_ptr<FrameDecoder> decoder);

}  // namespace robotaxi

#endif  // ROBOTAXI_NETWORK_INGRESS_H_
