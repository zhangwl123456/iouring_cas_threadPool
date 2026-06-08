#ifndef ROBOTAXI_STATUS_H_
#define ROBOTAXI_STATUS_H_

namespace robotaxi {

// 统一错误码定义。
// 约定：所有模块通过 Status 返回错误，不抛出业务异常。
enum class ErrorCode {
  // 成功。
  kOk = 0,
  // 参数非法，如空指针、越界值、配置不满足约束。
  kInvalidArgument = 1,
  // 网络或系统 IO 相关错误。
  kIoError = 2,
  // 帧切分/解码错误，如非法长度或协议头异常。
  kFrameError = 3,
  // 背压触发，调用方应降速或重试。
  kBackpressure = 4,
  // 队列已满。
  kQueueFull = 5,
  // 队列为空。
  kQueueEmpty = 6,
  // 在给定超时窗口内未完成操作。
  kTimeout = 7,
  // 组件未启动或运行态未就绪。
  kNotRunning = 8,
  // 组件重复启动或重复初始化。
  kAlreadyRunning = 9,
  // 未分类内部错误。
  kInternal = 10,
};

// 统一状态对象。
// - code: 机器可判定错误码。
// - message: 人类可读诊断信息，生命周期应至少覆盖本次调用返回后的读取期。
struct Status {
  ErrorCode code;
  const char* message;

  // 快速成功判定。
  // 线程安全：仅读取当前对象，无共享可变状态。
  [[nodiscard]] bool Ok() const noexcept { return code == ErrorCode::kOk; }

  // 构造标准成功状态。
  static Status Success() noexcept { return {ErrorCode::kOk, "ok"}; }
};

}  // namespace robotaxi

#endif  // ROBOTAXI_STATUS_H_
