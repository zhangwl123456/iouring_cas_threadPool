#ifndef ROBOTAXI_STATUS_H_
#define ROBOTAXI_STATUS_H_

namespace robotaxi {

enum class ErrorCode {
  kOk = 0,
  kInvalidArgument = 1,
  kIoError = 2,
  kFrameError = 3,
  kBackpressure = 4,
  kQueueFull = 5,
  kQueueEmpty = 6,
  kTimeout = 7,
  kNotRunning = 8,
  kAlreadyRunning = 9,
  kInternal = 10,
};

struct Status {
  ErrorCode code;
  const char* message;

  [[nodiscard]] bool Ok() const noexcept { return code == ErrorCode::kOk; }

  static Status Success() noexcept { return {ErrorCode::kOk, "ok"}; }
};

}  // namespace robotaxi

#endif  // ROBOTAXI_STATUS_H_
