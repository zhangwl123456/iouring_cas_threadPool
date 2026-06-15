#ifndef ROBOTAXI_FRAME_DECODER_H_
#define ROBOTAXI_FRAME_DECODER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "robotaxi/status.h"
#include "robotaxi/types.h"

namespace robotaxi {

// 单帧 payload 视图。
// 所有权：payload 为借用指针，不拥有底层内存；仅在输入 buffer 保持有效期间可读。
struct FrameView {
  const std::uint8_t* payload;
  std::uint32_t payload_size;
};

// 一次切分调用的结果。
// - bytes_consumed: 已成功消费的输入字节数，仅覆盖完整帧。
// - completed_frames: 本次识别出的完整 payload 视图列表。
struct FrameDecodeResult {
  std::size_t bytes_consumed;
  std::vector<FrameView> completed_frames;
};

// 固定长度前缀帧切分器接口。
// 设计目标：只负责消息边界切分，不解析业务字段，不创建 Task，不管理连接状态。
class FrameDecoder {
 public:
  virtual ~FrameDecoder() = default;

  // 对输入 buffer 执行一次无状态切分。
  // 参数：
  // - buffer: 输入字节流起始地址；当 buffer_size 为 0 时可为 nullptr。
  // - buffer_size: 输入 buffer 可读字节数。
  // - out_result: 输出参数；成功时写入已消费字节数与完整帧列表。
  // 返回：
  // - kOk: 切分成功；若输入不足以组成完整帧，completed_frames 可为空。
  // - kFrameError: 长度字段非法（0 或超限）。
  // - kInvalidArgument: 参数为空或配置非法。
  virtual Status Decode(const std::uint8_t* buffer,
                        std::size_t buffer_size,
                        FrameDecodeResult* out_result) const = 0;

  // 返回当前切分器配置快照。
  [[nodiscard]] virtual FrameConfig Config() const = 0;
};

// 工厂函数：创建固定长度前缀帧切分器。
// 异常：当配置不满足协议约束时抛出 std::invalid_argument。
std::unique_ptr<FrameDecoder> MakeFrameDecoder(const FrameConfig& config);

}  // namespace robotaxi

#endif  // ROBOTAXI_FRAME_DECODER_H_
