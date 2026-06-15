#include "robotaxi/frame_decoder.h"

#include <stdexcept>
#include <utility>

namespace robotaxi {
namespace {

// 固定长度前缀的协议里，前 4 个字节就是 body 长度。
constexpr std::uint32_t kRequiredLengthFieldBytes = 4;
// 这个实现先保留一个合理的最小值，避免把过小的配置传进来。
constexpr std::uint32_t kMinFrameLength = 1024;
// 最大帧长不能超过 16 MiB，和 SPEC 保持一致。
constexpr std::uint32_t kMaxFrameLength = 16U * 1024U * 1024U;

// 把 4 字节大端长度转成普通整数。
[[nodiscard]] std::uint32_t ReadLengthBigEndian(const std::uint8_t* data) noexcept {
  return (static_cast<std::uint32_t>(data[0]) << 24U) |
         (static_cast<std::uint32_t>(data[1]) << 16U) |
         (static_cast<std::uint32_t>(data[2]) << 8U) |
         static_cast<std::uint32_t>(data[3]);
}

class BasicFrameDecoder final : public FrameDecoder {
 public:
  // 保存配置并先检查是否合法。
  explicit BasicFrameDecoder(FrameConfig config) : config_(config) { ValidateConfig(); }

  Status Decode(const std::uint8_t* buffer,
                const std::size_t buffer_size,
                FrameDecodeResult* out_result) const override {
    // 输出参数不能为空，否则调用方拿不到切分结果。
    if (out_result == nullptr) {
      return {ErrorCode::kInvalidArgument, "out_result is null"};
    }

    // 先清空上一次的结果，避免旧数据混进来。
    out_result->bytes_consumed = 0;
    out_result->completed_frames.clear();

    // 有数据但指针为空，说明调用方式有问题。
    if (buffer == nullptr && buffer_size != 0U) {
      return {ErrorCode::kInvalidArgument, "buffer is null"};
    }

    // 从头开始扫：能切出完整帧就加入结果，剩下不完整的尾巴留给下次再补。
    std::size_t offset = 0;
    while (buffer_size - offset >= kRequiredLengthFieldBytes) {
      // 先读长度字段，再判断这一帧够不够完整。
      const std::uint32_t payload_size = ReadLengthBigEndian(buffer + offset);
      if (payload_size == 0U) {
        out_result->bytes_consumed = offset;
        return {ErrorCode::kFrameError, "frame length must be > 0"};
      }
      if (payload_size > config_.max_frame_length) {
        out_result->bytes_consumed = offset;
        return {ErrorCode::kFrameError, "frame length exceeds max_frame_length"};
      }

      const std::size_t frame_size =
          static_cast<std::size_t>(kRequiredLengthFieldBytes) + payload_size;
      if (buffer_size - offset < frame_size) {
        // 长度字段已经看到了，但 body 还没收全，这就是半包。
        break;
      }

      // 帧完整，直接把 body 的位置和长度返回给上层。
      out_result->completed_frames.push_back(FrameView{
          .payload = buffer + offset + kRequiredLengthFieldBytes,
          .payload_size = payload_size,
      });
      offset += frame_size;
    }

    out_result->bytes_consumed = offset;
    return Status::Success();
  }

  [[nodiscard]] FrameConfig Config() const override { return config_; }

 private:
  void ValidateConfig() const {
    // 这个切分器只支持固定 4 字节长度头。
    if (config_.length_field_bytes != kRequiredLengthFieldBytes) {
      throw std::invalid_argument("length_field_bytes must be 4");
    }
    // 当前协议约定长度字段就是大端。
    if (!config_.network_byte_order) {
      throw std::invalid_argument("network_byte_order must be true");
    }
    // 这里先做一个保护，避免传入过小或过大的帧长。
    if (config_.max_frame_length < kMinFrameLength ||
        config_.max_frame_length > kMaxFrameLength) {
      throw std::invalid_argument("max_frame_length out of supported range");
    }
  }

  FrameConfig config_;
};

}  // namespace

// 工厂函数：外部只需要调用这个函数，就能拿到一个可用的 FrameDecoder。
std::unique_ptr<FrameDecoder> MakeFrameDecoder(const FrameConfig& config) {
  return std::make_unique<BasicFrameDecoder>(config);
}

}  // namespace robotaxi
