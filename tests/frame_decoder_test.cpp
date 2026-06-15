#include "robotaxi/frame_decoder.h"

#include <cstdint>
#include <memory>
#include <vector>

#include "gtest/gtest.h"

namespace robotaxi {
namespace {

FrameConfig DefaultFrameConfig() {
  return FrameConfig{
      .length_field_bytes = 4,
      .max_frame_length = 1024U * 1024U,
      .network_byte_order = true,
  };
}

std::vector<std::uint8_t> BuildFrame(const std::vector<std::uint8_t>& payload) {
  const std::uint32_t payload_size = static_cast<std::uint32_t>(payload.size());
  std::vector<std::uint8_t> frame;
  frame.reserve(4U + payload.size());
  frame.push_back(static_cast<std::uint8_t>((payload_size >> 24U) & 0xFFU));
  frame.push_back(static_cast<std::uint8_t>((payload_size >> 16U) & 0xFFU));
  frame.push_back(static_cast<std::uint8_t>((payload_size >> 8U) & 0xFFU));
  frame.push_back(static_cast<std::uint8_t>(payload_size & 0xFFU));
  frame.insert(frame.end(), payload.begin(), payload.end());
  return frame;
}

std::vector<std::uint8_t> ZeroLengthFrame() {
  return std::vector<std::uint8_t>{0x00, 0x00, 0x00, 0x00};
}

std::vector<std::uint8_t> OversizedFrameHeader(const std::uint32_t payload_size) {
  return std::vector<std::uint8_t>{
      static_cast<std::uint8_t>((payload_size >> 24U) & 0xFFU),
      static_cast<std::uint8_t>((payload_size >> 16U) & 0xFFU),
      static_cast<std::uint8_t>((payload_size >> 8U) & 0xFFU),
      static_cast<std::uint8_t>(payload_size & 0xFFU),
  };
}

TEST(FrameDecoderTest, DecodeReturnsCompleteFrame) {
  const auto decoder = MakeFrameDecoder(DefaultFrameConfig());
  const std::vector<std::uint8_t> payload{0x11, 0x22, 0x33};
  const std::vector<std::uint8_t> frame = BuildFrame(payload);

  FrameDecodeResult result{};
  const Status s = decoder->Decode(frame.data(), frame.size(), &result);

  ASSERT_TRUE(s.Ok());
  ASSERT_EQ(result.bytes_consumed, frame.size());
  ASSERT_EQ(result.completed_frames.size(), 1U);
  EXPECT_EQ(result.completed_frames[0].payload_size, payload.size());
  EXPECT_EQ(std::vector<std::uint8_t>(result.completed_frames[0].payload,
                                      result.completed_frames[0].payload + payload.size()),
            payload);
}

TEST(FrameDecoderTest, DecodeReturnsOkForPartialHeader) {
  const auto decoder = MakeFrameDecoder(DefaultFrameConfig());
  const std::vector<std::uint8_t> buffer{0x00, 0x00, 0x00};

  FrameDecodeResult result{};
  const Status s = decoder->Decode(buffer.data(), buffer.size(), &result);

  EXPECT_TRUE(s.Ok());
  EXPECT_EQ(result.bytes_consumed, 0U);
  EXPECT_TRUE(result.completed_frames.empty());
}

TEST(FrameDecoderTest, DecodeReturnsOkForPartialPayload) {
  const auto decoder = MakeFrameDecoder(DefaultFrameConfig());
  std::vector<std::uint8_t> buffer{0x00, 0x00, 0x00, 0x04, 0xAA, 0xBB};

  FrameDecodeResult result{};
  const Status s = decoder->Decode(buffer.data(), buffer.size(), &result);

  EXPECT_TRUE(s.Ok());
  EXPECT_EQ(result.bytes_consumed, 0U);
  EXPECT_TRUE(result.completed_frames.empty());
}

TEST(FrameDecoderTest, DecodeSplitsMultipleFramesAndLeavesTrailingPartialFrame) {
  const auto decoder = MakeFrameDecoder(DefaultFrameConfig());
  const std::vector<std::uint8_t> payload_a{0x01, 0x02};
  const std::vector<std::uint8_t> payload_b{0x03};
  const std::vector<std::uint8_t> payload_c{0x04, 0x05, 0x06};
  const std::vector<std::uint8_t> frame_a = BuildFrame(payload_a);
  const std::vector<std::uint8_t> frame_b = BuildFrame(payload_b);
  const std::vector<std::uint8_t> frame_c = BuildFrame(payload_c);

  std::vector<std::uint8_t> buffer;
  buffer.insert(buffer.end(), frame_a.begin(), frame_a.end());
  buffer.insert(buffer.end(), frame_b.begin(), frame_b.end());
  buffer.insert(buffer.end(), frame_c.begin(), frame_c.begin() + 5);

  FrameDecodeResult result{};
  const Status s = decoder->Decode(buffer.data(), buffer.size(), &result);

  ASSERT_TRUE(s.Ok());
  ASSERT_EQ(result.completed_frames.size(), 2U);
  EXPECT_EQ(result.bytes_consumed, frame_a.size() + frame_b.size());
  EXPECT_EQ(result.completed_frames[0].payload_size, payload_a.size());
  EXPECT_EQ(result.completed_frames[1].payload_size, payload_b.size());
}

TEST(FrameDecoderTest, DecodeReturnsFrameErrorForZeroLength) {
  const auto decoder = MakeFrameDecoder(DefaultFrameConfig());
  const std::vector<std::uint8_t> buffer = ZeroLengthFrame();

  FrameDecodeResult result{};
  const Status s = decoder->Decode(buffer.data(), buffer.size(), &result);

  EXPECT_EQ(s.code, ErrorCode::kFrameError);
  EXPECT_EQ(result.bytes_consumed, 0U);
  EXPECT_TRUE(result.completed_frames.empty());
}

TEST(FrameDecoderTest, DecodeReturnsFrameErrorForOversizedLength) {
  FrameConfig config = DefaultFrameConfig();
  config.max_frame_length = 1024;
  const auto decoder = MakeFrameDecoder(config);
  const std::vector<std::uint8_t> buffer = OversizedFrameHeader(1025);

  FrameDecodeResult result{};
  const Status s = decoder->Decode(buffer.data(), buffer.size(), &result);

  EXPECT_EQ(s.code, ErrorCode::kFrameError);
  EXPECT_EQ(result.bytes_consumed, 0U);
  EXPECT_TRUE(result.completed_frames.empty());
}

TEST(FrameDecoderTest, DecodeSupportsByteByByteAccumulationAtCallSite) {
  const auto decoder = MakeFrameDecoder(DefaultFrameConfig());
  const std::vector<std::uint8_t> payload{0x21, 0x22, 0x23, 0x24};
  const std::vector<std::uint8_t> frame = BuildFrame(payload);

  std::vector<std::uint8_t> staging;
  staging.reserve(frame.size());

  for (std::size_t i = 0; i < frame.size(); ++i) {
    staging.push_back(frame[i]);

    FrameDecodeResult result{};
    const Status s = decoder->Decode(staging.data(), staging.size(), &result);
    ASSERT_TRUE(s.Ok());

    if (i + 1U != frame.size()) {
      EXPECT_EQ(result.bytes_consumed, 0U);
      EXPECT_TRUE(result.completed_frames.empty());
      continue;
    }

    ASSERT_EQ(result.completed_frames.size(), 1U);
    EXPECT_EQ(result.bytes_consumed, frame.size());
    EXPECT_EQ(result.completed_frames[0].payload_size, payload.size());
  }
}

}  // namespace
}  // namespace robotaxi
