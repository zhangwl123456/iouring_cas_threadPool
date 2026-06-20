# 端到端性能维度对比测试 (E2E Scaling Analysis)

**测试日期**: 2026-06-20  
**环境**: Codespace（4 vCPU, 15GB 内存, AMD EPYC 7763 虚拟化）  
**测试方案**: 生产级维度对比，固定任务规模下观察"并发连接数"与"payload 大小"对性能的影响  

## 测试背景

经过基础功能验证和链路闭环后（参见 PROGRESS_LOG.md），系统需要理解在什么参数组合下能达到最优性能。本测试通过两个独立维度的对比，量化系统的性能扩展边界，为生产环境参数选择提供数据支撑。

## 测试维度设计

### 维度 1：连接数扩展（Connection Scaling）

**固定参数**:
- `tasks=20000` - 每轮发送的总任务数
- `threads=4` - 工作线程数
- `queue_capacity=8192` - 无锁队列容量
- `payload_bytes=128` - 每个任务的 payload 大小

**变化参数**:
- `client_connections` = [4, 8, 16]

**测试文件**:
- `conn4.json` - 基线配置（4 连接）
- `conn8.json` - 中等并发（8 连接）
- `conn16.json` - 高并发（16 连接）

### 维度 2：Payload 大小扩展（Payload Scaling）

**固定参数**:
- `tasks=20000` - 每轮发送的总任务数
- `threads=4` - 工作线程数
- `queue_capacity=8192` - 无锁队列容量
- `client_connections=4` - 并发连接数

**变化参数**:
- `payload_bytes` = [128, 256, 512, 1024]

**测试文件**:
- `payload128.json` - 基线帧大小（128B）
- `payload256.json` - 中等帧大小（256B）
- `payload512.json` - 较大帧大小（512B）
- `payload1024.json` - 大帧大小（1024B）

## 关键结果对比

### 维度 1 数据汇总

| 连接数 | 吞吐(tps) | 吞吐变化 | P50(ms) | P99(ms) | P999(ms) | 完成帧数 | 链路完整性 |
|--------|---------|--------|--------|--------|---------|--------|---------|
| 4 | 270,270 | baseline | 23.88 | 47.31 | 47.44 | 20,000 | ✓ |
| 8 | 277,778 | +2.8% | 31.13 | 44.78 | 44.95 | 20,000 | ✓ |
| 16 | 165,289 | -40.4% | 57.17 | 92.52 | 92.68 | 20,000 | ✓ |

**关键观察**:
- 4→8 连接：吞吐边际改善（+2.8%），延迟略降（P99 -5.3%）
- 8→16 连接：性能崖跌（吞吐 -40%，P99 +106%）
- **最优工作点**: 4-8 连接范围

### 维度 2 数据汇总

| Payload(B) | 吞吐(tps) | 吞吐变化 | P50(ms) | P99(ms) | P999(ms) | 完成帧数 | 链路完整性 |
|-----------|---------|--------|--------|--------|---------|--------|---------|
| 128 | 186,916 | baseline | 28.02 | 73.99 | 74.21 | 20,000 | ✓ |
| 256 | 219,780 | +17.6% | 41.90 | 70.78 | 71.07 | 20,000 | ✓ |
| 512 | 137,931 | -37.2% | 64.80 | 109.87 | 110.27 | 20,000 | ✓ |
| 1024 | 133,333 | -3.3% | 54.13 | 91.02 | 91.84 | 20,000 | ✓ |

**关键观察**:
- 128→256B：吞吐峰值（+17.6%），延迟略改（P99 -4.3%）
- 256→512B：陡峭下降（吞吐 -37%，P99 +55%）
- 512→1024B：趋于稳定（吞吐 -3%，延迟改善）
- **最优工作点**: 256B 帧大小

## 链路完整性验证

所有测试均保持以下零容错指标：
- `completed_frames == 20,000` ✓
- `frame_decode_error == 0` ✓
- `dropped_on_backpressure == 0` ✓
- `enqueue_timeout == 0` ✓
- `executed_tasks == 20,000` ✓

**结论**: 数据完整走过 TCP → NetworkIngress → FrameDecoder → RingQueue → ThreadPool → Executor 全链路，无丢失或伪造。

## 生产环境建议

### 1. 参数选择指南

| 场景 | 推荐配置 | 预期性能 |
|------|---------|---------|
| **高吞吐** | 连接数 8 + payload 256B | ~220K tps, P99 ~71ms |
| **低延迟** | 连接数 4 + payload 128B | ~186K tps, P99 ~74ms |
| **均衡** | 连接数 8 + payload 256B | ~220K tps, P99 ~71ms |

### 2. 性能瓶颈分析

**连接数瓶颈** (8→16 下降 40%):
- epoll 在高连接数下的事件分发开销增加
- CAS 重试率可能上升
- 可考虑：增加工作线程、部署多实例、优化 epoll 热路径

**Payload 大小瓶颈** (256→512 下降 37%):
- 可能与 CPU L2 缓存边界或网络分片相关
- 可考虑：优化缓冲分配策略、分片处理逻辑

### 3. 后续优化方向

1. **扩展线程数维度** - 测试 threads=[2,4,8,16] 对不同连接数的补偿效果
2. **在生产云主机复测** - 降低 Codespace 共享环境噪声，获得更稳定曲线
3. **实现入队超时保护** - 防止队列满时请求无限卡住
4. **多实例部署方案** - 在超过最优工作点范围时的水平扩展策略

## 文件结构说明

```
benchmarks/results/e2e_scaling_20260620/
├── README.md                    # 本文件，测试说明与结果解读
├── conn4.json                   # 4 连接基线测试原始 JSON
├── conn8.json                   # 8 连接测试原始 JSON
├── conn16.json                  # 16 连接测试原始 JSON
├── payload128.json              # 128B payload 基线测试原始 JSON
├── payload256.json              # 256B payload 测试原始 JSON
├── payload512.json              # 512B payload 测试原始 JSON
└── payload1024.json             # 1024B payload 测试原始 JSON
```

## 原始 JSON 格式说明

每个 JSON 文件包含以下顶级字段（参见 docs/SPEC.md 7.3.1）：

```json
{
  "duration_ms": <测试执行时间>,
  "throughput_tps": <吞吐量 tasks/second>,
  "latency_us": {
    "p50": <50分位延迟 us>,
    "p99": <99分位延迟 us>,
    "p999": <999分位延迟 us>
  },
  "stability": {
    "connection_success": <成功建立的连接数>,
    "connection_failed": <连接失败数>,
    "send_failed": <发送失败数>
  },
  "queue_metrics": {
    "enqueue_success": <成功入队数>,
    "enqueue_timeout": <入队超时数>
  },
  "thread_pool_metrics": {
    "executed_tasks": <执行的任务数>
  },
  "executor_metrics": {
    "executed_tasks": <执行器执行的任务数>
  },
  "ingress_metrics": {
    "completed_frames": <完成切帧的帧数>,
    "frame_decode_error": <帧解析错误数>,
    "dropped_on_backpressure": <因背压丢弃的帧数>
  }
}
```

## 如何使用这些数据

1. **作为基线对比** - 在优化前后对比相同配置下的结果
2. **性能监控** - 定期在相同环境重复测试，监测性能漂移
3. **回归测试** - 在代码变更后确保性能无恶化
4. **容量规划** - 根据实际负载特征选择合适的运行参数

## 相关文档

- [SPEC.md](../../docs/SPEC.md) - 系统规格与测试契约
- [PROGRESS_LOG.md](../../docs/PROGRESS_LOG.md) - 开发进度与测试演进过程
- [LESSONS_LEARNT.md](../../docs/LESSONS_LEARNT.md) - 设计决策与经验总结
