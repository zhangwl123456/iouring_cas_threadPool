# 容量模式基线测试 (2026-06-17)

## 测试目标

建立 Codespace 环境下容量模式执行器的吞吐基线，并通过单变量优化探索性能提升空间。

## 测试参数

| 参数 | 值 |
|-----|-----|
| tasks | 20000 |
| threads | 4 |
| queue_capacity | 8192 |
| payload_bytes | 128 |
| 环境 | Codespace (4 vCPU, 15 GiB) |
| 采样方式 | 预热 1 轮 + 正式采样 5 轮 |

## 优化迭代

1. **baseline** - 初始基线
   - 吞吐均值：323,360.71 tps
   - P99 延迟：2,142.93 us
   
2. **step2_ingress_opt** - Ingress 热路径优化（缓冲压缩策略）
   - 吞吐均值：317,605.48 tps（↓1.8%）
   - P99 延迟：2,707.96 us（↑26.4%）
   - 评估：在 Codespace 环境中，缓冲压缩策略未呈现明显收益，需在固定云主机复测

3. **step2_conn_et** - 连接 FD 切换为 ET（边界触发）
   - 吞吐均值：223,632.57 tps（↓29.5%）
   - P99 延迟：1,753.37 us（↓35.3%）
   - 评估：ET 表现为吞吐下降、尾延迟改善、波动收敛

4. **step2_conn_et_reuseport** - 添加 SO_REUSEPORT
   - 吞吐均值：265,890.67 tps（+18.9% vs ET，-17.8% vs baseline）
   - P99 延迟：2,009.45 us
   - 评估：SO_REUSEPORT 相比纯 ET 有吞吐回升

5. **step2_multi_acceptor** - 多监听 socket（2 个）+ ET
   - 吞吐均值：348,002.04 tps（+7.6% vs baseline）
   - P99 延迟：1,799.34 us（↓15.9% vs baseline）
   - 评估：多 acceptor 架构在 Codespace 环境中呈现最优表现

## 关键发现

- **多 acceptor 架构**优于单 listen socket，且与 ET 模式组合效果最优
- **环境敏感性**：Codespace 共享环境的波动较大，结论需在固定云主机复测
- **CAS 重试率**低幅波动（0.0004~0.0012），说明队列竞争不是主瓶颈
- **工作线程空轮询**比例在 0.7%~1.24%，说明防饥饿策略有效

## 后续建议

1. 在 16 核 / 64GB 标准环境重复同一采样协议，对比结果差异
2. 如维持多 acceptor 方案，需补齐相应文档与集成测试
3. 考虑评估"增加工作线程数"对吞吐的补偿效果

## 文件清单

| 文件 | 说明 |
|-----|------|
| baseline_raw.csv | 基线原始数据（5 轮采样） |
| baseline_summary.csv | 基线汇总统计 |
| step2_ingress_opt_raw.csv | ingress 优化后的原始数据 |
| step2_ingress_opt_summary.csv | ingress 优化后的汇总 |
| step2_conn_et_raw.csv | ET 模式原始数据 |
| step2_conn_et_summary.csv | ET 模式汇总 |
| step2_conn_et_reuseport_raw.csv | ET+REUSEPORT 原始数据 |
| step2_conn_et_reuseport_summary.csv | ET+REUSEPORT 汇总 |
| step2_multi_acceptor_raw.csv | 多 acceptor 原始数据 |
| step2_multi_acceptor_summary.csv | 多 acceptor 汇总 |

---

**执行日期**: 2026-06-17  
**执行者**: 自动化测试  
**环境**: Codespace AMD EPYC 7763 虚拟化 (4 vCPU)  
**结论**: 多 acceptor 架构在 Codespace 环境中最优，但需在生产环境复测
