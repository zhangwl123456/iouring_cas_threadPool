# 测试结果文件夹组织规范

本文档定义了 `benchmarks/results/` 目录下所有测试结果、报告的组织方式。

## 目录结构

```
benchmarks/results/
├── ORGANIZATION.md                    # 本文件（组织规范）
├── capacity_baseline_20260617/        # 容量模式基线与优化测试（2026-06-17）
│   ├── README.md
│   ├── baseline_raw.csv               # 原始采样数据
│   ├── baseline_summary.csv           # 汇总统计
│   ├── step2_ingress_opt_raw.csv
│   ├── step2_ingress_opt_summary.csv
│   ├── step2_conn_et_raw.csv
│   ├── step2_conn_et_summary.csv
│   ├── step2_conn_et_reuseport_raw.csv
│   ├── step2_conn_et_reuseport_summary.csv
│   ├── step2_multi_acceptor_raw.csv
│   └── step2_multi_acceptor_summary.csv
├── e2e_scaling_20260620/              # 端到端性能维度测试（2026-06-20）
│   ├── README.md
│   ├── conn4.json                     # 连接数维度：4/8/16
│   ├── conn8.json
│   ├── conn16.json
│   ├── payload128.json                # Payload 大小维度：128/256/512/1024 字节
│   ├── payload256.json
│   ├── payload512.json
│   ├── payload1024.json
│   ├── summary.csv                    # 汇总对比表
│   └── README.md
├── e2e_scaling_20260622/              # 端到端补充测试（2026-06-22）
│   ├── README.md
│   ├── conn8_payload256.json          # 特定参数组合测试
│   ├── conn8_payload256_r1.json       # 多轮重复测试
│   ├── conn8_payload256_r2.json
│   ├── conn8_payload256_r3.json
│   └── conn8_payload256_rN.json       # （后续轮次）
├── e2e_spec_compliant_20260622/       # SPEC 7.3.1/7.3.2 规范化端到端测试（2026-06-22）
│   ├── README.md
│   ├── environment.txt                # 测试环境说明
│   ├── warmup.json                    # 预热轮结果
│   ├── sample_r1.json                 # 正式采样轮 1-5
│   ├── sample_r2.json
│   ├── sample_r3.json
│   ├── sample_r4.json
│   ├── sample_r5.json
│   ├── summary.csv                    # 多轮汇总表
│   ├── stats.txt                      # 统计分析（均值、标准差、最小/最大）
│   ├── REPORT.md                      # 完整分析报告（环境、参数、结果、对标）
│   └── chain_integrity_log.txt        # （可选）链路完整性验证日志
```

## 命名规则

### 目录命名
- 格式：`<测试类型>_<YYYYMMDD>[_<阶段>]`
  - 测试类型：`capacity_baseline`（容量模式基线）、`e2e_scaling`（端到端维度）、`e2e_spec_compliant`（SPEC 规范）、`e2e_stress`（压力测试）、`e2e_ha`（高可用测试）等
  - YYYYMMDD：测试日期（ISO 8601）
  - 阶段（可选）：如 `_step2`、`_optimization`、`_production` 等

- 示例：
  - `capacity_baseline_20260617` - 2026-06-17 的容量基线测试
  - `e2e_spec_compliant_20260622` - 2026-06-22 的SPEC规范化测试
  - `e2e_stress_20261015_iteration3` - 2026-10-15 的第 3 轮压力测试

### 文件命名

#### JSON 原始结果
- 格式：`<test_type>_<parameters>_r<N>.json` 或 `<test_type>_<parameters>.json`
  - test_type：测试类型简写（如 `e2e_scaling`、`capacity`）
  - parameters：参数组合（如 `conn8_payload256`、`threads4_queue8192`）
  - r<N>：多轮测试时的轮次号（r1, r2, ..., r5）

- 示例：
  - `e2e_scaling_20260622_conn8_payload256_r1.json` - 连接数 8、payload 256B 的第 1 轮
  - `capacity_baseline_threads4_queue8192_r3.csv` - 线程数 4、队列容量 8192 的第 3 轮

#### CSV 汇总表
- 格式：`<category>_<parameters>_[raw|summary].csv`
  - raw.csv：所有原始采样数据行
  - summary.csv：多轮统计汇总（均值、标准差、最小/最大）

- 示例：
  - `capacity_baseline_ingress_opt_raw.csv`
  - `capacity_baseline_ingress_opt_summary.csv`

#### 报告和说明
- 格式：`README.md`、`REPORT.md`、`ANALYSIS.md`、`<topic>.txt` 等
  - README.md：目录说明（测试目标、参数、时间、执行者、关键发现）
  - REPORT.md：详细分析报告（含环境、对标、结论、建议）
  - environment.txt：硬件环境与条件说明
  - chain_integrity_log.txt：链路完整性验证日志
  - stats.txt：统计数据与分析摘要

## 文件内容规范

### 每个日期/阶段的目录必须包含

✅ **必需**：
- [ ] `README.md` - 说明此次测试的目标、参数、执行时间、关键发现
- [ ] 至少一个结果文件（JSON、CSV 或 TXT）

✅ **推荐**：
- [ ] `REPORT.md` - 详细分析报告
- [ ] `environment.txt` - 硬件环境说明（仅在环境特殊或多环境对比时需要）
- [ ] 统计汇总（summary.csv 或 stats.txt）

### README.md 模板

```markdown
# 测试说明 - [测试类型] ([日期])

## 目标
此次测试的目标是什么？（如：验证背压机制、评估连接数扩展性等）

## 测试参数
- tasks: 20000
- threads: 4
- queue_capacity: 8192
- payload_bytes: 256
- client_connections: 8
- 采样方式：1 轮预热 + 5 轮正式采样

## 执行时间
- 日期：2026-06-22
- 环境：Codespace (4 vCPU, 15 GiB)
- 执行者：CI / 手动

## 关键结果
- 吞吐：206k tps（均值）
- P99 延迟：73.9 ms
- 链路完整性：✅ 零容错验证通过

## 文件清单
- warmup.json：预热轮结果（不计入统计）
- sample_r1.json - sample_r5.json：5 轮正式采样原始 JSON
- summary.csv：5 轮汇总表
- stats.txt：均值、标准差、最小/最大统计
- REPORT.md：详细分析

## 后续行动
- 如需继续深化，建议在 16 核标准环境复测
- 对标目标：吞吐 >= 200k tps，P99 <= 5ms
```

## 后续测试规则

### 新建测试时的流程

1. **创建目录**
   ```bash
   mkdir -p benchmarks/results/<test_type>_<YYYYMMDD>[_<phase>]
   ```

2. **在目录中生成结果文件**
   - 原始数据：`*_r1.json`、`*_r2.json` 等（单轮测试可省略 `_rN`）
   - 汇总表：`summary.csv`、`stats.txt` 等
   - 报告：`REPORT.md`、`README.md`

3. **添加说明文档**
   - 必需：`README.md`（说明目标、参数、结果摘要）
   - 推荐：`environment.txt`（硬件环境）、`REPORT.md`（详细分析）

4. **示例命令**
   ```bash
   # 1. 创建目录
   mkdir -p benchmarks/results/e2e_stress_20261015_round1

   # 2. 运行测试（以 e2e_benchmark 为例）
   ./build/e2e_benchmark \
     --tasks 100000 --threads 8 --queue-capacity 16384 \
     --payload-bytes 256 --client-connections 16 \
     --output-format json \
     --output-file benchmarks/results/e2e_stress_20261015_round1/stress_r1.json

   # 3. 重复多轮（r2, r3, ..., r5）

   # 4. 生成统计汇总脚本或 Python 脚本

   # 5. 创建 README.md 和 REPORT.md

   # 6. 提交 commit
   git add benchmarks/results/e2e_stress_20261015_round1/
   git commit -m "Test: e2e stress test round 1 on 2026-10-15"
   ```

## 历史结构对照

| 日期 | 目录 | 测试类型 | 参数 | 文件数 |
|-----|------|--------|------|--------|
| 2026-06-17 | capacity_baseline_20260617 | 容量基线 + 优化 | tasks=20000, threads=4, queue=8192, payload=128 | 10 (5 组测试各含 raw+summary) |
| 2026-06-20 | e2e_scaling_20260620 | 端到端维度 | tasks=20000, threads=4, queue=8192, 多参数维度 | 9 (7 个 JSON + summary.csv + README) |
| 2026-06-22 | e2e_scaling_20260622 | 端到端补充 | tasks=20000, threads=4, queue=8192, conn8, payload256 | 3+ (多轮 JSON) |
| 2026-06-22 | e2e_spec_compliant_20260622 | SPEC 规范化 | 预热 1 + 采样 5 | 9+ (warmup + 5 samples + 汇总 + 报告) |

## 备注

- 所有时间戳使用 ISO 8601 格式 (YYYYMMDD)
- JSON 文件应遵循 SPEC 7.3.1 的输出契约
- CSV 文件应包含列头，支持 Excel/Pandas 打开
- 报告文件使用 Markdown，便于查看和版本控制
- 敏感文件（密钥、内部 IP）不应提交到版本控制

---

**最后更新**: 2026-06-22  
**规范状态**: ✅ 生效
