#!/usr/bin/env bash
set -euo pipefail

# 固定基线脚本（长连接稳态场景）
# 目标：为“优化前后对比”提供长期稳定、可重复、可追溯的同口径测试。

BASELINE_ID="long_conn_v1_20260701"

# 固定参数（默认不建议改动）
TASKS=20000
THREADS=4
QUEUE_CAPACITY=8192
PAYLOAD_BYTES=256
CLIENT_CONNECTIONS=8
WARMUP_ROUNDS=1
REPEATS=5

# 回归门槛（用于快速判定优化是否有效）
MIN_THROUGHPUT_TPS=300000
MAX_P99_US=45000

BIN="./build/e2e_benchmark"
OUT_ROOT="benchmarks/results"
TAG=""

usage() {
  cat <<'EOF'
用法:
  benchmarks/run_fixed_baseline.sh [options]

说明:
  该脚本固定长连接基线参数，执行“预热 + 正式采样”，输出 raw/summary/gate 三类结果。

可选参数:
  --warmup-rounds N    预热轮数（默认 1）
  --repeats N          正式采样轮数（默认 5）
  --out-root PATH      输出根目录（默认 benchmarks/results）
  --tag NAME           输出目录附加标签（可选）
  -h, --help           显示帮助

固定参数（由脚本统一维护）:
  tasks=20000
  threads=4
  queue_capacity=8192
  payload_bytes=256
  client_connections=8
EOF
}

require_positive_int() {
  local name="$1"
  local value="$2"
  if ! [[ "$value" =~ ^[0-9]+$ ]] || [[ "$value" == "0" ]]; then
    echo "参数错误: ${name} 必须是正整数, 当前=${value}" >&2
    exit 2
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --warmup-rounds)
      WARMUP_ROUNDS="$2"
      shift 2
      ;;
    --repeats)
      REPEATS="$2"
      shift 2
      ;;
    --out-root)
      OUT_ROOT="$2"
      shift 2
      ;;
    --tag)
      TAG="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "未知参数: $1" >&2
      usage
      exit 2
      ;;
  esac
done

require_positive_int "--warmup-rounds" "$WARMUP_ROUNDS"
require_positive_int "--repeats" "$REPEATS"

if [[ ! -x "$BIN" ]]; then
  echo "未找到可执行文件: $BIN" >&2
  echo "请先执行: cmake --build build --target e2e_benchmark" >&2
  exit 3
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "缺少依赖: jq（用于解析 JSON 输出）" >&2
  exit 3
fi

DATE_TAG="$(date +%Y%m%d_%H%M%S)"
RUN_DIR_NAME="fixed_baseline_${BASELINE_ID}_${DATE_TAG}"
if [[ -n "$TAG" ]]; then
  RUN_DIR_NAME="${RUN_DIR_NAME}_${TAG}"
fi

RUN_DIR="${OUT_ROOT}/${RUN_DIR_NAME}"
ROUNDS_DIR="${RUN_DIR}/rounds"
mkdir -p "$ROUNDS_DIR"

RAW_CSV="${RUN_DIR}/raw.csv"
SUMMARY_CSV="${RUN_DIR}/summary.csv"
GATE_TXT="${RUN_DIR}/gate.txt"
MANIFEST_TXT="${RUN_DIR}/manifest.txt"

cat > "$MANIFEST_TXT" <<EOF
baseline_id=${BASELINE_ID}
tasks=${TASKS}
threads=${THREADS}
queue_capacity=${QUEUE_CAPACITY}
payload_bytes=${PAYLOAD_BYTES}
client_connections=${CLIENT_CONNECTIONS}
warmup_rounds=${WARMUP_ROUNDS}
sample_repeats=${REPEATS}
min_throughput_tps=${MIN_THROUGHPUT_TPS}
max_p99_us=${MAX_P99_US}
git_branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)
git_commit=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
hostname=$(hostname)
kernel=$(uname -sr)
run_dir=${RUN_DIR}
EOF

printf "run_id,phase,total_tasks,worker_threads,queue_capacity,payload_bytes,client_connections,duration_ms,throughput_tps,p50_us,p99_us,p999_us,connection_success,connection_failed,send_failed,enqueue_success,enqueue_timeout,tp_executed,executor_executed,completed_frames,frame_decode_error,dropped_on_backpressure,integrity_pass\n" > "$RAW_CSV"

run_one() {
  local phase="$1"
  local run_id="$2"
  local json_file="${ROUNDS_DIR}/${phase}_r${run_id}.json"

  "$BIN" \
    --tasks "$TASKS" \
    --threads "$THREADS" \
    --queue-capacity "$QUEUE_CAPACITY" \
    --payload-bytes "$PAYLOAD_BYTES" \
    --client-connections "$CLIENT_CONNECTIONS" \
    --output-format json \
    --output-file "$json_file" >/dev/null

  local line
  line="$(jq -r '[
    .total_tasks,
    .worker_threads,
    .queue_capacity,
    .payload_bytes,
    .client_connections,
    .duration_ms,
    .throughput_tps,
    .latency_us.p50,
    .latency_us.p99,
    .latency_us.p999,
    .stability.connection_success,
    .stability.connection_failed,
    .stability.send_failed,
    .queue_metrics.enqueue_success,
    .queue_metrics.enqueue_timeout,
    .thread_pool_metrics.executed_tasks,
    .executor_metrics.executed_tasks,
    .ingress_metrics.completed_frames,
    .ingress_metrics.frame_decode_error,
    .ingress_metrics.dropped_on_backpressure
  ] | @csv' "$json_file")"

  # 完整性判定：链路必须全闭合且零错误。
  local integrity_pass
  integrity_pass="$(jq -r --argjson tasks "$TASKS" '
    if (
      .queue_metrics.enqueue_success == $tasks and
      .thread_pool_metrics.executed_tasks == $tasks and
      .executor_metrics.executed_tasks == $tasks and
      .ingress_metrics.completed_frames == $tasks and
      .queue_metrics.enqueue_timeout == 0 and
      .stability.connection_failed == 0 and
      .stability.send_failed == 0 and
      .ingress_metrics.frame_decode_error == 0 and
      .ingress_metrics.dropped_on_backpressure == 0
    ) then 1 else 0 end
  ' "$json_file")"

  printf "%s,%s,%s,%s\n" "$run_id" "$phase" "$line" "$integrity_pass" >> "$RAW_CSV"
}

echo "[fixed-baseline] 开始预热: ${WARMUP_ROUNDS} 轮"
for ((i = 1; i <= WARMUP_ROUNDS; ++i)); do
  run_one "warmup" "$i"
  echo "  预热完成: ${i}/${WARMUP_ROUNDS}"
done

echo "[fixed-baseline] 开始正式采样: ${REPEATS} 轮"
for ((i = 1; i <= REPEATS; ++i)); do
  run_one "sample" "$i"
  echo "  采样完成: ${i}/${REPEATS}"
done

awk -F',' '
BEGIN {
  OFS=",";
  print "sample_count,throughput_mean,throughput_stddev,throughput_min,throughput_max,p50_mean,p99_mean,p999_mean,duration_mean,integrity_pass_count,integrity_pass_rate";
}
NR > 1 && $2 == "sample" {
  n++;
  t = $9 + 0.0;
  p50 = $10 + 0.0;
  p99 = $11 + 0.0;
  p999 = $12 + 0.0;
  d = $8 + 0.0;
  pass = $23 + 0;

  sum_t += t; sumsq_t += t * t;
  sum_p50 += p50;
  sum_p99 += p99;
  sum_p999 += p999;
  sum_d += d;
  pass_count += pass;

  if (n == 1 || t < min_t) min_t = t;
  if (n == 1 || t > max_t) max_t = t;
}
END {
  if (n == 0) {
    print 0,0,0,0,0,0,0,0,0,0,0;
    exit 0;
  }

  mean_t = sum_t / n;
  mean_p50 = sum_p50 / n;
  mean_p99 = sum_p99 / n;
  mean_p999 = sum_p999 / n;
  mean_d = sum_d / n;

  var_t = (sumsq_t / n) - (mean_t * mean_t);
  if (var_t < 0) var_t = 0;
  std_t = sqrt(var_t);

  pass_rate = pass_count / n;

  printf "%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%.4f\n", \
    n, mean_t, std_t, min_t, max_t, mean_p50, mean_p99, mean_p999, mean_d, pass_count, pass_rate;
}
' "$RAW_CSV" > "$SUMMARY_CSV"

THROUGHPUT_MEAN="$(awk -F',' 'NR==2 {print $2}' "$SUMMARY_CSV")"
P99_MEAN="$(awk -F',' 'NR==2 {print $7}' "$SUMMARY_CSV")"
PASS_COUNT="$(awk -F',' 'NR==2 {print $10}' "$SUMMARY_CSV")"

GATE_PASS=1

if [[ "$PASS_COUNT" -ne "$REPEATS" ]]; then
  GATE_PASS=0
fi

if ! awk -v v="$THROUGHPUT_MEAN" -v min="$MIN_THROUGHPUT_TPS" 'BEGIN {exit !(v+0 >= min+0)}'; then
  GATE_PASS=0
fi

if ! awk -v v="$P99_MEAN" -v max="$MAX_P99_US" 'BEGIN {exit !(v+0 <= max+0)}'; then
  GATE_PASS=0
fi

{
  echo "baseline_id=${BASELINE_ID}"
  echo "gate_result=$([[ "$GATE_PASS" -eq 1 ]] && echo PASS || echo FAIL)"
  echo "rule_integrity=all sample rounds must pass integrity"
  echo "rule_throughput=throughput_mean >= ${MIN_THROUGHPUT_TPS}"
  echo "rule_p99=p99_mean <= ${MAX_P99_US}"
  echo "throughput_mean=${THROUGHPUT_MEAN}"
  echo "p99_mean=${P99_MEAN}"
  echo "integrity_pass_count=${PASS_COUNT}/${REPEATS}"
} > "$GATE_TXT"

echo "[fixed-baseline] 执行完成"
echo "- 结果目录: ${RUN_DIR}"
echo "- 明细: ${RAW_CSV}"
echo "- 汇总: ${SUMMARY_CSV}"
echo "- 判定: ${GATE_TXT}"
echo
cat "$SUMMARY_CSV"
echo
cat "$GATE_TXT"
