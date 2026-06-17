#!/usr/bin/env bash
set -euo pipefail

TASKS=20000
THREADS=4
QUEUE_CAPACITY=8192
PAYLOAD_BYTES=128
WARMUP_ROUNDS=1
REPEATS=5
OUT_DIR="benchmarks/results"
TAG=""
BIN="./build/capacity_baseline_benchmark"

usage() {
  cat <<'EOF'
用法:
  benchmarks/run_capacity_sampling.sh [options]

可选参数:
  --tasks N             任务总数 (默认 20000)
  --threads N           工作线程数 (默认 4)
  --queue-capacity N    队列容量 (默认 8192)
  --payload-bytes N     payload 字节数 (默认 128)
  --warmup-rounds N     预热轮数 (默认 1)
  --repeats N           正式采样轮数 (默认 5)
  --out-dir PATH        输出目录 (默认 benchmarks/results)
  --tag NAME            文件名前缀标签 (可选)
  -h, --help            显示帮助
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
    --tasks)
      TASKS="$2"
      shift 2
      ;;
    --threads)
      THREADS="$2"
      shift 2
      ;;
    --queue-capacity)
      QUEUE_CAPACITY="$2"
      shift 2
      ;;
    --payload-bytes)
      PAYLOAD_BYTES="$2"
      shift 2
      ;;
    --warmup-rounds)
      WARMUP_ROUNDS="$2"
      shift 2
      ;;
    --repeats)
      REPEATS="$2"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="$2"
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

require_positive_int "--tasks" "$TASKS"
require_positive_int "--threads" "$THREADS"
require_positive_int "--queue-capacity" "$QUEUE_CAPACITY"
require_positive_int "--payload-bytes" "$PAYLOAD_BYTES"
require_positive_int "--warmup-rounds" "$WARMUP_ROUNDS"
require_positive_int "--repeats" "$REPEATS"

if [[ ! -x "$BIN" ]]; then
  echo "未找到可执行文件: $BIN" >&2
  echo "请先执行: cmake --build build --target capacity_baseline_benchmark" >&2
  exit 3
fi

mkdir -p "$OUT_DIR"

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
PREFIX="capacity_baseline_${TIMESTAMP}"
if [[ -n "$TAG" ]]; then
  PREFIX="${PREFIX}_${TAG}"
fi

RAW_CSV="${OUT_DIR}/${PREFIX}_raw.csv"
SUMMARY_CSV="${OUT_DIR}/${PREFIX}_summary.csv"

printf "run_id,phase,task_count,worker_threads,queue_capacity,payload_bytes,duration_ms,throughput_tps,p99_latency_us,p999_latency_us,cas_retry_rate,worker_empty_poll_ratio\n" > "$RAW_CSV"

run_one() {
  local phase="$1"
  local run_id="$2"

  local line
  line="$($BIN \
    --tasks "$TASKS" \
    --threads "$THREADS" \
    --queue-capacity "$QUEUE_CAPACITY" \
    --payload-bytes "$PAYLOAD_BYTES" \
    --output-format csv | tail -n 1)"

  printf "%s,%s,%s\n" "$run_id" "$phase" "$line" >> "$RAW_CSV"
}

echo "开始预热: ${WARMUP_ROUNDS} 轮"
for ((i = 1; i <= WARMUP_ROUNDS; ++i)); do
  run_one "warmup" "$i"
  echo "  预热完成: ${i}/${WARMUP_ROUNDS}"
done

echo "开始正式采样: ${REPEATS} 轮"
for ((i = 1; i <= REPEATS; ++i)); do
  run_one "sample" "$i"
  echo "  采样完成: ${i}/${REPEATS}"
done

awk -F',' '
BEGIN {
  OFS=",";
  print "sample_count,throughput_mean,throughput_stddev,throughput_min,throughput_max,p99_mean,p99_stddev,p99_min,p99_max,p999_mean,p999_stddev,p999_min,p999_max,cas_retry_rate_mean,worker_empty_poll_ratio_mean";
}
NR > 1 && $2 == "sample" {
  n++;
  t = $8 + 0.0;
  p99 = $9 + 0.0;
  p999 = $10 + 0.0;
  cas = $11 + 0.0;
  empty = $12 + 0.0;

  sum_t += t; sumsq_t += t * t;
  sum_p99 += p99; sumsq_p99 += p99 * p99;
  sum_p999 += p999; sumsq_p999 += p999 * p999;
  sum_cas += cas;
  sum_empty += empty;

  if (n == 1 || t < min_t) min_t = t;
  if (n == 1 || t > max_t) max_t = t;
  if (n == 1 || p99 < min_p99) min_p99 = p99;
  if (n == 1 || p99 > max_p99) max_p99 = p99;
  if (n == 1 || p999 < min_p999) min_p999 = p999;
  if (n == 1 || p999 > max_p999) max_p999 = p999;
}
END {
  if (n == 0) {
    print 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0;
    exit 0;
  }

  mean_t = sum_t / n;
  mean_p99 = sum_p99 / n;
  mean_p999 = sum_p999 / n;

  var_t = (sumsq_t / n) - (mean_t * mean_t);
  var_p99 = (sumsq_p99 / n) - (mean_p99 * mean_p99);
  var_p999 = (sumsq_p999 / n) - (mean_p999 * mean_p999);

  if (var_t < 0) var_t = 0;
  if (var_p99 < 0) var_p99 = 0;
  if (var_p999 < 0) var_p999 = 0;

  std_t = sqrt(var_t);
  std_p99 = sqrt(var_p99);
  std_p999 = sqrt(var_p999);

  printf "%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.6f,%.6f\n", \
    n, mean_t, std_t, min_t, max_t,
    mean_p99, std_p99, min_p99, max_p99,
    mean_p999, std_p999, min_p999, max_p999,
    (sum_cas / n), (sum_empty / n);
}
' "$RAW_CSV" > "$SUMMARY_CSV"

echo "采样完成"
echo "- 明细文件: $RAW_CSV"
echo "- 汇总文件: $SUMMARY_CSV"
echo
cat "$SUMMARY_CSV"
