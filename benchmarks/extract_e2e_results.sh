#!/bin/bash
# 脚本用途: 从 e2e_benchmark 的 JSON 结果文件生成 CSV 摘要表格
# 使用方法: ./extract_e2e_results.sh <json_directory>

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <json_directory>"
    echo "Example: $0 ./benchmarks/results/e2e_scaling_20260620"
    exit 1
fi

JSONDIR="$1"

if [[ ! -d "$JSONDIR" ]]; then
    echo "Error: Directory '$JSONDIR' not found"
    exit 1
fi

# 输出 CSV 表头
echo "test_name,duration_ms,throughput_tps,p50_us,p99_us,p999_us,connection_success,connection_failed,send_failed,enqueue_success,enqueue_timeout,tp_executed,executor_executed,completed_frames,frame_decode_error,dropped_on_backpressure"

# 处理每个 JSON 文件
for jsonfile in "$JSONDIR"/*.json; do
    if [[ ! -f "$jsonfile" ]]; then
        continue
    fi
    
    basename_json=$(basename "$jsonfile" .json)
    
    # 使用 jq 抽取字段
    jq -r --arg name "$basename_json" '
        [
          $name,
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
        ] | @csv' "$jsonfile"
done
