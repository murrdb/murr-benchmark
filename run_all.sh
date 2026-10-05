#!/usr/bin/env bash
# Run every backend's Criterion bench against one workload config.
# Usage: ./run_synthetic.sh [--fast] <workload config>
#   --fast  run each bench with Criterion's --profile-time 3 (for testing)
# Env: BENCHES="<name> ..."  restrict the run to these bench targets
#      REPORT_DIR=<dir>      where JSON reports are written (default: reports)
set -euo pipefail

USAGE="Usage: $0 [--fast] <workload config>"
WORKLOAD=""
criterion_args=()
for arg in "$@"; do
    case "$arg" in
        --fast)
            criterion_args=(--profile-time 3)
            ;;
        -*)
            echo "unknown option: $arg" >&2; echo "$USAGE" >&2; exit 1
            ;;
        *)
            [ -z "$WORKLOAD" ] || { echo "$USAGE" >&2; exit 1; }
            WORKLOAD="$arg"
            ;;
    esac
done
[ -n "$WORKLOAD" ] || { echo "$USAGE" >&2; exit 1; }
[ -f "$WORKLOAD" ] || { echo "workload config not found: $WORKLOAD" >&2; exit 1; }

# Benches resolve configs/ relative to the repo root; keep the workload path valid after the cd.
WORKLOAD="$(realpath "$WORKLOAD")"
cd "$(dirname "$0")"

# Bench targets are the files in benches/.
if [ -n "${BENCHES:-}" ]; then
    read -r -a benches <<< "$BENCHES"
else
    benches=()
    for file in benches/*.rs; do
        benches+=("$(basename "$file" .rs)")
    done
fi

# Build everything up front so a compile error fails before any bench runs.
cargo bench --no-run >&2

failed=()
for bench in "${benches[@]}"; do
    echo "=== $bench ($WORKLOAD) ===" >&2
    # A failing backend must not stop the remaining ones.
    if ! WORKLOAD="$WORKLOAD" cargo bench --bench "$bench" -- "${criterion_args[@]}"; then
        echo "=== $bench FAILED ===" >&2
        failed+=("$bench")
    fi
done

echo "ran ${#benches[@]} benches, ${#failed[@]} failed; reports in ${REPORT_DIR:-reports}/" >&2
if [ "${#failed[@]}" -gt 0 ]; then
    echo "failed: ${failed[*]}" >&2
    exit 1
fi
