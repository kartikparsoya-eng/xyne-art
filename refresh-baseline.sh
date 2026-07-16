#!/usr/bin/env bash
# Reproducible ART pipeline: pull 7d PROD telemetry into raw/, then rebuild art-baseline.json.
# Requires: GR_KEY exported (Grafana service-account token) + Juspay VPN.
#
#   export GR_KEY='glsa_...'
#   ./refresh-baseline.sh
#
# Windows (see ART.md §1): counts/weights = 7d, client latency = 72h (7d quantile overloads the
# log backend), server histograms = 7d increase. Everything lands in raw/, then build_baseline.py
# assembles art-baseline.json.
#
# Weekly archive: because log retention is only ~8 days, each run archives raw/ into
# raw/archive-YYYYMMDD/ before overwriting. After a month you have 4 weekly snapshots;
# their union is your "1-month mine" — for free — and drift tracking (whale growth,
# new shapes after app releases) comes along for the ride.
set -euo pipefail

: "${GR_KEY:?export GR_KEY first (Grafana service-account token, on VPN)}"
BASE="https://grafana.spaces.xyne.juspay.net"
LOGS="$BASE/api/datasources/proxy/8/select/logsql/query"
PROM="$BASE/api/datasources/proxy/7/api/v1/query"
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"                       # inline python below uses raw/ relative paths
RAW="$DIR/raw"; mkdir -p "$RAW"

echo "# access check"
code=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $GR_KEY" "$BASE/api/user")
[ "$code" = "200" ] || { echo "Grafana returned $code (VPN? rotated key?)"; exit 1; }
echo "ok ($code)"

# --- 0) archive previous raw/ (retention is 8d — archive is the only long-term memory) ---
ARCH_TAG="$(date +%Y%m%d)"
ARCH_DIR="$RAW/archive-$ARCH_TAG"
if [ -d "$RAW" ] && [ "$(ls -A "$RAW" 2>/dev/null)" ]; then
  mkdir -p "$ARCH_DIR"
  cp -p "$RAW"/*.ndjson "$RAW"/*.json "$RAW"/*.txt "$ARCH_DIR/" 2>/dev/null || true
  echo "  archived previous raw/ -> $ARCH_DIR"
else
  echo "  (no previous raw/ to archive)"
fi

logs () { # $1=LogsQL  $2=start  $3=limit  $4=outfile
  curl -s -G -H "Authorization: Bearer $GR_KEY" "$LOGS" \
    --data-urlencode "query=$1" --data-urlencode "start=$2" --data-urlencode "limit=$3" -o "$4"
}

echo "# 1/8 query counts (7d, full breadth)"
logs 'container:"xyne-logging-bridge" AND event:"zero_query_complete" | stats by (query) count() as calls | sort by (calls desc)' 168h 500 "$RAW/queries_7d_counts.ndjson"

echo "# 2/8 query latency percentiles (72h — 7d quantile overloads the backend)"
logs 'container:"xyne-logging-bridge" AND event:"zero_query_complete" | stats by (query) count() as calls, quantile(0.50, latency) as p50, quantile(0.95, latency) as p95, quantile(0.99, latency) as p99, max(latency) as max | sort by (calls desc)' 72h 500 "$RAW/queries_72h_stats.ndjson"

echo "# 3/8 mutation mix (7d, full breadth + percentiles)"
logs 'container:"xyne-logging-bridge" AND event:"zero_mutation_complete" | stats by (mutation) count() as calls, quantile(0.50, duration) as p50, quantile(0.95, duration) as p95, quantile(0.99, duration) as p99, max(duration) as max | sort by (calls desc)' 168h 500 "$RAW/mutations_7d.ndjson"

echo "# 4/8 one-shot mix (7d)"
logs 'container:"xyne-logging-bridge" AND event:"zero_run_complete" | stats by (query) count() as calls, quantile(0.50, latency) as p50, quantile(0.95, latency) as p95, quantile(0.99, latency) as p99 | sort by (calls desc)' 168h 500 "$RAW/oneshot_7d.ndjson"

echo "# 5/8 event volume (7d, health gates)"
logs 'container:"xyne-logging-bridge" AND event:* | stats by (event) count() as n | sort by (n desc)' 168h 300 "$RAW/events_7d.ndjson"

echo "# 6/8 platform split (7d)"
logs 'container:"xyne-logging-bridge" AND event:"zero_query_complete" | stats by (platformName) count_uniq(emailId) as users, count_uniq(zeroClientGroupId) as cgids, count() as events | sort by (events desc)' 168h 20 "$RAW/platform_7d.ndjson"

echo "# 7/8 server engine histograms (7d increase; ivm_advance = 1h rate fallback)"
{
  echo "{"
  first=1
  for m in zero_sync_hydration_time_seconds zero_sync_advance_time_seconds zero_sync_ivm_advance_time_seconds \
           zero_sync_poke_time_seconds zero_sync_cvr_flush_time_seconds zero_sync_query_transformation_time_seconds; do
    [ $first -eq 0 ] && echo ","; first=0
    win="increase(${m}_bucket[7d])"
    [ "$m" = "zero_sync_ivm_advance_time_seconds" ] && win="rate(${m}_bucket[1h])"
    p50=$(curl -s -G -H "Authorization: Bearer $GR_KEY" "$PROM" --data-urlencode "query=histogram_quantile(0.50, sum($win) by (le))" | sed -n 's/.*"value":\[[0-9.]*,"\([^"]*\)"\].*/\1/p')
    p95=$(curl -s -G -H "Authorization: Bearer $GR_KEY" "$PROM" --data-urlencode "query=histogram_quantile(0.95, sum($win) by (le))" | sed -n 's/.*"value":\[[0-9.]*,"\([^"]*\)"\].*/\1/p')
    p99=$(curl -s -G -H "Authorization: Bearer $GR_KEY" "$PROM" --data-urlencode "query=histogram_quantile(0.99, sum($win) by (le))" | sed -n 's/.*"value":\[[0-9.]*,"\([^"]*\)"\].*/\1/p')
    printf '  "%s": {"p50_s": %s, "p95_s": %s, "p99_s": %s}' "$m" "${p50:-null}" "${p95:-null}" "${p99:-null}"
  done
  echo ""; echo "}"
} > "$RAW/server_hist_7d.json"

echo "# 7b backend log levels (7d) -> backend_levels_7d.json"
python3 - "$LOGS" > "$RAW/backend_levels_7d.json" <<'PY'
import os, sys, json, urllib.parse, urllib.request
key, logs = os.environ["GR_KEY"], sys.argv[1]
q = 'container:"xyne-backend" | stats by (level) count() as n | sort by (n desc) | limit 10'
data = urllib.parse.urlencode({"query": q, "start": "168h", "limit": "10"}).encode()
req = urllib.request.Request(logs, data=data, headers={"Authorization": "Bearer " + key})
out = {"info": 0, "warn": 0, "error": 0}
for line in urllib.request.urlopen(req).read().decode().splitlines():
    if not line.strip():
        continue
    r = json.loads(line)
    lvl = r.get("level")
    if lvl in out:
        out[lvl] = int(r["n"])
print(json.dumps(out))
PY

echo "# 8/8 arg schemas: sample + targeted lookups for the rare tail"
logs 'container:"xyne-logging-bridge" AND event:"zero_query_complete"' 6h 15000 "$RAW/qsample.ndjson"
python3 "$DIR/tools/extract_arg_schemas.py" "$RAW/arg_schemas.json" < "$RAW/qsample.ndjson"
python3 - > "$RAW/missing_queries.txt" <<'PY'
import json
counts = [json.loads(l)["query"] for l in open("raw/queries_7d_counts.ndjson") if l.strip()]
have = set(json.load(open("raw/arg_schemas.json"))["schema"].keys())
print("\n".join(q for q in counts if q not in have and q != "unknown"))
PY
: > "$RAW/targeted.ndjson"
while IFS= read -r q; do
  [ -z "$q" ] && continue
  curl -s -G -H "Authorization: Bearer $GR_KEY" "$LOGS" \
    --data-urlencode "query=container:\"xyne-logging-bridge\" AND event:\"zero_query_complete\" AND query:\"$q\"" \
    --data-urlencode "start=168h" --data-urlencode "limit=1" >> "$RAW/targeted.ndjson"
done < "$RAW/missing_queries.txt"
cat "$RAW/qsample.ndjson" "$RAW/targeted.ndjson" > "$RAW/qcombined.ndjson"
python3 "$DIR/tools/extract_arg_schemas.py" "$RAW/arg_schemas.json" < "$RAW/qcombined.ndjson"

echo "# 8b/9 whale rowCounts (24h max — for seeder calibration)"
logs 'container:"xyne-logging-bridge" AND event:"zero_query_complete" | stats by (query) max(rowCount) as max_rows | sort by (max_rows desc) | limit 20' 24h 20 "$RAW/whale_rowcounts_24h.ndjson"

echo "# 8c/9 storm sizing: worst 5-min concurrent-CG and connects/min (7d)"
# Prom queries for peak concurrent CGs and connect rate — used by G27 storm scaling
PEAK_CG=$(curl -s -G -H "Authorization: Bearer $GR_KEY" "$PROM" \
  --data-urlencode "query=max_over_time(count(count by (zeroClientGroupId) (zero_socket_connected))5m)[7d])" \
  | sed -n 's/.*"value":\[[0-9.]*,"\([^"]*\)"\].*/\1/p')
PEAK_CONNECTS=$(curl -s -G -H "Authorization: Bearer $GR_KEY" "$PROM" \
  --data-urlencode "query=max_over_time(rate(zero_socket_connected[5m])[7d:5m]) * 60" \
  | sed -n 's/.*"value":\[[0-9.]*,"\([^"]*\)"\].*/\1/p')
cat > "$RAW/storm_sizing.json" <<EOF
{
  "peak_concurrent_cgs_5min": ${PEAK_CG:-null},
  "peak_connects_per_min_5min": ${PEAK_CONNECTS:-null},
  "window": "7d",
  "note": "worst 5-min bucket; use for G27 storm scaling and --ceiling lane CG count"
}
EOF
echo "  peak concurrent CGs: ${PEAK_CG:-?}, peak connects/min: ${PEAK_CONNECTS:-?}"

echo "# assemble art-baseline.json"
python3 "$DIR/tools/build_baseline.py"
python3 -c "import json; json.load(open('$DIR/art-baseline.json')); print('art-baseline.json is valid JSON')"

echo "# 9/9 coverage diff: prod shapes vs replay corpus"
python3 - "$DIR" <<'PY'
import json, os, sys
dir = sys.argv[1]
bl = json.load(open(os.path.join(dir, "art-baseline.json")))
prod_shapes = {q["name"] for q in bl["query_workload"]["queries"]}
# The replay corpus is the baseline itself (replay.py loads from art-baseline.json).
# Check the run report from the last ART run for actually-hydrated shapes.
import glob
runs = sorted(glob.glob(os.path.join(dir, "reports/run-*.json")))
hydrated = set()
if runs:
    try:
        r = json.load(open(runs[-1]))
        hydrated = set(r.get("coverage", {}).get("queries_hydrated", 0) and
                       r.get("per_query", {}).keys())
    except Exception:
        pass
missing = prod_shapes - hydrated if hydrated else set()
report = {
    "prod_distinct_shapes": len(prod_shapes),
    "last_run_hydrated": len(hydrated),
    "missing_from_last_run": sorted(missing),
    "missing_count": len(missing),
}
with open(os.path.join(dir, "reports/coverage-diff.json"), "w") as f:
    json.dump(report, f, indent=2)
print(f"  prod shapes: {len(prod_shapes)}, last run hydrated: {len(hydrated)}", end="")
if missing:
    print(f", missing: {len(missing)} -> {', '.join(sorted(missing)[:5])}")
else:
    print()
PY

echo "done."
