import sys
import json

schema = {}
counts = {}
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        outer = json.loads(line)
    except Exception:
        continue
    msg = outer.get("_msg")
    if not msg:
        continue
    try:
        e = json.loads(msg)
    except Exception:
        continue
    if e.get("event") != "zero_query_complete":
        continue
    q = e.get("query")
    if not q:
        continue
    a = e.get("args", {})
    counts[q] = counts.get(q, 0) + 1
    keys = set(schema.get(q, []))
    if isinstance(a, dict):
        keys.update(a.keys())
    schema[q] = sorted(keys)

out_path = sys.argv[1] if len(sys.argv) > 1 else "/dev/stdout"
with open(out_path, "w") as f:
    json.dump({"schema": schema, "sample_counts": counts}, f, indent=2, sort_keys=True)
sys.stderr.write("captured arg schemas for %d queries from %d sampled events\n"
                 % (len(schema), sum(counts.values())))
