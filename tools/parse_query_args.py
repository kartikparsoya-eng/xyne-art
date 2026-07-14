import sys
import json
from collections import defaultdict

schema = defaultdict(lambda: {"keys": set(), "sample": None, "n": 0})
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
    q = e.get("query", "?")
    a = e.get("args", {})
    s = schema[q]
    s["n"] += 1
    if isinstance(a, dict):
        s["keys"].update(a.keys())
        if s["sample"] is None and a:
            s["sample"] = a

total = sum(v["n"] for v in schema.values())
rows = sorted(schema.items(), key=lambda kv: -kv[1]["n"])
print("parsed %d events across %d distinct queries\n" % (total, len(schema)))
for q, v in rows[:45]:
    keys = ", ".join(sorted(v["keys"])) or "(no args)"
    print("%-42s n=%-6d args: %s" % (q, v["n"], keys))
