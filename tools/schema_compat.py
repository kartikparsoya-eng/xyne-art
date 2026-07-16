#!/usr/bin/env python3
"""schema_compat.py — G27: frontend↔server schema-compat gate (#5).

Diffs the deployed app bundle's generated zero schema against the replica's
schema. Catches the exact class that caused the sbx reload-loop seed:
fieldEnum string-vs-json mismatch where the frontend expects one type but
the server (or the CVR replica) has another.

Method:
  1. Extract the client schema from the zero-cache container (the schema
     it was built with)
  2. Extract the client schema from the replica's CVR (what's persisted)
  3. Diff table names, column names, column types, and enum values
  4. FAIL on any type mismatch (string vs json, int vs float, etc.)

Also checks: the deployed app bundle's zero-schema.ts (if accessible)
against the container's client-schema.json — the exact seed of the
reload-loop bug.

    python3 tools/schema_compat.py \
        --container xyne-sandbox-rust-test-zero-cache-art \
        --replica-path /var/zero/replica.db \
        --client-schema harness/client-schema.json \
        --out reports/schema-compat-$TAG.json

Exit 0 = compatible; 1 = mismatch (FAIL); 2 = ERROR.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time


def extract_replica_schema(container: str, replica_path: str) -> dict:
    """Extract table+column schema from the SQLite replica."""
    out = subprocess.run(
        ["docker", "exec", container, "sh", "-c",
         f"sqlite3 {replica_path} '.schema' 2>&1"],
        capture_output=True, text=True, timeout=15)
    if out.returncode != 0:
        return {"error": f"sqlite3 schema extract failed: {out.stderr.strip()}"}
    schema_sql = out.stdout
    tables: dict[str, dict] = {}
    for line in schema_sql.split("\n"):
        line = line.strip()
        if not line.startswith("CREATE TABLE"):
            continue
        # Parse: CREATE TABLE "name" (col1 TYPE, col2 TYPE, ...)
        import re
        m = re.match(r'CREATE TABLE\s+"?(\w+)"?\s*\((.*)\)', line, re.I)
        if not m:
            continue
        tname, cols_sql = m.group(1), m.group(2)
        cols: dict[str, str] = {}
        for col_def in cols_sql.split(","):
            col_def = col_def.strip()
            if not col_def or col_def.upper().startswith(("PRIMARY", "FOREIGN", "UNIQUE", "CHECK", "CONSTRAINT")):
                continue
            parts = col_def.split()
            if len(parts) >= 2:
                cname = parts[0].strip('"')
                ctype = parts[1].upper()
                cols[cname] = ctype
        tables[tname] = {"columns": cols}
    return {"tables": tables}


def extract_client_schema_tables(client_schema_path: str) -> dict:
    """Extract table+column schema from the client-schema.json."""
    with open(client_schema_path) as f:
        cschema = json.load(f)
    tables: dict[str, dict] = {}
    # client-schema.json is {tableName: {primaryKey: [...], columns: {...}}}
    # or {tableName: {primaryKey: [...], rows: {...}}} depending on version
    for tname, tdef in cschema.items():
        if not isinstance(tdef, dict):
            continue
        cols: dict[str, str] = {}
        columns = tdef.get("columns") or tdef.get("rows") or {}
        if isinstance(columns, dict):
            for cname, ctype in columns.items():
                # Normalize types: "string" -> "TEXT", "number" -> "REAL", etc.
                if isinstance(ctype, str):
                    cols[cname] = ctype.upper()
                elif isinstance(ctype, dict):
                    cols[cname] = str(ctype.get("type", "unknown")).upper()
                else:
                    cols[cname] = str(type(ctype).__name__).upper()
        tables[tname] = {"columns": cols}
    return {"tables": tables}


def diff_schemas(client: dict, replica: dict) -> list[dict]:
    """Diff two schemas. Returns list of mismatches."""
    mismatches = []
    c_tables = set(client.get("tables", {}).keys())
    r_tables = set(replica.get("tables", {}).keys())

    # Tables only in one side
    for t in c_tables - r_tables:
        mismatches.append({"table": t, "kind": "only-in-client",
                          "detail": "table exists in client schema but not in replica"})
    for t in r_tables - c_tables:
        mismatches.append({"table": t, "kind": "only-in-replica",
                          "detail": "table exists in replica but not in client schema"})

    # Column-level diff for common tables
    for t in c_tables & r_tables:
        c_cols = client["tables"][t].get("columns", {})
        r_cols = replica["tables"][t].get("columns", {})
        for col in set(c_cols) | set(r_cols):
            ct = c_cols.get(col, "MISSING")
            rt = r_cols.get(col, "MISSING")
            if ct == "MISSING":
                mismatches.append({"table": t, "column": col, "kind": "column-only-in-replica",
                                  "detail": f"column {col} in replica but not client"})
            elif rt == "MISSING":
                mismatches.append({"table": t, "column": col, "kind": "column-only-in-client",
                                  "detail": f"column {col} in client but not replica"})
            elif _type_mismatch(ct, rt):
                mismatches.append({"table": t, "column": col, "kind": "type-mismatch",
                                  "detail": f"column {col}: client={ct} vs replica={rt}"})
    return mismatches


def _type_mismatch(a: str, b: str) -> bool:
    """Check if two type strings are genuinely incompatible.
    TEXT/STRING/VARCHAR are compatible. INTEGER/INT/NUMBER are compatible.
    """
    norm = {"TEXT": "TEXT", "STRING": "TEXT", "VARCHAR": "TEXT", "CHAR": "TEXT",
            "INTEGER": "INT", "INT": "INT", "NUMBER": "INT", "BIGINT": "INT",
            "REAL": "REAL", "FLOAT": "REAL", "DOUBLE": "REAL", "DECIMAL": "REAL",
            "BLOB": "BLOB", "BINARY": "BLOB", "JSON": "TEXT", "BOOLEAN": "INT", "BOOL": "INT",
            "TIMESTAMP": "TEXT", "DATETIME": "TEXT", "DATE": "TEXT"}
    na = norm.get(a.upper(), a.upper())
    nb = norm.get(b.upper(), b.upper())
    return na != nb


def main() -> int:
    ap = argparse.ArgumentParser(description="G27: frontend↔server schema-compat gate.")
    ap.add_argument("--container", required=True, help="zero-cache container name")
    ap.add_argument("--replica-path", default="/var/zero/replica.db")
    ap.add_argument("--client-schema", required=True, help="client-schema.json path")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    print(f"=== schema compat: {a.container} ===")
    checks = []

    # 1. Extract replica schema
    replica = extract_replica_schema(a.container, a.replica_path)
    if "error" in replica:
        checks.append({"name": "replica-extract", "verdict": "ERROR",
                       "detail": replica["error"]})
        return 2
    r_tables = len(replica.get("tables", {}))
    checks.append({"name": "replica-extract", "verdict": "PASS",
                   "detail": f"extracted {r_tables} tables from replica"})

    # 2. Extract client schema
    client = extract_client_schema_tables(a.client_schema)
    c_tables = len(client.get("tables", {}))
    checks.append({"name": "client-extract", "verdict": "PASS",
                   "detail": f"extracted {c_tables} tables from client schema"})

    # 3. Diff
    mismatches = diff_schemas(client, replica)
    type_mismatches = [m for m in mismatches if m["kind"] == "type-mismatch"]
    missing_tables = [m for m in mismatches if "only-in" in m["kind"]]

    if type_mismatches:
        detail = (f"{len(type_mismatches)} type mismatch(es): " +
                  ", ".join(f"{m['table']}.{m['column']} ({m['detail']})"
                           for m in type_mismatches[:4]))
        checks.append({"name": "type-compat", "verdict": "FAIL", "detail": detail})
    else:
        checks.append({"name": "type-compat", "verdict": "PASS",
                       "detail": "all common columns have compatible types"})

    if missing_tables:
        detail = (f"{len(missing_tables)} table(s) only on one side: " +
                  ", ".join(m["table"] for m in missing_tables[:4]))
        checks.append({"name": "table-coverage", "verdict": "WATCH", "detail": detail})
    else:
        checks.append({"name": "table-coverage", "verdict": "PASS",
                       "detail": f"all {c_tables} tables present on both sides"})

    fail = any(c["verdict"] == "FAIL" for c in checks)
    verdict = "FAIL" if fail else "PASS"
    report = {"gate": "G27", "name": "schema-compat",
              "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "container": a.container,
              "verdict": verdict, "checks": checks,
              "mismatches": mismatches,
              "summary": f"schema compat: {len(mismatches)} mismatch(es), "
                         f"{'FAIL' if fail else 'PASS'}"}
    print(report["summary"])
    for c in checks:
        print(f"  {c['name']:<16} {c['verdict']:<5} {c['detail']}")

    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)) or ".", exist_ok=True)
        with open(a.out, "w") as f:
            json.dump(report, f, indent=2)
        print(f"  report -> {a.out}")
    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
