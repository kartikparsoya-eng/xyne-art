#!/usr/bin/env bash
# validate-g8fix2.sh — stand up the G8-fix rust image as a fresh syncer, run the
# single-op diff oracle for myChannelParticipations against the TS 1.9 mirror,
# and assert 0 channels only_primary (the G8 fix landed).
set -euo pipefail

IMAGE="${IMAGE:-zero-cache-rust-syncer:g8fix2}"
NAME="${NAME:-xyne-sandbox-rust-test-zero-cache-g8fix2}"
NET="${NET:-sandbox-net}"
VOL="${VOL:-g8fix2_zero}"
U=cms5vksku005e11z9mqhq1y2u
JWT="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJjbXM1dmtza3UwMDVlMTF6OW1xaHExeTJ1IiwiZW1haWwiOiJzYW5kYm94QHh5bmUuYWkiLCJuYW1lIjoic2FuZGJveCIsImlhdCI6MTc4Nzc2NzUxMywiZXhwIjoxNzg3ODUzOTEzLCJpc3MiOiJ4eW5lIiwiYXVkIjoieHluZS11c2VyIiwibWVtYmVySWQiOiJjbXM1dmtza3AwMDVjMTF6OTYzenhtbXYyIiwid29ya3NwYWNlSWQiOiJjbXM1dmtzNWMwMDFmMTF6OXp1ZDN0eWZsIn0.NZ-5MnsRFfXkjjlbeCR-tf0AILM3DnuUCSi96PlcpKk"

echo "== (re)create g8fix2 syncer container from $IMAGE =="
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker volume rm -f "$VOL" >/dev/null 2>&1 || true
docker volume create "$VOL" >/dev/null
docker run -d --name "$NAME" --network "$NET" \
  --env-file /tmp/rust_env.txt -e ZERO_SYNCER=rust \
  -v "$VOL:/var/zero" "$IMAGE" >/dev/null

echo "== wait for WS + replica sync =="
for i in $(seq 1 45); do
  ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$NAME" 2>/dev/null || true)
  if [ -n "$ip" ] && nc -z -w2 "$ip" 4848 2>/dev/null; then echo "g8fix2 up at $ip"; break; fi
  sleep 4
done
IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$NAME")
TS_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' xyne-sandbox-rust-test-zero-cache-ts)
echo "primary(g8fix2)=$IP  mirror(ts19)=$TS_IP"
sleep 25   # let the fresh replica catch up from PG

echo "== single-op oracle: myChannelParticipations =="
DIFF_ORACLE_DUMP_TABLE=channels DIFF_ORACLE_MAX_EXAMPLES=50 python3 harness/diff_oracle.py \
  --primary "ws://$IP:4848" --mirror "ws://$TS_IP:4848" \
  --id-pool harness/id-pool.sandbox.json --client-schema harness/client-schema.json \
  --auth-token "$JWT" --extra-param userID=$U \
  --pairs 1 --catalog-batch-size 1 --full-catalog --only-ops myChannelParticipations \
  --duration 15 --quiesce-s 35 --out reports/diff-mcp-g8fix2.json 2>/tmp/g8fix2_oracle.txt || true

echo "== result =="
grep '\[dump\]' /tmp/g8fix2_oracle.txt || true
python3 -c "import json;d=json.load(open('reports/diff-mcp-g8fix2.json'));r=d['results'][0];pt=r['per_table'];print('verdict',d['verdict'],'per_table',pt);print('P rows',r['primary']['rows'],'errs',r['primary']['errors']);print('M rows',r['mirror']['rows'],'errs',r['mirror']['errors']);import sys;ch=pt.get('channels',{});sys.exit(0 if ch.get('only_primary',0)==0 else 1)" \
  && echo 'G8 FIX CONFIRMED: 0 channels only_primary' || echo 'G8 STILL DIVERGES (or transform failed — check errors above)'
