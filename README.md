# Xyne Spaces — Application Regression Test (ART)

A **production-derived baseline** of what the Xyne Spaces app actually does
and how fast it does it, so major changes (to `zero-cache`, the ZQL/IVM query
engine, the Zero schema, or the backend) can be validated against real user
behavior instead of guesswork.

> **What "ART" means here:** an Application Regression Test — a representative
> workload (the queries/mutations users really run, weighted by frequency, with
> real argument shapes) plus baseline SLOs and health gates. Run it before/after
> a change and fail the change if it regresses the baseline.

The full design, methodology, SLOs, and gate model (G1–G15) live in
**[`ART.md`](./ART.md)** — start there. This README is the quickstart.

## Quickstart

```bash
# 1. create a venv and install deps (only `websockets` is third-party)
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 2. (optional) dev/test deps: pytest + ruff
.venv/bin/pip install -r requirements-dev.txt

# 3. run the unit tests (no live server needed — pure model + wire-format)
.venv/bin/python -m pytest tests/

# 4. lint
.venv/bin/ruff check .
```

## Running against a live zero-cache

```bash
# live driver needs: a reachable zero-cache + valid auth + GR_KEY (on VPN) for the gate
export GR_KEY='glsa_...'
./run-art.sh --target wss://zero-canary.example/zero --auth-token "$JWT"
```

For the local sandbox (docker compose, no Grafana), the full harness lives in
[`run-art-local.sh`](./run-art-local.sh):

```bash
./run-art-local.sh --soak            # 1h leak hunt (resource sampler + slope gates G6)
./run-art-local.sh --oracle          # + G8 differential correctness oracle
./run-art-local.sh --chaos           # + G10 fault injection (docker-pause mid-run)
./run-art-local.sh --negative --users 2   # + G11 adversarial negative suite
```

## Layout

```
art-baseline.json   # canonical machine-readable baseline (generated; do not hand-edit)
ART.md              # full design doc, SLOs, and the G1–G15 gate model — read this first
harness/            # live drivers + oracles (replay, diff_oracle, matrix_oracle, ...)
├── workload.py     # pure stdlib workload model + wire-message builders (unit-testable)
├── protocol.py     # vendored Zero wire-protocol primitives (DEFAULT_PROTOCOL_VERSION, encode_sec_protocols)
├── replay.py       # Mode-A load driver
└── ...             # oracles + adversarial suites
tools/              # baseline builders, id-pool harvesters, gate evaluators, samplers
raw/                # production telemetry pulls (gitignored — may contain PII)
reports/            # run/gate/resource reports (gitignored — regenerated per rig)
tests/              # unit tests for the pure model + wire protocol
docs/               # incident capture + analysis
```

## Make targets

```bash
make venv      # create .venv and install runtime + dev deps
make test      # run the unit test suite
make lint      # ruff check
make check     # lint + test
make clean-reports   # remove generated reports/ (regenerated per rig)
```

## Notes

- `raw/`, `reports/`, `harness/id-pool*.json`, `harness/client-schema*.json`, and
  `harness/auth-pool.json` are **gitignored** — they hold real data/PII or are
  environment-specific. See [`.gitignore`](./.gitignore).
- Mutations are **OFF by default** (they write real data); enabling requires both
  `--enable-mutations` and `--i-know-this-writes`. Disposable/staging envs only.
