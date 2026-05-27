# YAKt Modernization Plan — Correctness, Backend & SRE Showcase

## Context

`YAKt-Yet-Another_KRaft/` is a university Big Data project: a from-scratch Raft consensus
implementation (Python + ZeroMQ PUB/SUB) backing a Kafka-style metadata store exposed over
Flask. The goal of this plan is two-fold and in this order:

1. **Make it correct.** The core Raft logic works for leader election but has real
   distributed-systems bugs and gaps (commit-index calculation, dynamic membership, zero
   persistence). A resume claim is only credible if the internals actually hold up.
2. **Wrap it in a credible backend/SRE shell.** There is currently *no* packaging, tests,
   CI, containerization, observability, or git hygiene. Adding these turns a course project
   into a portfolio piece that demonstrably showcases distributed systems, backend, and SRE
   skills.

Scope is deliberately **high-impact but realistic** (achievable over ~2–4 weekends). Each
phase below maps to a feature branch, a PR into `master`, and a defensible resume bullet.
This file describes *what* to do and *how*, in steps — no code.

### Compliance check against the assignment spec (summary)

The HackMD spec requires: Raft leader election, log replication, failover guarantees,
event log, **snapshotting (creation + retrieval, periodic at leader)**, the 5 metadata
record types, CRD HTTP APIs, broker-mgmt + client-mgmt offset/diff endpoints (full
snapshot if offset > 10 min stale).

| Requirement | Status today |
|---|---|
| Leader election | Implemented, works |
| Log replication | Implemented but buggy (commit-index shadowing) |
| Failover (N-node tolerates floor failures) | Partially — breaks after dynamic membership (quorum size never updated) |
| Event log to reconstruct store | Present in-memory; lost on restart (no persistence) |
| **Snapshotting (periodic, retrievable)** | **NOT implemented — spec gap** |
| 5 record types + CRD APIs | Implemented |
| broker-mgmt / client-mgmt diff + 10-min snapshot rule | Implemented but index-alignment bugs |

**Bottom line:** mostly compliant on APIs, **non-compliant on snapshotting**, and the
replication/membership/persistence layer is fragile. Phases 1–3 close the spec gaps and
fix correctness; Phases 4–7 build the showcase.

---

## Known issues to fix (verified against source)

- `raft/raft.py:509` `_load_config(self, config)` is missing the `name` param but is called
  as `self._load_config(config, name)` (line 45) — crashes on file-based config. The correct
  signature lives only in the unused `modified_raft.py`.
- `_broadcast_commmit_entries` (~line 679, both files): loop var `index` from
  `enumerate(self.match_index)` shadows the parameter `index`; `if (index >= index)` is
  always true → commit broadcast ignores per-node match state.
- Dynamic membership (`/api/new_node`, `/api/remove_node` in `flask_http_server.py:65,99`)
  never updates `current_num_nodes` on running nodes → stale quorum after join/leave.
- No persistence anywhere: `current_term`, `voted_for`, `log`, `metadata_store` are
  in-memory only. Raft safety requires these be durable before responding.
- Entire `metadata_store` dict is replicated as one log entry per mutation (not deltas).
- `flask_http_server.py` blocks every mutating route on `time.sleep(5)` and runs
  `app.run(debug=True)` with a hardcoded LAN IP `192.168.136.128`.
- `flask_http_server.py:354` `node_records()` raises `UnboundLocalError` when no node matches.
- No `.gitignore`; `__pycache__/*.pyc` and `app.log` are committed; 3 commits total with
  messages like "my own repo".
- `raft/start.py` is dead scaffolding unrelated to the real implementation.
- Two near-duplicate 37KB files (`raft.py` vs `modified_raft.py`); only `raft.py` is imported.

---

## Git workflow for the whole plan

- Branch off `master` per phase: `git checkout -b <branch>`.
- Use **conventional commits** (`feat:`, `fix:`, `test:`, `ci:`, `docs:`, `refactor:`,
  `chore:`). Keep commits small and logically scoped within a branch.
- Open a PR per phase, self-review, then **squash-merge** to `master` so history reads as
  one clean change per capability. Tag milestones (`v0.1` after Phase 1, etc.) if desired.
- Each phase section below lists its branch name, the ordered commits, and the resume bullet
  it unlocks.

---

## Phase 0 — Repo hygiene & baseline (branch: `chore/repo-hygiene`)

Goal: a clean, reproducible starting point before touching logic.

1. Add a `.gitignore` (Python template: `__pycache__/`, `*.pyc`, `*.log`, `.venv/`,
   `.pytest_cache/`, `.ruff_cache/`, `*.egg-info/`, `.coverage`, `htmlcov/`).
2. `git rm -r --cached raft/__pycache__ app.log` to stop tracking generated artifacts
   (keep `app.log` locally if useful, just untrack it).
3. Pin dependencies: create `requirements.txt` (flask, pyzmq, requests) and a
   `requirements-dev.txt` (pytest, ruff, mypy, pytest-cov). Remove the `future`/`past`
   Python-2 compat shims from `raft.py`/`protocol.py` — the code targets 3.10.
4. Add `pyproject.toml` with `[tool.ruff]`, `[tool.mypy]`, and project metadata so the repo
   is installable (`pip install -e .`). Fix the tab/space mix in `protocol.py`.
5. Delete dead code: remove `raft/start.py` and `raft/modified_raft.py` *after* porting the
   two good fixes from `modified_raft.py` (the `_load_config` signature and extra logging)
   into `raft.py` — see Phase 1. Remove empty `server/server.py`.

Commits: `chore: add gitignore and stop tracking build artifacts` →
`chore: pin runtime and dev dependencies` → `build: add pyproject with ruff/mypy config` →
`chore: drop python2 compat shims and dead scaffolding`.

Resume value: shows you set up reproducible, lint-clean Python projects.

---

## Phase 1 — Raft correctness fixes (branch: `fix/raft-consensus-correctness`)

Goal: the consensus core is actually safe and matches the Raft paper.

1. **Fix `_load_config`** in `raft/raft.py`: adopt the correct `(self, config, name)`
   signature from `modified_raft.py` and the file/dict handling, so both dict and
   JSON-path configs work.
2. **Fix the commit-index shadowing bug** in `_broadcast_commmit_entries`: rename the loop
   variable so it no longer collides with the `index` parameter; the per-node committal must
   compare each node's `match_index` against the computed committable index. Re-derive the
   committable index as the highest log index replicated on a majority (standard Raft
   `matchIndex` quorum rule) and only advance commit for entries from the current term.
3. **Tighten voting safety**: before granting a vote, require the candidate's log to be at
   least as up-to-date (lastLogTerm/lastLogIndex check), not just `term > current_term`.
4. **Guard shared state**: extend `client_lock` (or add a dedicated lock) to cover
   `current_term`, `voted_for`, `leader_id`, `next_index`, `match_index`, since Flask threads
   call into the node concurrently with the Raft loop.
5. **Add defensive parsing**: wrap `parse_json_message`/message-loop body so a malformed
   message logs and is dropped instead of killing the node thread.

Commits: `fix: correct _load_config signature for file and dict configs` →
`fix: resolve commit-index variable shadowing in commit broadcast` →
`fix: enforce log-up-to-date check before granting vote` →
`fix: guard volatile raft state with locks` →
`fix: drop malformed messages instead of crashing node loop`.

Resume value: "Debugged and fixed safety violations in a Raft implementation (commit-index
quorum, vote up-to-dateness, data races)." This is the strongest distributed-systems bullet.

---

## Phase 2 — Persistence & log compaction / snapshotting (branch: `feat/persistence-and-snapshots`)

Goal: close the spec's snapshotting gap and give real crash recovery.

1. **Durable Raft state**: persist `current_term`, `voted_for`, and the `log` to disk before
   responding to RPCs. Simplest credible approach: an append-only WAL file per node (JSON
   lines) plus a small `state.json` for term/vote, fsync'd on write. Load on startup to
   reconstruct state. Use a per-node data directory keyed by node id.
2. **Snapshotting** (spec requirement): at the leader, periodically (configurable interval)
   serialize the committed `metadata_store` as a snapshot file with the last-included index
   and term; truncate the in-memory/WAL log up to that point. Implement *retrieval*: a method
   to load the latest snapshot on startup and a path for a lagging follower to be caught up
   via snapshot when its needed index is below the snapshot's first index.
3. **Wire snapshot into MetadataFetch / broker-mgmt**: when a broker's offset is older than
   the snapshot (the spec's "10 minutes / too far behind" rule), return the snapshot instead
   of a diff. Reuse the existing offset logic in the broker-mgmt/client-mgmt routes.
4. **Switch replication to deltas**: replicate individual record operations as log entries
   instead of the whole `metadata_store` blob, so the log/snapshot sizes are meaningful.

Commits: `feat: persist term, vote, and log to a write-ahead log` →
`feat: add periodic leader snapshots with last-included index/term` →
`feat: serve snapshots to lagging followers and stale broker offsets` →
`refactor: replicate per-record deltas instead of full store blob`.

Resume value: "Implemented WAL persistence and log-compaction snapshots for crash recovery
in a Raft cluster." Directly satisfies the previously-missing spec requirement.

---

## Phase 3 — Dynamic membership & API hardening (branch: `feat/membership-and-api-hardening`)

Goal: failover guarantees survive cluster changes; the HTTP layer is production-shaped.

1. **Single-server membership changes**: when `/api/new_node` or `/api/remove_node` runs,
   propagate the new peer set / quorum size to all running nodes (add a Raft config-change
   log entry or, minimally, an explicit "update_peers" call on each node) so
   `current_num_nodes` and address books stay consistent. Document that this is a simplified
   single-at-a-time change (not joint consensus).
2. **Remove blocking `time.sleep(5)`**: replace with an event/condition that fires when the
   client request reaches `commit_index`, with a timeout. Return proper HTTP status
   (202/200/504) instead of always assuming success.
3. **Fix `node_records()` `UnboundLocalError`**: return 404 when no node matches.
4. **Config not constants**: move `ip_addr`, ports, election/heartbeat timings, snapshot
   interval, and data dir into env vars / a `config.yaml` (with sane localhost defaults).
   Drop `debug=True`; document running under a real WSGI server (gunicorn) in Phase 5.
5. **Input validation**: validate request bodies for each record-type route; return 400 on
   bad input rather than raising.

Commits: `feat: propagate membership changes to running nodes` →
`refactor: replace blocking sleep with commit-wait and real status codes` →
`fix: return 404 from node_records on unknown node` →
`feat: externalize configuration via env and config file` →
`feat: validate request payloads on record endpoints`.

Resume value: "Hardened a stateful HTTP service: dynamic cluster membership, request
validation, non-blocking commit semantics, externalized config."

---

## Phase 4 — Tests (branch: `test/raft-and-api-coverage`)

Goal: prove correctness; this is what makes the Phase 1–3 claims believable.

1. Add `pytest` + `pytest-cov`. Create a `tests/` package.
2. **Unit tests** for protocol (`jsonify`/`un_jsonify` round-trips), log up-to-dateness
   comparison, commit-index quorum calculation, and WAL load/replay.
3. **Integration tests** spinning up a 3- and 5-node cluster in-process: assert a single
   leader is elected, a committed entry appears on a majority, and the cluster tolerates
   floor((N-1)/2) failures (the spec's failover guarantee). Reuse/replace the existing
   `test_failures()` smoke logic, turning it into asserted tests.
4. **Persistence test**: kill and restart a node, assert it recovers term/vote/log and
   catches up via snapshot.
5. **API tests** with Flask's test client for each endpoint (happy path + validation errors).

Commits: `test: add protocol and commit-index unit tests` →
`test: add multi-node election and replication integration tests` →
`test: cover crash recovery and snapshot catch-up` →
`test: cover HTTP API happy paths and validation`.

Resume value: "Wrote unit + multi-node integration tests for a Raft cluster including
failover and crash-recovery scenarios."

---

## Phase 5 — Containerization & local cluster (branch: `feat/docker-and-compose`)

Goal: anyone can run a real multi-node cluster in one command.

1. Add a `Dockerfile` (slim Python base, non-root user, install from `requirements.txt`,
   run the Flask app under **gunicorn**, expose the port).
2. Add `docker-compose.yml` that brings up a 3-node (and a 5-node profile) cluster — each
   node its own container with its own data volume and config via env — plus the coordinator.
   This replaces the hardcoded single-IP demo with a genuine distributed deployment.
3. Add a `Makefile` with `make up`, `make down`, `make test`, `make lint`, `make demo`
   (runs the `client/` scripts against the compose cluster).
4. Refactor `client/` scripts to read the target URL from an env var / CLI arg instead of
   hardcoded IPs, and collapse the eight near-identical scripts into one parametrized client.

Commits: `feat: add Dockerfile running under gunicorn` →
`feat: add docker-compose for 3- and 5-node clusters` →
`build: add Makefile for up/down/test/lint/demo` →
`refactor: parametrize client scripts via env/CLI`.

Resume value: "Containerized a distributed system; reproducible N-node cluster via
docker-compose with per-node persistence."

---

## Phase 6 — Observability (branch: `feat/observability`)

Goal: SRE story — you can see what the cluster is doing.

1. **Structured logging**: replace `print()` in the Raft core with the `logging` module,
   JSON-formatted, including node id, term, role, and event (election_started, became_leader,
   vote_granted, entry_committed). Configurable level via env.
2. **Health/readiness/status endpoints**: `/health` (process up), `/ready` (node has a known
   leader), and `/status` (this node's role, term, commit index, log length, current leader).
3. **Prometheus metrics** via `prometheus_client` at `/metrics`: counters/gauges for
   elections, term, role, commit index, log size, replication lag per follower, request
   latency, and snapshot count. Keep it lightweight — no Grafana required (mention it as a
   stretch).

Commits: `feat: structured JSON logging for raft state transitions` →
`feat: add health, readiness, and status endpoints` →
`feat: expose prometheus metrics for cluster state`.

Resume value: "Instrumented a distributed system with structured logging, health/readiness
probes, and Prometheus metrics (leader, term, commit lag)."

---

## Phase 7 — CI, load test & docs (branch: `ci/pipeline-and-docs`)

Goal: automation + a benchmark number + a real README.

1. **GitHub Actions**: a `ci.yml` that on push/PR runs ruff, mypy, and `pytest` (with the
   multi-node integration tests) and uploads coverage. Add a build job that builds the
   Docker image. Add status badges to the README.
2. **Load/throughput test**: a small script (e.g. `locust` or a simple async client) that
   drives the broker/topic/partition APIs against the compose cluster and records
   throughput/latency. Capture a before/after number (e.g. after removing `time.sleep(5)`)
   for the README — a concrete metric is gold on a resume.
3. **Rewrite README**: architecture diagram, the consensus guarantees you implemented, how
   to run the cluster (compose), how to run tests, the observability endpoints, the
   benchmark results, and a clear "what's a simplification vs. the real Raft paper" section
   (honesty about joint consensus, etc.).
4. **Optional stretch (note only, not required for the resume claim)**: Kubernetes manifests
   / Helm chart, Grafana dashboard JSON, chaos test (kill random node under load) — list as
   "future work" so the scope stays realistic.

Commits: `ci: add lint, type-check, test, and docker-build pipeline` →
`test: add load/throughput benchmark harness` →
`docs: rewrite README with architecture, run guide, and benchmarks`.

Resume value: "Built CI (lint/type/test/build) and a load-test harness; documented
architecture and measured throughput improvement after removing blocking commits."

---

## Suggested resume bullets (after the plan is executed)

- Built and **debugged a Raft consensus system** (leader election, log replication,
  snapshotting) in Python over ZeroMQ; fixed commit-index quorum and vote-safety bugs and
  added WAL persistence for crash recovery.
- **Containerized a 3/5-node distributed cluster** with docker-compose and per-node
  persistence; replaced blocking request handling, improving write throughput by ~Nx
  (measured via a load-test harness).
- Added **SRE tooling**: structured JSON logging, health/readiness probes, and Prometheus
  metrics (leader, term, commit lag); wired a **GitHub Actions CI** running lint, type
  checks, and multi-node integration/failover tests.

---

## Verification (end to end)

After each phase, locally: `make lint && make test`. After Phase 5+: `make up` to launch the
compose cluster, run `make demo` (the client scripts), then:

- Confirm one leader via `/status` on each node; kill the leader container and confirm a new
  leader is elected (failover).
- Register a broker/topic/partition; restart a node and confirm it recovers state from
  WAL/snapshot (`/status` log length matches).
- Hit `/metrics` and confirm election/commit gauges move; scrape during a `make demo` load
  run for the benchmark number.
- Push the branch and confirm GitHub Actions CI passes (lint + mypy + pytest, including the
  multi-node failover test) before squash-merging the PR to `master`.
