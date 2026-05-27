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

This is being built as a **flagship portfolio project** — the full scope, including the
previously-optional stretch work (Kubernetes/Helm, Prometheus+Grafana, chaos testing, gRPC
transport), is now committed as real phases. Each phase maps to a feature branch, a PR into
`main`, and a defensible resume bullet. This file describes *what* to do and *how*, in steps
— no code.

---

## Portfolio framing — how this gets read

The audience is a hiring manager or interviewer skimming your GitHub for ~3 minutes, then (if
interested) digging into the README, the CI badge, and one or two source files. Optimize for
that reader:

- **The README is the product.** Most reviewers never run the code. A clear architecture
  diagram, a one-command `make up` demo, a failover GIF, and a benchmark number do more than
  any single feature. (Phases 7 + 11)
- **A green CI badge + passing multi-node failover tests** is the single biggest credibility
  signal — it separates this from the thousands of abandoned Raft toys. (Phases 4 + 7)
- **Honesty reads as senior.** A short "Simplifications vs. the Raft paper" section (single
  coordinator HTTP layer, simplified single-server membership, etc.) is a strength, not a
  weakness. Don't overclaim.
- **Provenance:** this started as a university course project. Keep the original 3 commits as
  the base so history is honest; all improvements land as your own phased PRs on top. Credit
  the original course project in the README and clearly mark what *you* added.
- **Pin it.** When done, pin the repo on your GitHub profile, give it a clear description and
  topics (`raft`, `distributed-systems`, `kafka`, `kubernetes`, `sre`), and link it from your
  resume next to the bullets in the "Suggested resume bullets" section below.

## Execution playbook — how I'm going to do this

### One-time GitHub setup

1. **Fork** the original repo to your GitHub account on github.com (keeps the "forked from"
   link — honest provenance for a course-derived project), then update the local clone's
   remote: set `origin` to your fork and (optionally) keep the original as `upstream`.
2. Keep the existing 3 commits as the base — do **not** re-init git. Your work is all new
   commits on top.
3. Confirm prerequisites are installed before the phases that need them: Docker + Docker
   Compose (Phase 5), `kind` or `minikube` + `kubectl` + `helm` (Phase 8), and the GitHub
   Actions runner is cloud-side so nothing local is needed for CI (Phase 7).

### Per-phase workflow (repeat for every phase)

1. `git checkout main && git pull` → `git checkout -b <phase-branch>` (branch names listed
   per phase below).
2. Do the work in the small ordered commits listed for that phase (conventional-commit
   messages: `feat:`, `fix:`, `test:`, `ci:`, `docs:`, `refactor:`, `chore:`).
3. Run the phase's verification locally (`make lint && make test`, plus the phase-specific
   checks in the Verification section) before pushing.
4. Push the branch, open a **PR into `main`**, self-review the diff, let CI go green (from
   Phase 7 on), then **squash-merge**. Tag a milestone (`v0.1` after Phase 1, `v1.0` after
   Phase 7, `v2.0` after Phase 11) so the GitHub releases page tells a story.
5. After merge, capture any demo artifact the phase produces (a `/status` screenshot, a
   failover GIF, a benchmark number, a Grafana dashboard PNG) into a `docs/` folder — these
   feed the README rewrite.

### Work-session cadence

- Aim for **one phase per focused session/weekend** for the smaller phases (0, 5, 6, 7, 10),
  and split the big ones (1, 2, 8, 12) across two sessions. Don't start a new phase until the
  previous PR is merged — keeps the history linear and each bullet self-contained.
- End every session in a green, mergeable state (or on a WIP branch that isn't merged). Never
  leave `main` broken.

### Realistic time estimates (solo, intermediate Python; hours)

Estimates assume you're comfortable with Python but learning Raft/Docker/K8s/gRPC as you go.

| Phase | Work | Low | High |
|---|---|---:|---:|
| 0 | Repo hygiene & baseline | 2 | 3 |
| 1 | Raft correctness fixes | 6 | 10 |
| 2 | Persistence & snapshotting | 8 | 14 |
| 3 | Membership & API hardening | 5 | 8 |
| 4 | Tests | 6 | 10 |
| 5 | Containerization & compose | 4 | 6 |
| 6 | Observability (logging, health, metrics) | 4 | 6 |
| 7 | CI, load test & docs | 5 | 8 |
| 8 | Kubernetes + Helm | 8 | 12 |
| 9 | Prometheus + Grafana dashboards | 4 | 6 |
| 10 | Chaos testing | 4 | 6 |
| 11 | gRPC transport | 10 | 15 |
| — | **Total** | **66** | **104** |

Realistically **~75–85 h** for the full flagship scope → **~9–12 weekends** at ~7 h each.
Add ~20–30% if learning Raft/gRPC from scratch.

**If you need to ship sooner**, the order is designed so you can cut a strong "v1.0" after
Phase 7 (~40–55 h: correctness, persistence, tests, containerized cluster, CI, README) and
treat Phases 8–11 as a "v2.0" you add over time. Each later phase is independent enough to
land on its own.

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

- Branch off `main` per phase: `git checkout -b <branch>`.
- Use **conventional commits** (`feat:`, `fix:`, `test:`, `ci:`, `docs:`, `refactor:`,
  `chore:`). Keep commits small and logically scoped within a branch.
- Open a PR per phase, self-review, then **squash-merge** to `main` so history reads as
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
   latency, and snapshot count. (Grafana dashboards built on these come in Phase 9.)

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
3. **Rewrite README (v1)**: architecture diagram, the consensus guarantees you implemented,
   how to run the cluster (compose), how to run tests, the observability endpoints, the
   benchmark results, and a clear "what's a simplification vs. the real Raft paper" section
   (honesty about joint consensus, etc.). This is the **v1.0 tag / shippable portfolio cut.**

Commits: `ci: add lint, type-check, test, and docker-build pipeline` →
`test: add load/throughput benchmark harness` →
`docs: rewrite README with architecture, run guide, and benchmarks`.

Resume value: "Built CI (lint/type/test/build) and a load-test harness; documented
architecture and measured throughput improvement after removing blocking commits."

---

## Phase 8 — Kubernetes + Helm (branch: `feat/k8s-helm`)

Goal: deploy the cluster on Kubernetes as a stateful workload — the headline SRE phase.
Target environment: **local `kind` or `minikube`** (free, screenshot-friendly).

1. **StatefulSet**: model the Raft nodes as a `StatefulSet` (stable network identities +
   per-pod `PersistentVolumeClaim` for the WAL/snapshot data dir from Phase 2). Use a
   **headless Service** so pods get stable DNS names the nodes use as their peer addresses
   (replaces the hardcoded `comm_dict`). Add readiness/liveness probes wired to the
   `/ready` and `/health` endpoints from Phase 6.
2. **Config**: a `ConfigMap` for cluster size / timings / snapshot interval; env injected
   into pods (reuse the Phase 3 env-based config).
3. **Helm chart**: package the above into a chart under `charts/yakt/` with `values.yaml`
   exposing `replicaCount` (3/5), image tag, resources, and storage size. `helm install`
   should bring up a working cluster.
4. **Run guide**: documented `kind create cluster` → load image → `helm install` → port-
   forward → run the demo client. Capture screenshots/PNG for the README.

Commits: `feat: add k8s StatefulSet, headless service, and probes` →
`feat: add ConfigMap-driven cluster configuration` →
`feat: package deployment as a Helm chart` →
`docs: add kind/minikube + helm run guide`.

Resume value: "Deployed a stateful Raft cluster on Kubernetes (StatefulSet + headless
service + per-pod persistent volumes) packaged as a Helm chart."

---

## Phase 9 — Prometheus + Grafana dashboards (branch: `feat/grafana-dashboards`)

Goal: a real metrics pipeline with a committed, reproducible dashboard. Builds on the
`/metrics` endpoint from Phase 6.

1. **Prometheus**: scrape config for the cluster — a `prometheus.yml` for the compose stack
   and a `ServiceMonitor` (or scrape annotations) for the k8s deployment. Add Prometheus +
   Grafana services to `docker-compose.yml` (a `monitoring` profile) and to the Helm chart
   (optional dependency or a sibling manifest).
2. **Grafana dashboard**: build a dashboard (leader per node, current term, commit index &
   commit lag per follower, election rate, request latency, log/snapshot size) and **export
   the dashboard JSON** into `monitoring/grafana/` so it's version-controlled and auto-
   provisioned. Provision the Prometheus datasource via config too (no click-ops).
3. **Demo artifact**: capture a dashboard PNG during a load/chaos run for the README.

Commits: `feat: add prometheus scrape config for compose and k8s` →
`feat: provision grafana datasource and cluster dashboard json` →
`docs: add monitoring run guide and dashboard screenshot`.

Resume value: "Built a Prometheus + Grafana monitoring stack with a version-controlled,
auto-provisioned dashboard visualizing leader, term, and per-follower commit lag."

---

## Phase 10 — Chaos testing (branch: `feat/chaos-testing`)

Goal: prove failover and recovery automatically, under load — the best demo material.

1. **Chaos harness**: a script/test that, while a load generator (Phase 7) drives writes,
   periodically kills a random node (compose: `docker kill`; k8s: `kubectl delete pod`) and
   asserts the cluster (a) elects a new leader within a bound, (b) keeps accepting writes
   after a brief blip, and (c) the killed/restarted node rejoins and catches up via
   snapshot (reuses Phase 2 recovery + Phase 6 `/status`).
2. **Invariant checks**: assert no committed entry is ever lost and at most one leader per
   term across the run (linearizability-lite safety check by reading `/status` from all
   nodes).
3. **Demo artifact**: record a terminal GIF (kill leader → new leader → writes continue) for
   the README; capture the corresponding Grafana panel.

Commits: `test: add chaos harness killing random nodes under load` →
`test: assert leader-uniqueness and no-committed-entry-loss invariants` →
`docs: add chaos demo gif and runbook`.

Resume value: "Wrote an automated chaos test that kills random nodes under load and verifies
leader re-election, write availability, and snapshot-based recovery."

---

## Phase 11 — gRPC transport (branch: `feat/grpc-transport`)

Goal: replace the ZeroMQ PUB/SUB + JSON inter-node transport with gRPC + protobuf — the
biggest backend-engineering lift and a strong "I understand RPC/serialization" signal.

1. **Define `.proto`**: model the Raft RPCs (`RequestVote`, `AppendEntries`/`Heartbeat`,
   `InstallSnapshot`, `ClientRequest`) and message types mirroring `raft/protocol.py`. Set up
   code generation (`grpcio-tools`) into a generated package; wire it into the build.
2. **Transport abstraction**: introduce a transport interface in `raft/interface.py` so the
   node logic doesn't care about the wire; implement a gRPC transport behind it. Keep the
   existing ZMQ transport selectable via config so you can A/B and avoid a risky big-bang
   swap. Each node runs a gRPC server (its peers are gRPC clients).
3. **Migrate + benchmark**: switch the default to gRPC, re-run the Phase 7 load test, and
   record the throughput/latency delta vs. the ZMQ/JSON baseline in the README.
4. **Tests**: ensure the Phase 4 integration/failover tests pass over the gRPC transport too
   (parametrize the cluster fixture by transport).

Commits: `feat: define raft rpc protobuf schema and codegen` →
`refactor: introduce pluggable transport interface` →
`feat: implement grpc transport and make it the default` →
`test: run integration suite over grpc transport` →
`docs: record grpc vs zmq benchmark`.

Resume value: "Re-architected inter-node communication behind a pluggable transport and
implemented a gRPC/protobuf transport; benchmarked it against the original ZeroMQ/JSON path."

This is the **v2.0 tag / full flagship cut.** Rewrite-bump the README to cover Phases 8–11.

---

## Suggested resume bullets (after the plan is executed)

- Built and **debugged a Raft consensus system** (leader election, log replication,
  snapshotting) in Python; fixed commit-index quorum and vote-safety bugs and added WAL
  persistence for crash recovery, verified by an **automated chaos test** that kills nodes
  under load.
- **Deployed the cluster on Kubernetes** as a StatefulSet (headless service + per-pod
  persistent volumes) packaged as a **Helm chart**; containerized it with docker-compose and
  improved write throughput by ~Nx after removing blocking commit handling (load-tested).
- Added **SRE tooling**: structured JSON logging, health/readiness probes, **Prometheus +
  Grafana** dashboards (leader, term, per-follower commit lag), and a **GitHub Actions CI**
  running lint, type checks, and multi-node integration/failover tests.
- **Re-architected inter-node RPC behind a pluggable transport** and implemented a
  **gRPC/protobuf** transport, benchmarked against the original ZeroMQ/JSON path.

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
  multi-node failover test) before squash-merging the PR to `main`.

For the flagship phases:

- **Phase 8 (k8s/Helm):** `kind create cluster` → build/load the image → `helm install yakt
  charts/yakt` → confirm 3 pods Ready, readiness probes green; `kubectl port-forward` and run
  the demo client; `kubectl delete pod <leader>` and confirm a new leader via `/status` and
  that the pod rejoins with its PVC intact.
- **Phase 9 (Grafana):** bring up the `monitoring` compose profile (or k8s stack), open
  Grafana, confirm the auto-provisioned dashboard shows live leader/term/commit-lag panels.
- **Phase 10 (chaos):** run the chaos harness against compose and against k8s; confirm the
  leader-uniqueness and no-lost-commit invariants hold and the recording/GIF is captured.
- **Phase 11 (gRPC):** run the full integration + failover suite with `transport=grpc` and
  `transport=zmq`; both must pass. Re-run the load test on both and record the delta.

## Note on file sync (post-approval)

This plan lives at two paths that have diverged. Once approved and out of plan mode, the
repo's `YAKt-Yet-Another_KRaft/Plan.md` is the **source of truth** — copy this final content
there so it ships with the project; this `~/.claude/plans/...` copy is just the working draft.
