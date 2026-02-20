# StateFabric Implementation Roadmap (Phase-to-Phase)

Last updated: 2026-02-19
Primary references:
- `FULL-ARCHITECTURE.md`
- `TECHNICAL-DETAILS.md`
- `SF-TDD-WORKFLOW.md`
- `risk assesment.md`

## 1. Roadmap Objective
Provide a full execution roadmap from current planning state to production-ready release, with clear phase transitions, SF-TDD delivery flow, and hard release gates.

## 2. Timeline Model

If kickoff starts the week of **February 23, 2026**, use this baseline:
- Phase 0: Weeks 1-2
- Phase 1: Weeks 3-6
- Phase 2: Weeks 7-10
- Phase 3: Weeks 11-14
- Phase 4: Weeks 15-19
- Phase 5: Weeks 20-24
- Phase 6: Weeks 25-28

Total: 28 weeks to GA-ready core platform (excluding optional enterprise expansions).

## 3. Phase Overview

| Phase | Name | Main Outcome | Gate |
|---|---|---|---|
| 0 | Foundation Lock | Repo, standards, scaffolds, CI gates ready | G0 |
| 1 | Correctness Core | CRDT/API correctness blockers closed | G1 |
| 2 | Durability Core | Durable-before-ack, replay-safe recovery | G2 |
| 3 | Serverless Replication | Object-log sync and recovery scale | G3 |
| 4 | Strong Consistency | Strong namespace + CAS/transactions | G4 |
| 5 | Security + Operability | Authz, encryption, observability hardening | G5 |
| 6 | GA Stabilization | Soak, perf, docs, release readiness | G6 |

## 4. Phase-by-Phase Details

## Phase 0: Foundation Lock (Weeks 1-2)

### Goals
- Freeze architecture contracts and SF-TDD process.
- Create compile-ready scaffolds for core modules.
- Set CI rules to enforce scaffold/test/feature flow.

### In scope
- Package structure and module boundaries.
- Interface definitions and stub implementations.
- Baseline CI: lint, typecheck, unit tests, coverage thresholds.
- PR template aligned to SF-TDD.

### SF-TDD cycle
1. Scaffold core interfaces (`Engine`, `DurabilityManager`, `ReplicationManager`).
2. Add failing contract tests for API shape and error paths.
3. Implement minimal pass-through stubs.
4. Refactor package boundaries.
5. Add CI policy tests (forbidden API patterns, e.g. eventual CAS).

### Deliverables
- Compiling monorepo skeleton.
- Test harness wired (Vitest + integration test runner).
- Static guards for banned patterns:
  - no CAS in eventual interface
  - no `transaction` in eventual API

### Exit criteria (G0)
- CI green on scaffold + test baseline.
- SF-TDD workflow accepted and documented in repo.
- All Phase 1 work items created with SF-TDD templates.

### Handoff to Phase 1
- Stable contracts and test harness.
- Work item backlog with owners and estimates.

---

## Phase 1: Correctness Core (Weeks 3-6)

### Goals
- Close correctness blockers from P0:
  - CRDT-Map merge bug
  - OR-Set tombstone merge bug
  - Eventual CAS removal
  - Eventual transaction rename to batch

### In scope
- CRDT invariants and regression suite.
- API contract split (`eventual` vs `strong`) at type level.
- Conflict event envelope structure.

### SF-TDD cycle
1. Scaffold CRDT merge interfaces and API wrappers.
2. Add failing tests for known bug reproductions.
3. Implement minimal fixes.
4. Refactor shared merge utilities.
5. Add property-based tests (commutative/associative/idempotent).

### Deliverables
- Fixed CRDT merge behavior.
- Eventual API free of linearizable semantics.
- Regression tests permanently covering bug classes.

### Exit criteria (G1)
- All P0 correctness risks closed: #9, #10, #15, #4.
- 100% pass on CRDT invariant test suite.
- No data-loss/correctness open blockers tagged `P0-correctness`.

### Handoff to Phase 2
- Verified merge engine.
- Stable operation envelope semantics.

---

## Phase 2: Durability Core (Weeks 7-10)

### Goals
- Enforce durable append before write acknowledgement.
- Implement replay-safe recovery with segment manifests and snapshots.

### In scope
- Segment writer, segment manifest, checkpoint model.
- Snapshot creation, validation, and replay.
- Crash recovery and partial-write handling.

### SF-TDD cycle
1. Scaffold append/replay contracts.
2. Add failing crash-path tests.
3. Implement append-before-ack.
4. Refactor manifest and checkpoint utilities.
5. Add fault injection for timeout/OOM/abrupt termination.

### Deliverables
- Durable write path (`append -> apply -> ack`).
- Recovery pipeline:
  - load snapshot
  - load committed segments
  - canonical replay `(hlc, nodeId, sequence)`
- Segment checksum and corruption handling.

### Exit criteria (G2)
- P0 durability risk #1 closed by tests.
- Acknowledged writes survive crash-recovery tests.
- Replay determinism verified with randomized operation ordering.

### Handoff to Phase 3
- Reliable persistence core ready for replication scale.

---

## Phase 3: Serverless Replication (Weeks 11-14)

### Goals
- Ship serverless-first sync model using object-log synchronization.
- Address cost/latency with batched segments and bounded parallel recovery.

### In scope
- Batched append strategy and flush policy.
- Recovery read parallelism with backpressure limits.
- Replication lag metrics and conflict visibility.
- HLC skew rejection and telemetry.

### SF-TDD cycle
1. Scaffold replication mode interfaces (`serverless`, `cluster`).
2. Add failing tests for batching, lag, and skew rejection.
3. Implement serverless mode first.
4. Refactor sync scheduler and retry policies.
5. Add load tests for burst writes and cold recovery.

### Deliverables
- S3/object-store batch append (no per-op PUT behavior).
- Parallel segment loading with configurable caps.
- HLC max skew guardrail with reject counters.

### Exit criteria (G3)
- P0 risks #5 and #6 closed.
- Recovery benchmark meets target under expected segment counts.
- Cost model documented with measured request profile.

### Handoff to Phase 4
- Stable eventual replication path.
- Validated performance envelope for serverless mode.

---

## Phase 4: Strong Consistency (Weeks 15-19)

### Goals
- Deliver strong namespace path for operations requiring linearizability.
- Enable safe CAS and strong transactions only under consensus.

### In scope
- Strong key registry.
- Consensus integration (embedded or external adapter).
- Strong `register/map/counter` implementations.
- Failure behavior (`consensus unavailable => fail fast`).

### SF-TDD cycle
1. Scaffold strong API and consensus adapter contracts.
2. Add failing tests for CAS/transaction linearizability behavior.
3. Implement minimal strong path with single consensus group.
4. Refactor state routing (`eventual` vs `strong`).
5. Add partition/failover tests.

### Deliverables
- Strong API with correct CAS semantics.
- Strict boundary: no fallback from strong to eventual.
- Financial/inventory sample flows moved to strong API only.

### Exit criteria (G4)
- Strong operations pass linearizability acceptance tests.
- P0 risk #2 fully mitigated for bounded resources through strong path.
- Zero unsafe examples left in docs.

### Handoff to Phase 5
- Complete dual-consistency runtime behavior.

---

## Phase 5: Security + Operability Hardening (Weeks 20-24)

### Goals
- Enforce policy controls and auditability.
- Complete production observability and cluster safety controls.

### In scope
- AuthN/AuthZ integrations (IAM/OIDC/JWT adapters).
- Default-deny policy engine.
- SSE-KMS defaults and audit trails.
- Conflict event stream + dashboards.
- Backpressure controls for replication channels.

### SF-TDD cycle
1. Scaffold security and telemetry interfaces.
2. Add failing authorization/audit tests.
3. Implement mandatory checks and encryption defaults.
4. Refactor policy evaluation and event schemas.
5. Add chaos tests for replication backpressure/failure spikes.

### Deliverables
- Security controls covering all read/write/admin paths.
- Audit events for policy and write operations.
- Operational dashboards and SLO monitors.

### Exit criteria (G5)
- P1 security/ops risks closed: #8, #14, #16, #19.
- Encryption-at-rest default active (#18 tracked for enterprise posture if additional controls needed).
- On-call runbook and alert thresholds published.

### Handoff to Phase 6
- Production operations baseline ready for soak and release.

---

## Phase 6: GA Stabilization (Weeks 25-28)

### Goals
- Validate system under sustained load and fault scenarios.
- Complete release docs, migration docs, and go/no-go evidence.

### In scope
- End-to-end soak tests.
- Multi-profile memory and performance qualification.
- Schema evolution migration rehearsal.
- Final release checklist closure.

### SF-TDD cycle
1. Scaffold release validation suites.
2. Add failing end-to-end acceptance suites.
3. Implement missing hardening deltas.
4. Refactor docs/tooling.
5. Add regression pack for all P0/P1 incidents found.

### Deliverables
- GA test report with pass/fail matrix.
- Release documentation:
  - consistency guide
  - operational runbook
  - upgrade/migration procedures

### Exit criteria (G6)
- All P0 and P1 checklist items closed (`risk assesment.md`).
- Performance and reliability SLOs achieved.
- Formal go/no-go sign-off completed.

## 5. Transition Rules (Phase N -> N+1)

Each phase transition requires:
1. Exit criteria evidence attached to milestone.
2. No unresolved blocker labeled `phase-gate-blocker`.
3. Risk register updated with residual risks and mitigations.
4. Handoff artifact bundle completed:
  - architecture deltas
  - test reports
  - known issues
  - rollback plan

If any gate fails:
- Freeze new feature scope.
- Open remediation sprint.
- Re-run gate with updated evidence.

## 6. Cross-Phase Engineering Cadence

- Sprint length: 2 weeks.
- Planning: first day of sprint.
- Mid-sprint risk review: day 5 or 6.
- Demo + retrospective + gate review: last day.
- Release branch cut: only at phase gate completion.

## 7. Ownership Model

- Architecture owner: maintains contracts and ADR updates.
- Runtime owner: engine, CRDT, durability implementation.
- Platform owner: CI/CD, test infra, release gates.
- Security owner: policy, auth integrations, audit.
- SRE owner: observability, SLOs, runbooks.

## 8. Metrics by Phase

### Delivery metrics
- Story completion rate per sprint.
- Escaped defect count per phase.
- Mean time to fix regression.

### Quality metrics
- Test pass rate and flake rate.
- Mutation score for critical merge/durability code paths.
- Crash recovery success rate.

### Runtime metrics
- Write ack latency.
- Recovery time.
- Replication lag.
- Memory budget adherence.

## 9. Go/No-Go Checklist (Final)

Must be true before production launch:
- Durable-before-ack guarantee verified under fault injection.
- Eventual and strong API semantics are clearly separated and tested.
- CRDT invariant suite is stable and green.
- Security controls enforced with audit evidence.
- Rollback and incident response procedures validated.

## 10. Immediate Next Actions
1. Approve phase timeline and owners.
2. Create Phase 0 and Phase 1 ticket board using `SF-TDD-WORKFLOW.md`.
3. Start with WP1 (`FULL-ARCHITECTURE.md`) and enforce G1 as first hard gate.
