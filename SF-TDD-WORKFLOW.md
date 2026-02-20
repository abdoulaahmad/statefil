# SF-TDD Workflow (Scaffold-First Test-Driven Development)

Last updated: 2026-02-19

## 1. Objective
Standardize how StateFabric features are built so correctness and safety are validated before implementation detail expands.

## 2. Loop

### Phase A: Scaffold
- Create module directory and public interface.
- Add types, config structs, and error enums.
- Add stub methods with explicit `NotImplemented` paths.
- Ensure scaffold compiles.

Expected output:
- Compiling skeleton with zero business logic.

### Phase B: Tests First
- Add contract tests against public interfaces.
- Add invariants tests for merge/ordering/durability where relevant.
- Confirm tests fail for the intended reason.

Expected output:
- Red test suite proving behavior is unimplemented.

### Phase C: Minimal Implementation
- Implement smallest logic to satisfy tests.
- Avoid optimization and refactor during this step.

Expected output:
- Green tests with minimal code.

### Phase D: Refactor
- Improve structure/performance without changing behavior.
- Keep test suite green after each change.

Expected output:
- Cleaner design, unchanged contract behavior.

### Phase E: Hardening
- Add fault-injection tests.
- Add regression tests for edge bugs found during implementation.
- Add operational telemetry assertions where applicable.

Expected output:
- Green suite including failure-mode coverage.

## 3. Required Test Types by Module

### CRDT modules
- Contract tests for API behavior.
- Merge algebra tests:
  - commutative
  - associative
  - idempotent
- Regression tests for known bug classes:
  - OR-Set delete resurrection
  - CRDT-Map nested merge overwrite

### Durability modules
- Crash-before-ack and crash-after-append scenarios.
- Partial segment replay behavior.
- Manifest consistency tests.

### Replication modules
- Out-of-order delivery handling.
- Duplicate delta idempotency.
- Backpressure and bounded queue behavior.

### Time/ordering modules
- HLC drift rejection.
- Canonical ordering determinism with concurrent operations.

### Security modules
- Default-deny policy checks.
- Unauthorized read/write denial.
- Signature verification failure handling.

## 4. Work Item Template

```text
[SF-TDD] <Work Item Name>

Scaffold
- [ ] Interfaces/types/stubs created
- [ ] Build compiles

Tests
- [ ] Contract tests added (failing first)
- [ ] Invariant/fault tests added (as applicable)

Implementation
- [ ] Minimal code to pass tests
- [ ] Refactor pass

Hardening
- [ ] Regression tests for discovered edge cases
- [ ] Metrics/events/logging assertions (if operationally relevant)
```

## 5. PR Gate Checklist
- Scaffold commit exists and is separated from feature logic.
- Tests were introduced before final implementation logic.
- New behavior has at least one failure-mode test.
- No strong semantics exposed through eventual API.
- No acknowledged write path bypasses durable append.

## 6. StateFabric-Specific Guardrails
- Never add CAS to eventual interfaces.
- Never acknowledge write before durable append succeeds.
- Never resolve CRDT-Map key conflicts by overwrite when child CRDT merge is required.
- Never merge OR-Set elements before unioning tombstones from both replicas.
- Never use UUID ordering as conflict ordering source.
