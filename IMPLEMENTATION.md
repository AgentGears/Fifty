# G1 kernel bootstrap

This increment establishes only the verification and canonical-store seams needed before canonical Workspace state is implemented.

It contains:

- opaque semantic identifier primitives;
- controllable time and identifier sources for deterministic verification;
- the transactional canonical-store port;
- an in-memory verification store with rollback, transaction-lifetime, stale-version, and context-cancelable serialization behavior;
- a scenario runner with fail-closed cancellation, panic containment, and non-vacuous qualification semantics;
- the minimal project module manifest needed for a clean checkout to resolve intra-project packages.

It deliberately does not contain Workspace, Principal, Interaction, specialist, work, memory, background execution, or external-effect implementations.

## Verification boundary

The repository is expected to verify from a clean checkout under the runtime supplied by the approved external supply boundary.

Before this increment can merge, the commit-bound verification evidence must include:

- ordinary unit verification;
- race/concurrency verification;
- static analysis;
- terminology-sovereignty validation;
- exhaustive first-pass maintainer review;
- independent secondary review or an explicitly separated fallback secondary pass;
- reconciliation of both review passes.

Scenario runners execute synchronously and must honor context cancellation. Panic containment applies to the goroutine executing the scenario runner. If a scenario starts concurrent work, it must join that work before returning and must contain panics raised by its child goroutines. An uncontained child-goroutine panic is a process-level verification failure rather than a contained scenario result.

The in-process harness does not abandon a timed-out runner in a background goroutine because that runner could continue mutating verification fixtures after failure was reported. A non-cooperative hang must therefore be bounded by a process-level wall-clock watchdog at the external verification boundary, where termination and failure evidence can be handled without claiming the scenario itself was safely stopped.

The in-memory `Report` is the deterministic semantic result of scenario execution. Durable verification receipts and artifacts are produced by the external commit-bound verification boundary rather than by this bootstrap runner.

No local-only wrapper or unrecorded source file may be required to compile the checked-in code.
