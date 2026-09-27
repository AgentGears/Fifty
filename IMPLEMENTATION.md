# G1 kernel bootstrap

This increment establishes only the verification and canonical-store seams needed before canonical Workspace state is implemented.

It contains:

- opaque semantic identifier primitives;
- controllable time and identifier sources for deterministic verification;
- the transactional canonical-store port;
- an in-memory verification store with rollback, transaction-lifetime, and stale-version behavior;
- a scenario runner with fail-closed cancellation and non-vacuous qualification semantics;
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

No local-only wrapper or unrecorded source file may be required to compile the checked-in code.
