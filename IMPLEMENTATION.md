# G1 kernel bootstrap

This increment establishes only the verification and canonical-store seams needed before canonical Workspace state is implemented.

It contains:

- opaque semantic identifier primitives;
- controllable time and identifier sources for deterministic verification;
- the transactional canonical-store port;
- an in-memory verification store with rollback and stale-version behavior;
- a scenario runner where a skipped required scenario cannot qualify as passing.

It deliberately does not contain Workspace, Principal, Interaction, specialist, work, memory, background execution, or external-effect implementations.
