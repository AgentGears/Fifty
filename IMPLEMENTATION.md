# Fifty implementation

Fifty is developed as a solo-developer, AI-assisted product. The implementation process is intentionally lightweight: build one coherent capability increment, run the relevant deterministic/race/static checks available for the increment, perform a focused AI-assisted review, fix material defects, merge, and continue. Independent secondary review and external commit-bound verification are optional diagnostics rather than merge gates.

## Current kernel

Completed foundation:

- opaque semantic identifiers and controllable clocks;
- transactional canonical-store seam plus in-memory verification store;
- first-party filesystem-backed persistence with atomic publication, integrity checking, restart reconstruction, and backup/restore;
- durable Workspace and Principal identity;
- access/recovery credential rotation with digest-only persistence.

## CAP-102 increment A — canonical interaction history and bounded context

This increment introduces the first Conversation & Context implementation:

- `Interaction` is canonical history with explicit semantic identity, author attribution, direction, admission time, content reference, data classification, semantic references, and deletion state;
- interaction history is durable across process/session replacement;
- `ContextView` is a non-canonical copy derived from canonical Interaction state and is bounded by an explicit maximum interaction count;
- mutating a `ContextView` cannot mutate canonical Interaction state;
- exact semantic-reference resolution fails closed when a label is unresolved or maps to more than one semantic identity;
- D2 admission fails closed until its conditional admission workflow exists;
- D3 is never admitted to ordinary conversation state;
- context regeneration excludes D2 and any non-`RECORDED` interaction state.

The storage representation intentionally aggregates Interaction objects inside one workspace-scoped canonical history record. Storage record identity is not semantic object identity; every Interaction retains its own Fifty semantic identifier and lifecycle fields.

## Deliberate non-scope for this increment

This increment does not choose or activate:

- the exact Interaction deletion/tombstone implementation;
- the D2 admission workflow or model-disclosure record;
- fuzzy/similarity-based reference resolution;
- memory, work, specialist, authority/effect, or unresolved-matter semantics;
- an external content store or provider abstraction.

Those concerns should be added only when their owning capability or an explicit trust/data decision requires them.
