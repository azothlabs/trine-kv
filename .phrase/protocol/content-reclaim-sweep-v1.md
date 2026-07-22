# Content Reclaim Sweep v1

V1 is superseded by `content-reclaim-sweep-v2.md`.

Its durable `TRNCRSW1` record enabled only the qualified native filesystem and
did not bind a backend/provider evidence identity. Current code writes and
accepts only `TRNCRSW2`; there is no deployed database migration requirement.
The authority chain, chunk-before-descriptor order, Prepared recovery, and
Reclaimed tombstone semantics continue in v2.
