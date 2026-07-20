# Content Read Lease v1

## Purpose

This protocol is the Trine KV storage boundary for short-lived reads of sealed
immutable `ContentObject` bytes. Higher layers decide who may read. Trine KV
receives only an opaque owner identity and prevents future relocation or
reclamation from treating an unexpired exact-content lease as absent.

## Identity and ownership

- `ContentLeaseId` is a generated, versioned 16-byte identity. Byte zero is
  format version `1`; the remaining bytes are random.
- `ContentLeaseOwnerId` is an opaque 16-byte higher-layer decision or session
  identity. Trine KV compares it but does not interpret Principal, tenant, role,
  or Policy semantics.
- One lease fixes exactly `(StorageDomainId, ContentId, ContentLeaseId)`.
- Handle clones share the same lease and deadline. The lease does not follow a
  name, File identity, Branch, or later content replacement.

## Durable record

Lease records live in the protected `\x01trine-content-lease\x01` bucket.

Key, in byte order:

```text
StorageDomainId[16] | ContentId[33] | ContentLeaseId[16]
```

The prefix therefore supports bounded inspection of leases for one exact
content identity without scanning other domains or content.

Value, in byte order:

```text
"TRNCNLS1"[8]
| ContentLeaseId[16]
| ContentLeaseOwnerId[16]
| StorageDomainId[16]
| ContentId[33]
| expires_at_unix_ms[u64 little-endian]
```

Decoding validates format, full length, versioned lease identity, and equality
between key and value identities. Unknown or inconsistent records fail closed.

## Open, read, renewal, and expiry

`open_content_leased` validates the sealed descriptor, validates a lifetime of
at least one whole millisecond, generates a lease identity, commits the durable
record, and only then returns a leased handle. Failure before commit returns no
handle. A committed record followed by a lost response is conservative and
expires without explicit cleanup.

Every leased range read checks the clone-shared deadline before reading and at
each chunk boundary. Once current Unix time is greater than or equal to the
deadline, the read returns `ContentLeaseExpired`. A read already in progress may
fail explicitly if its lease expires; it must never switch to another
`ContentId` or return unverified bytes.

Renewal reads and validates the durable record in an optimistic transaction. It
requires the exact content, lease, and owner identities and rejects an expired
lease; expiry cannot be revived. The new deadline is the greater of the current
deadline and `now + ttl`, and becomes visible to handle clones only after the
durable commit succeeds. Repeated conflicts are bounded.

Drop performs no asynchronous I/O. Abandoned lease records remain conservative
until expiry. Background deletion of expired records is optional housekeeping,
not a correctness requirement.

## Reclamation contract

An exact-prefix scan is a conservative precheck, not deletion authority. A new
leased open or renewal can race immediately after an ordinary scan. Before
removing or making unavailable any representation of one
`(StorageDomainId, ContentId)`, the future reclamation path must first establish
a per-content quarantine/fence that makes later open and renewal fail or join a
newer generation. It then rechecks the exact lease prefix under that barrier and
treats every well-formed unexpired record as live. Malformed records fail closed
and block reclamation until repaired. Grace starts only after the barrier and
recheck succeed; the barrier/generation is revalidated before physical delete.

The current `content_has_active_lease` helper is intentionally internal and is
only this conservative precheck. It must not be promoted into a final absence
proof without the barrier above. Lease checks compose with, but do not replace,
higher-layer reachability proof, quarantine, and grace.

Physical relocation and reclamation are outside this slice. Their future tests
must cover a read spanning relocation, expiry races, lease renewal races, and
crash/reopen behavior against this record format.
