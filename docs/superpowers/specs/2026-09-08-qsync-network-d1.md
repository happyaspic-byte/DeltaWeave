# QSync D1: authenticated roster and heartbeat

This note records the D1 implementation checkpoint for the network worktree. It
does not change the existing `owner_share_catalog_v3` postcard or the
`deltaweave/share/3` variant order.

## Public surface

`ShareSession::refresh_roster()` sends the appended `Operation::Roster` control
operation and returns an owner-signed `SignedRoster`. The one-use challenge is
held privately by the session and can be read only as the opaque
`ShareSession::roster_challenge()` value. `ShareSession::heartbeat(challenge)`
signs the current device endpoint and sends the appended
`Operation::Heartbeat` operation.

`SignedRoster::verify_for(owner, share, now)` checks the trusted owner/share
binding, owner signature, bounded lifetime, clock skew, entry shape, and member
uniqueness. `fresh_members(now)` is the provider-selection input; stale rows
remain observable in the signed roster but are excluded from this list. Roster
addresses and heartbeat state never authorize enrollment or permission.

## Durable owner state

The owner registry stores the signed roster and one challenge per
`(share_id, member_id)` in separate `share_roster_v1` and
`share_roster_heartbeat_v1` redb tables. A challenge is one-use, owner/share/
member-bound, and expires after 90 seconds. The owner checks both the
authenticated QUIC `remote_id()` and the member signature before replacing an
address. The owner receive timestamp anchors `heartbeat_at` and
`heartbeat_expires_at`; the member-provided `sent_at` is only a bounded
freshness check.

The roster is bounded to 4096 entries, 16 transport addresses per entry, and a
64 KiB serialized frame. A roster that cannot satisfy the frame bound fails
closed with `Busy`; the durable membership catalog is not truncated. Epoch-zero,
duplicate, cross-share, wrong-owner, stale, replayed, and endpoint-identity
mismatched inputs are rejected.

## D1 verification

The focused tests are recorded under
`/home/ubuntu/project/DeltaWeave-qsync-evidence/2026-09-08/d-roster/`:

- `d1-roster-unit.log`: three shape, binding, freshness, signature, and
  endpoint checks passed.
- `d1-roster-service.log`: the isolated owner/member DirectOnly flow passed;
  it exercised roster refresh, owner-received heartbeat time, address update,
  one-use replay rejection, cross-share rejection, and non-member rejection.

These are DirectOnly fixture results. Internet/N0 relay and multi-device
network evidence remain a later D/F gate, and D2 owner control/grant input
surfaces remain pending.
