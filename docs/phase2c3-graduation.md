# Phase 2C.3 graduation proof

A newly observed graduation creates separate V4 and hook bootstrap rows at the
graduation block. The curve filter ends **before** the graduation log position;
V4 and hook logs start **after** it. The live worker uses the existing durable
Validation HTTP bootstrap jobs for the new filters. Ranges may exceed 100 blocks
only in this explicit bootstrap path. A filter cursor appears only after every
range from activation to the frozen head is proved. Incomplete, budget-waiting,
or expired jobs retain their bootstrap gap and have no cursor. Once bootstrapped,
normal recovery still rejects ranges above 100 blocks.

For a feature cutoff before graduation, only curve coverage is required.
Graduation exactly at the cutoff requires curve coverage before the graduation
log and V4/hook coverage after it, including a zero-duration clock interval
where same-second logs can exist. A later cutoff requires curve coverage up to
graduation and V4/hook coverage thereafter. Proof versions record these
intervals, boundary positions, filter identities, and their deterministic hash.
The feature payload contains no later-graduation predictor. Earlier eligible
versions remain immutable when graduation happens after their cutoff.

The read-only point-in-time audit counts all due ledger-era windows by whether
they cross graduation, reports their final coverage and first eligible version,
and compares eligibility rates. These are selection-bias diagnostics, not
evidence of a trading signal. A purported eligible version whose cutoff crosses
graduation but lacks all three filter proofs is excluded from model counts.
Historical gaps and versions are never rewritten by this change.
