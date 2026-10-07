# Phase 2C.4r — stopped, complete local startup contract

This task authorizes containment and structural repair, never a flow start. The
existing Epoch2 segment and fixed Validation boundary remain unchanged. Operational
ACTIVE is retained history; it is not research validation. No fresh PIT or clean
clock is created by schema repair or local preflight.

## Exact failure and ownership

At 85f82dc, app.flow_worker.main checked db.state('schema_version') != '1' and
raised ValueError('Run SQLite-safe flow initialization first'). FlowDB.state used
SELECT value FROM flow_state WHERE key=? with parameter schema_version. The table
is flow_state(key TEXT PRIMARY KEY,value TEXT NOT NULL); the expected stored value
is TEXT '1', not an SQL column named schema_version. Production had no such row.

scripts.init_flow.initialize and flow_epochs.prepare separately called
set_state('schema_version',1), whose str(value) and INSERT...ON CONFLICT update
stored TEXT '1'. flow_segments.prepare called migrate but omitted that separate
write. Phase4p repair/prestart checked objects only, so tables complete/version
missing was wrongly accepted. The old real entrypoint is retained verbatim in the
offline reproduction fixture; with tracking enabled it fails before FlowWorker.

Structural version ownership now belongs to FlowDB.migrate's canonical schema
transaction via flow_partition_schema.initialize_metadata. Existing version is
never silently downgraded. Unknown/malformed versions fail closed; current missing
version is repaired by insertion only. No new completion/readiness marker exists.
Segment sibling metadata is epoch_catalog_path and phase2b_coverage_start_at,
validated against catalog ownership and the exact fixed boundary. Creation already
establishes these plus its matching prospective ledger. recovery_state is mutable
operational/proof state, not a structural readiness marker: repair does not set it
or seed healthy/starting/connected/ACK/head/cursor/bootstrap state.

## One startup truth source

validate_startup_contract is deterministic and read-only. It derives canonical
objects from the actual initializer, avoiding copied schema definitions. Every
mandatory table's table_xinfo, primary keys, generated columns, defaults, complete
DDL constraints and foreign keys are compared, as are all required index/view/
trigger definitions including uniqueness and immutable-ledger triggers. Integrity
and foreign_key_check must pass. flow_state must have exactly TEXT version1.

For a segment, validate parent epoch/state, current segment identity/state/path,
Validation chain4663/header number/timestamp/hash, source revision, catalog path,
coverage start, matching prospective ledger, and preclean debt identity. When a
catalog is supplied, validate allocator/usage/epoch/segment query columns and
positive integer connection/switch allocator rows without allocating IDs.
Failures return SEGMENT_SCHEMA_INCOMPLETE with sorted exact diagnostics. Quoted SQL
literals remain case-sensitive. Integrity scans run at startup/preflight/repair;
repeated runtime structural checks reuse the same validator without full-data scans.

Creation checks the same contract before publishing its prospective segment.
Stopped repair runs canonical IF NOT EXISTS structural DDL and structural metadata
inside BEGIN IMMEDIATE, then the same full validator before commit. Wrong existing
definitions/unknown versions/identity mismatches roll back; existing data is never
ALTERed/deleted. Commit loss is observable and an idempotent rerun converges. No
chain/proof table rows or catalog segment/epoch rows are inserted by repair.

## Real local startup inventory and boundary

1. Service executes .venv/bin/python -m app.flow_worker; main loads FlowSettings,
   Config and FlowProviders through their existing canonical validators. Guards:
   flags/windows/tx enrichment, numeric limits, protected permissions, provider
   URL roles, chain4663, factories, stock registry, scanner modes/limits.
2. Writer lock uses no-follow exclusive flock; separate main/flow paths required.
   FlowDB selects the existing current epoch/segment and refuses missing selected
   partitions or inconsistent segment parent. Main opens SQLite mode=ro.
3. initialize_worker validates active/activating parent, full segment contract,
   split research routing, and canonical version before constructing FlowWorker.
4. FlowWorker constructor establishes queues/cache/empty routes/recovery maps and
   creates an HTTP client object without sending. No lazy shadow DDL is needed.
5. local_startup_plan runs in actual run() and production preflight: canonical
   cutover/session parsing and split-start guard; active target/filter planning;
   last-connected-block/timestamps parsing; switch/bootstrap JSON parsing;
   persisted-cohort discovery query; resource/disk pressure; catalog WSS-byte
   query; configured WSS-provider selection. SQL/JSON/type/path failures become
   local SEGMENT_SCHEMA_INCOMPLETE, never provider failures or reconnects.
6. Actual run then performs its unchanged operational starting/dirty/uncertainty
   writes on live startup. The local plan has validated its inputs; dry-run does
   not execute those writes. Resource/budget pressure can defer transport without
   manufacturing proof. The first external boundary is WSS connect; subsequent
   chainId, subscriptions, discovery/Validation bootstrap/tail are next-task work.

No current target is invented to test subscriptions in production. Offline tests
use canonical fresh segments and production-like post4q state, retain66 gaps and
FAILED historical switch, and drive real run() to a denied first connect sentinel.
The no-network preflight uses the same initialize_worker and local_startup_plan,
read-only DB transactions and writer lock. It denies socket connect/connect_ex,
create_connection and DNS resolution and requires zero attempts. No provider call,
subscription, feature rebuilding, eligibility or recovery write can run there.

## Stopped production commands

From clean tested /opt/meme-scanner, while flow inactive/PID0:

```sh
sudo -u ubuntu .venv/bin/python scripts/flow_segment_schema.py \
  --repair-existing-segment EPOCH2_RESEARCH_CLEAN_1 \
  --expected-epoch GENERALIZED_BOOTSTRAP_EPOCH_2 \
  --expected-start-block 82474128 --expected-start-at 1791376255
sudo -u ubuntu .venv/bin/python scripts/flow_segment_schema.py
sudo -u ubuntu .venv/bin/python scripts/flow_local_preflight.py
```

Repair inserts only absent canonical structural metadata and missing structural
objects. Full preexisting-table fingerprints exclude only the authorized new
schema_version row when comparing; all other state/proof/data/history must match.
No source or DB rollback implies an extra start. On failure retain evidence and
leave flow stopped. Do not alter Restart policy, disable/mask unit or clear debt.
