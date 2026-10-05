"""Derived EVM/query identities; never rewrite retained representations."""
import json
import re


def canonical_address(value):
    if not isinstance(value, str) or not re.fullmatch(r'0x[0-9a-fA-F]{40}', value):
        raise ValueError('Invalid 20-byte EVM address')
    return value.lower()


def query_identity(query):
    if not isinstance(query, dict) or 'address' not in query:
        raise ValueError('Address query required')
    return dict(query, address=canonical_address(query['address']))


def compatible_ranges(db, launch, kind, query, semantics, first, last, legacy_before=None):
    """Only identity-bound, successfully committed ranges with matching lifecycle."""
    identity = query_identity(query)
    rows = db.conn.execute('''SELECT r.*,i.query_json,i.upper_at,j.original_safe_start,
      m.value AS semantics FROM flow_shadow_ranges r
      JOIN flow_bootstrap_identity i USING(stage,launch_id,kind)
      JOIN flow_shadow_jobs j USING(stage,launch_id,kind)
      LEFT JOIN flow_shadow_meta m ON m.key=r.stage||':semantics:'||r.launch_id||':'||r.kind
      WHERE r.launch_id=? AND r.kind=? AND r.first_block<=? AND r.last_block>=?
        AND r.last_block<=j.highest_contiguous_verified_block
        AND r.first_block>=j.original_safe_start AND r.last_block<=j.reconciliation_upper_bound
        AND r.last_block>=r.first_block
      ORDER BY r.first_block,r.last_block,r.stage''', (launch, kind, last, first))
    result = []
    for row in rows:
        try:
            if query_identity(json.loads(row['query_json'])) != identity:
                continue
        except (ValueError, TypeError):
            continue
        lifecycle_ok = row['semantics'] == semantics
        # Legacy live curve bootstrap predates lifecycle meta. Its exact activation
        # record, immutable launch identity and pre-switch query must corroborate it.
        if row['semantics'] is None and legacy_before is not None and kind == 'curve':
            state = db.conn.execute('SELECT * FROM flow_bootstrap WHERE launch_id=? AND kind=?',
                                    (launch, kind)).fetchone()
            lifecycle = json.loads(semantics)
            lifecycle_ok = (row['stage'] == f'live_bootstrap:{launch}' and state is not None
                and state['status'] == 'complete' and state['safe_start'] == lifecycle['launch_block']
                and row['original_safe_start'] == state['safe_start']
                and row['upper_at'] <= legacy_before and row['last_block'] <= state['completed_head'])
        if lifecycle_ok:
            result.append({k: row[k] for k in ('stage', 'launch_id', 'kind', 'first_block', 'last_block', 'was_terminal')})
    return result
