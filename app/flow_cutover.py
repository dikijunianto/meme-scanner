"""Durable, migration-only barrier between split WSS startup and HTTP proof."""
import json
import time
from uuid import uuid4


KEY = 'phase2b2_cutover_session'
PENDING = {'STOP_TAIL_VERIFIED', 'SPLIT_WSS_CONNECTING', 'SUBSCRIPTIONS_READY',
           'READY_TAIL_PENDING'}


def session(db):
    value = db.state(KEY)
    return json.loads(value) if value else None


def save(db, value):
    db.conn.execute('''INSERT INTO flow_state VALUES(?,?) ON CONFLICT(key)
      DO UPDATE SET value=excluded.value''', (KEY, json.dumps(value, sort_keys=True)))


def advance(db, value, state, **fields):
    value = dict(value, **fields, state=state)
    value['history'] = [*value.get('history', []), [state, time.time()]]
    with db.conn:
        save(db, value)
        db.conn.execute('''INSERT INTO flow_state VALUES('cutover_state',?) ON CONFLICT(key)
          DO UPDATE SET value=excluded.value''', (state.lower(),))
    return value


def new(db, h_prefetch, h_stop, targets):
    prior = session(db)
    if prior and prior['state'] in PENDING:
        if prior['H_prefetch']==h_prefetch and prior['H_stop']==h_stop and prior['targets']==targets:
            return prior
        raise ValueError('Another cutover session is incomplete')
    if prior:
        with db.conn:
            db.conn.execute('''INSERT OR IGNORE INTO flow_shadow_meta VALUES(?,?)''',
                            ('cutover_session_archive:'+prior['id'],json.dumps(prior,sort_keys=True)))
    value = {'id': uuid4().hex, 'state': 'STOP_TAIL_VERIFIED',
             'H_prefetch': h_prefetch, 'H_stop': h_stop, 'H_live': None,
             'expected': {'wss': 'publicnode', 'fallback_wss': 'validation',
                          'http': 'validation'}, 'targets': targets,
             'gap_id_before': db.conn.execute('SELECT coalesce(max(id),0) FROM flow_gaps').fetchone()[0],
             'subscription_ready_at': None, 'subscription_ready_provider': None,
             'ready_tail_verified': False, 'history': []}
    return advance(db, value, 'STOP_TAIL_VERIFIED')


def gap_counts(db):
    active = db.conn.execute('''SELECT count(*) FROM flow_gaps g JOIN flow_tracking_targets t
      ON t.launch_id=g.launch_id WHERE g.resolved=0 AND t.status NOT IN ('completed','partial')''').fetchone()[0]
    historical = db.conn.execute('''SELECT count(*) FROM flow_gaps g JOIN flow_tracking_targets t
      ON t.launch_id=g.launch_id WHERE g.resolved=0 AND t.status IN ('completed','partial')''').fetchone()[0]
    return active, historical


def unexpected_gap(db, value):
    ids={t['launch_id'] for t in value['targets']}
    return any(r['launch_id'] in ids or r['status'] not in ('completed','partial')
               for r in db.conn.execute('''SELECT g.launch_id,t.status FROM flow_gaps g
                 JOIN flow_tracking_targets t ON t.launch_id=g.launch_id
                 WHERE g.resolved=0 AND g.id>?''', (value['gap_id_before'],)))
