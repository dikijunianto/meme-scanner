"""Minute counters, local observations only; never provider billing data."""
from datetime import datetime, timezone

METHODS = {"eth_chainId", "eth_blockNumber", "eth_getBlockByNumber", "eth_getLogs", "eth_call", "eth_getCode"}
METRICS = {"http_total", "http_429", "rpc_rate_limits", "retries", "failed_requests",
           "ws_reconnects", "ws_log_notifications", "ws_bytes", "launches_received",
           "stock_paired_launches", "metadata_calls", "blocks_recovered"} | {"method:" + m for m in METHODS}
METRICS |= {"ws_events_processed", "ws_launch_events", "ws_graduation_events",
            "startup_recovery_getlogs", "reconnect_recovery_getlogs", "manual_backfill_getlogs",
            "diagnostic_getlogs", "metadata_calls_stock", "metadata_calls_nonstock",
            "block_header_cache_hits", "block_header_cache_misses", "quote_registry_hits",
            "quote_registry_misses", "skipped_nonstock_metadata_calls",
            "intentional_gap_count", "intentional_gap_blocks"}
METRICS |= {"market_snapshot_curve", "market_snapshot_v4", "market_static_cache_fill",
            "market_retry", "market_target_due", "market_targets_completed",
            "market_targets_skipped_budget", "market_targets_failed", "market_targets_partial",
            "market_long_horizon_sampled", "market_long_horizon_not_sampled", "market_rpc_calls"}


class Telemetry:
    def __init__(self, db):
        self.db = db
        with db.conn:
            if db.state("telemetry_started_at") is None:
                db.set_state("telemetry_started_at", datetime.now(timezone.utc).isoformat())

    def add(self, metric, count=1):
        if metric not in METRICS or type(count) is not int or count < 0:
            raise ValueError("Invalid telemetry counter")
        minute = int(datetime.now(timezone.utc).timestamp()) // 60 * 60
        with self.db.conn:
            self.db.conn.execute("INSERT INTO rpc_usage VALUES(?,?,?) ON CONFLICT(minute,metric) "
                                 "DO UPDATE SET count=count+excluded.count", (minute, metric, count))
            # ponytail: minute buckets retained 90 days; export externally if longer history is needed.
            self.db.conn.execute("DELETE FROM rpc_usage WHERE minute<?", (minute - 90 * 86400,))

    def status(self, value):
        if value not in ("connected", "disconnected", "stopped"):
            raise ValueError("Invalid WebSocket state")
        with self.db.conn:
            self.db.set_state("websocket_status", value)
