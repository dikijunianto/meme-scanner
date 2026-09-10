from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit
import math
import os

from dotenv import dotenv_values
from eth_utils import to_checksum_address

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Config:
    rpc_http: str = field(repr=False)
    chain_id: int
    factories: tuple[str, ...]
    stock_url: str
    database: Path
    log_path: Path
    poll: float = 5
    timeout: float = 20
    retry_max: float = 60
    confirmations: int = 3
    overlap: int = 5
    batch: int = 50
    retention: int = 10000
    reorg_depth: int = 100
    start_block: int | None = None
    rpc_rps: float = 5
    rpc_ws: str = field(default="", repr=False)
    fallback_http: str = field(default="", repr=False)
    retry_attempts: int = 5
    retry_base: float = 2
    log_span: int = 10
    ws_subscription: str = "logs"
    mode: str = "live"
    live_overlap: int = 5
    recovery_max: int = 10000
    transport_mode: str = "http_complete"
    startup_recovery_max: int = 500
    head_healthcheck: float = 60

    @classmethod
    def load(cls):
        path = Path(os.environ.get("SCANNER_ENV", ROOT / "config/.env"))
        if not path.is_file():
            raise ValueError("Missing config/.env; copy config/.env.example first")
        if os.name != "nt" and path.stat().st_mode & 0o077:
            raise ValueError("Configuration must have permissions 600")
        env = dotenv_values(path, interpolate=False)

        def number(key, default, integer=False, minimum=0):
            value = (int if integer else float)(env.get(key) or default)
            if not math.isfinite(value) or value < minimum:
                raise ValueError(f"Invalid {key}")
            return value

        rpc = env.get("ROBINHOOD_RPC_HTTP") or ""
        if urlsplit(rpc).scheme not in ("http", "https") or not urlsplit(rpc).hostname:
            raise ValueError("ROBINHOOD_RPC_HTTP must be an HTTP(S) URL")
        if urlsplit(rpc).hostname == "rpc.mainnet.chain.robinhood.com":
            raise ValueError("Production requires a provider RPC; public RPC is for fallback connectivity only")
        chain = number("ROBINHOOD_CHAIN_ID", 0, True, 1)
        # Only this network's Pons deployment has been researched.
        if chain != 4663:
            raise ValueError("Pons integration is verified for mainnet chain 4663 only")
        factories = tuple(to_checksum_address(a.strip()) for a in
                          (env.get("PONS_FACTORIES") or "").split(",") if a.strip())
        from app.pons import VERIFIED_FACTORIES
        if not factories or any(a not in VERIFIED_FACTORIES for a in factories):
            raise ValueError("Unverified Pons factory; verify address and ABI before adding it")
        stock_url = env.get("STOCK_ASSETS_URL") or ""
        if stock_url != "https://api.robinhood.com/rhj/assets":
            raise ValueError("Stock registry must use the verified official assets endpoint")
        ws = env.get("ROBINHOOD_RPC_WS") or ""
        fallback = env.get("ROBINHOOD_RPC_FALLBACK_HTTP") or ""
        if ws and (urlsplit(ws).scheme != "wss" or not urlsplit(ws).hostname):
            raise ValueError("ROBINHOOD_RPC_WS must be a secure WebSocket URL")
        if fallback and (urlsplit(fallback).scheme != "https" or not urlsplit(fallback).hostname):
            raise ValueError("ROBINHOOD_RPC_FALLBACK_HTTP must be HTTPS")
        result = cls(rpc, chain, factories, stock_url,
                     ROOT / (env.get("DATABASE_PATH") or "data/scanner.db"),
                     ROOT / (env.get("LOG_PATH") or "logs/scanner.log"),
                     number("POLL_SECONDS", 5, minimum=1),
                     number("RPC_TIMEOUT", 20, minimum=1),
                     number("RETRY_MAX_SECONDS", 60, minimum=2),
                     number("CONFIRMATIONS", 3, True),
                     number("OVERLAP_BLOCKS", 5, True, 1),
                     number("BATCH_BLOCKS", 50, True, 1),
                     number("BLOCK_RETENTION", 10000, True, 1),
                     number("MAX_REORG_DEPTH", 100, True, 1),
                     int(env["START_BLOCK"]) if env.get("START_BLOCK") else None,
                     number("RPC_REQUESTS_PER_SECOND", 5, minimum=0.1), ws, fallback,
                     number("RPC_MAX_ATTEMPTS", 5, True, 1),
                     number("RETRY_BASE_SECONDS", 2, minimum=1),
                     number("LOG_BLOCK_RANGE", 10, True, 1),
                     env.get("ROBINHOOD_WS_SUBSCRIPTION") or "logs",
                     os.environ.get("SCANNER_MODE") or env.get("SCANNER_MODE") or "live",
                     number("LIVE_START_OVERLAP_BLOCKS", 5, True, 1),
                     number("LIVE_MAX_RECOVERY_BLOCKS", env.get("LIVE_RECOVERY_MAX_BLOCKS") or 1000, True, 1),
                     env.get("LIVE_TRANSPORT_MODE") or "ws_first",
                     number("LIVE_MAX_STARTUP_RECOVERY_BLOCKS", 500, True, 1),
                     number("HEAD_HEALTHCHECK_SECONDS", 60, minimum=60))
        if result.batch > 500 or result.retention <= max(result.reorg_depth, result.overlap):
            raise ValueError("Batch must be <=500; retention must exceed overlap and reorg depth")
        if result.start_block is not None and result.start_block < 0:
            raise ValueError("START_BLOCK must be nonnegative")
        if result.retry_attempts > 10 or result.retry_base > result.retry_max:
            raise ValueError("RPC_MAX_ATTEMPTS must be <=10 and retry base <= maximum")
        if result.ws_subscription not in ("logs", "newHeads"):
            raise ValueError("ROBINHOOD_WS_SUBSCRIPTION must be logs or newHeads")
        if result.mode not in ("live", "backfill"):
            raise ValueError("SCANNER_MODE must be live or backfill; hybrid is disabled")
        if (env.get("BACKFILL_ENABLED") or "false").lower() != "false":
            raise ValueError("Automatic backfill is disabled; use the explicit range CLI")
        if result.mode == "live" and result.ws_subscription != "logs":
            raise ValueError("Live mode requires filtered logs subscriptions")
        if not result.confirmations <= result.live_overlap <= min(100, result.reorg_depth):
            raise ValueError("Live overlap must cover confirmations and be <=100 and reorg depth")
        if result.transport_mode not in ("ws_first", "http_complete"):
            raise ValueError("LIVE_TRANSPORT_MODE must be ws_first or http_complete")
        if result.recovery_max < result.live_overlap or result.startup_recovery_max < result.live_overlap or result.rpc_rps > 5:
            raise ValueError("Recovery bound must cover overlap; free-tier HTTP rate must be <=5")
        return result
