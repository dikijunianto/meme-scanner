"""Audit-only RPC adapter; never imported by production collectors."""
import json
from collections import Counter
from pathlib import Path
from app.rpc import Rpc, RpcError


class AuditRpc(Rpc):
    ALLOWED = Rpc.ALLOWED | {"eth_getTransactionByHash", "eth_getTransactionReceipt"}
    LIMITS = {"http": 500, "eth_getLogs": 200, "eth_call": 500,
              "eth_getTransactionByHash": 100, "eth_getTransactionReceipt": 100}

    def __init__(self, config, ledger):
        super().__init__(config)
        self.ledger = Path(ledger)
        self.counts = Counter(json.loads(self.ledger.read_text()) if self.ledger.exists() else {})

    async def _send(self, payload, method):
        members = payload if isinstance(payload, list) else [payload]
        usage = Counter(m["method"] for m in members)
        usage["http"] = 1
        if any(self.counts[k] + v > self.LIMITS.get(k, 500) for k, v in usage.items()):
            raise RpcError("Audit hard budget exhausted; stop research")
        self.counts.update(usage)
        # Persist BEFORE network I/O, so failures/retries/process exit remain counted.
        self.ledger.write_text(json.dumps(dict(self.counts), indent=2))
        return await super()._send(payload, method)
