from dataclasses import dataclass
from datetime import datetime, timezone
import re


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def quantity(value):
    if not isinstance(value, str) or not re.fullmatch(r"0x(?:0|[1-9a-fA-F][0-9a-fA-F]*)", value):
        raise ValueError("Invalid RPC quantity")
    return int(value, 16)


def hash32(value):
    if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", value):
        raise ValueError("Invalid 32-byte hash")
    return value.lower()


@dataclass(frozen=True)
class Block:
    number: int
    hash: str
    parent: str
    timestamp: str
    tx_count: int

    @classmethod
    def parse(cls, raw):
        if not isinstance(raw, dict) or not isinstance(raw.get("transactions"), list):
            raise ValueError("Invalid block response")
        return cls(quantity(raw["number"]), hash32(raw["hash"]), hash32(raw["parentHash"]),
                   datetime.fromtimestamp(quantity(raw["timestamp"]), timezone.utc).isoformat(),
                   len(raw["transactions"]))
