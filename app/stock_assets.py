"""Canonical address registry: https://docs.robinhood.com/chain/stock-token-apis/.

The API lists RHJ Stock Tokens (including ETFs), not direct equity ownership.
Only deployments on the configured chain qualify; names never establish identity.
"""
import json
import httpx
from eth_utils import to_checksum_address
from app.models import utc_now
from app.pons import ZERO


def parse_assets(payload, chain_id, source):
    if not isinstance(payload, dict) or not isinstance(payload.get("assets"), list):
        raise ValueError("Invalid official asset registry")
    if payload.get("next") or payload.get("nextPageToken"):
        raise ValueError("Registry is paginated; refusing partial replacement")
    rows = {}
    for asset in payload["assets"]:
        if not isinstance(asset, dict) or not isinstance(asset.get("deployments"), list):
            raise ValueError("Invalid registry entry")
        for deployment in asset["deployments"]:
            if type(deployment.get("chainId")) is not int:
                raise ValueError("Invalid registry chain ID")
            if deployment["chainId"] != chain_id:
                continue
            address = to_checksum_address(deployment["contractAddress"])
            symbol, name = asset["tokenSymbol"], asset["tokenName"]
            if address == ZERO or not isinstance(symbol, str) or not isinstance(name, str) or not symbol or not name:
                raise ValueError("Invalid asset identity")
            row = (address, symbol, name, symbol, source, 1, utc_now())
            if address in rows and rows[address][1:4] != row[1:4]:
                raise ValueError("Conflicting asset identities")
            rows[address] = row
    if not rows:
        raise ValueError("Empty chain registry; keeping previous snapshot")
    return list(rows.values())


async def sync_assets(config, db):
    try:
        async with httpx.AsyncClient(timeout=config.timeout) as client:
            async with client.stream("GET", config.stock_url) as response:
                response.raise_for_status()
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > 4 * 1024 * 1024:
                        raise ValueError("Asset registry exceeds size limit")
        rows = parse_assets(json.loads(body), config.chain_id, config.stock_url)
    except Exception as exc:
        raise ValueError(f"Official asset registry sync failed ({type(exc).__name__}); snapshot retained") from None
    db.replace_assets(rows, config.stock_url)
    return len(rows)
