"""Verified integration surface, reviewed 2026-09-09.

Current factory: https://docs.ponsfamily.com/v2 (Deployed addresses).
Event: same page, Events to index; independently matches project Solidity:
https://github.com/ponsdotdev/ponsfamily/blob/main/contractsV2/src/v2/PonsV2LaunchFactory.sol
Source commit checked: 33c2281bfcf91f18ddc3e8497894ae764118ce37.
Live logs and deployed code were read successfully via official RPC.
Blockscout's API returned a Cloudflare 403; no explorer bytecode/source match is claimed.
Explorer: https://robinhoodchain.blockscout.com/address/0x7eD598BcEf8bd9Edd8C97A195C6d13f40801EC7e

Older search snapshots list 0x7E1EAbd52Ae29598e6483F72dCf1a70b14284dB8;
it is NOT the current factory. Do not enable it without separate verification.
V1 0xA5aAb3F0c6EeadF30Ef1D3Eb997108E976351feB uses a DIFFERENT event.
Native ETH is pairToken=zero. The emitted curve is not a Uniswap pool;
V4 pools only exist after graduation and use a bytes32 pool ID.
"""
import asyncio
import logging
import re

from eth_abi import decode
from eth_utils import keccak, to_checksum_address

from app.models import hash32, quantity, utc_now
from app.rpc import LogRangeError, RpcError

log = logging.getLogger(__name__)
ZERO = "0x0000000000000000000000000000000000000000"
VERIFIED_FACTORIES = {"0x7eD598BcEf8bd9Edd8C97A195C6d13f40801EC7e"}
# Observed via official eth_getCode on 2026-09-09. Pins the researched deployment;
# this is NOT a compiler reproduction or a claim of an independent contract audit.
RUNTIME_HASHES = {"0x7eD598BcEf8bd9Edd8C97A195C6d13f40801EC7e":
                  "89a27da6f703e0a7cdd4f233e7cb57604ff75b164530962d3ff7cf8483a67d84"}
EVENT_ABI = {
    "type": "event", "name": "TokenLaunched", "anonymous": False,
    "inputs": [
        {"name": "token", "type": "address", "indexed": True},
        {"name": "curve", "type": "address", "indexed": True},
        {"name": "deployer", "type": "address", "indexed": True},
        {"name": "pairToken", "type": "address", "indexed": False},
        {"name": "launchConfigId", "type": "uint256", "indexed": False},
        {"name": "graduationThreshold", "type": "uint256", "indexed": False},
    ],
}
EVENT_SIGNATURE = "TokenLaunched(address,address,address,address,uint256,uint256)"
TOPIC = "0x" + keccak(text=EVENT_SIGNATURE).hex()


def hex_data(value):
    if not isinstance(value, str) or not re.fullmatch(r"0x(?:[0-9a-fA-F]{2})*", value):
        raise ValueError("Invalid ABI hex")
    return bytes.fromhex(value[2:])


def decode_launch(event, block, factories):
    factory = to_checksum_address(event["address"])
    topics = event["topics"]
    if (factory not in factories or len(topics) != 4 or topics[0].lower() != TOPIC
            or event.get("removed", False) is not False
            or hash32(event["blockHash"]) != block.hash
            or quantity(event["blockNumber"]) != block.number):
        raise ValueError("Log does not match verified factory/event/canonical block")
    addresses = []
    for topic in topics[1:]:
        addresses.append(to_checksum_address(decode(["address"], hex_data(hash32(topic)))[0]))
    data = hex_data(event["data"])
    if len(data) != 96 or ZERO in addresses:
        raise ValueError("Invalid launch data")
    quote, config_id, threshold = decode(["address", "uint256", "uint256"], data)
    return dict(tx_hash=hash32(event["transactionHash"]), log_index=quantity(event["logIndex"]),
                block_number=block.number, block_timestamp=block.timestamp,
                factory_address=factory, token_address=addresses[0], curve_address=addresses[1],
                creator_address=addresses[2], quote_asset_address=to_checksum_address(quote),
                pair_or_pool_address=None, launch_type="pons-v2", raw_event_name="TokenLaunched",
                launch_config_id=str(config_id), graduation_threshold=str(threshold),
                detected_at=utc_now())


def metadata_text(raw):
    data = hex_data(raw)
    if len(data) > 8192:
        raise ValueError("Oversized metadata")
    text = (data.rstrip(b"\0").decode("utf-8") if len(data) == 32
            else decode(["string"], data)[0])
    return "".join(c for c in text if c.isprintable())[:256]


async def metadata(rpc, address, block_number):
    if address == ZERO:
        return {"symbol": "ETH", "name": "Ether"}
    fields = ("symbol", "name")
    calls = [("eth_call", [{"to": address, "data": "0x" + keccak(text=f"{field}()").hex()[:8]},
                           hex(block_number)]) for field in fields]
    responses = await rpc.batch(calls)
    result = {}
    for field, raw in zip(fields, responses):
        try:
            if isinstance(raw, Exception):
                raise raw
            result[field] = metadata_text(raw)
        except RpcError:
            # Provider outages/rate limits are not broken tokens. Retry the block
            # so transient failures do not become permanently missing metadata.
            raise
        except Exception as exc:
            # ABI errors and nonstandard contracts must never discard a valid launch.
            # No provider message or untrusted token string enters the log.
            log.warning("Metadata unavailable address=%s field=%s error=%s",
                        address, field, type(exc).__name__)
            result[field] = None
    return result


class Pons:
    def __init__(self, rpc, config, db):
        self.rpc, self.config, self.db = rpc, config, db
        self.log_span = config.log_span

    async def verify(self):
        for address in self.config.factories:
            code = hex_data(await self.rpc.call("eth_getCode", [address, "latest"]))
            if not code:
                raise ValueError("Verified Pons factory has no deployed bytecode")
            if keccak(code).hex() != RUNTIME_HASHES[address]:
                raise ValueError("Pons factory bytecode changed; re-verify deployment before continuing")

    async def logs(self, first, last):
        result = []
        while first <= last:
            end = min(last, first + self.log_span - 1)
            try:
                part = await self.rpc.call("eth_getLogs", [{
                    "address": list(self.config.factories), "topics": [TOPIC],
                    "fromBlock": hex(first), "toBlock": hex(end),
                }])
            except LogRangeError:
                if end == first:
                    raise
                self.log_span = max(1, (end - first + 1) // 2)
                log.warning("Provider rejected log range; reducing span to %d blocks", self.log_span)
                await asyncio.sleep(self.config.poll)
                continue
            if not isinstance(part, list):
                raise ValueError("Invalid logs response")
            result.extend(part)
            first = end + 1
        return result

    async def launches(self, events, block):
        launches = []
        for event in events:
            try:
                launch = decode_launch(event, block, self.config.factories)
            except Exception:
                log.error("Malformed launch event at block=%d; checkpoint held", block.number)
                raise ValueError("Malformed launch event") from None
            existing = self.db.launch(launch["tx_hash"], launch["log_index"])
            if (existing and existing["block_number"] == block.number
                    and all(existing[key] is not None for key in
                            ("token_symbol", "token_name", "quote_asset_symbol", "quote_asset_name"))):
                existing["is_stock_quote"] = int(self.db.is_stock(existing["quote_asset_address"]))
                launches.append(existing)
                continue
            token, quote = await asyncio.gather(
                metadata(self.rpc, launch["token_address"], block.number),
                metadata(self.rpc, launch["quote_asset_address"], block.number))
            launch.update(token_symbol=token["symbol"], token_name=token["name"],
                          quote_asset_symbol=quote["symbol"], quote_asset_name=quote["name"],
                          is_stock_quote=int(self.db.is_stock(launch["quote_asset_address"])))
            launches.append(launch)
            log.info("Launch block=%d token=%s quote=%s stock=%s tx=%s",
                     block.number, launch["token_address"], launch["quote_asset_address"],
                     bool(launch["is_stock_quote"]), launch["tx_hash"])
        return launches
