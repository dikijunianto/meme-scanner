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

from eth_abi import decode, encode
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
    def __init__(self, rpc, config, db, telemetry=None):
        self.rpc, self.config, self.db = rpc, config, db
        self.telemetry = telemetry
        self.log_span = config.log_span
        self.include_graduations = False

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
                    "address": list(self.config.factories),
                    "topics": [EVENT_TOPICS if self.include_graduations else TOPIC],
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
            if event["topics"][0].lower() == GRADUATION_TOPIC:
                continue
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
            if self.telemetry:
                self.telemetry.add("metadata_calls", 2 + (2 if launch["quote_asset_address"] != ZERO else 0))
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

    async def graduations(self, events, block):
        result = []
        for event in events:
            if event["topics"][0].lower() != GRADUATION_TOPIC:
                continue
            existing = self.db.conn.execute("SELECT * FROM graduations WHERE tx_hash=? AND log_index=?",
                                            (hash32(event["transactionHash"]), quantity(event["logIndex"]))).fetchone()
            if existing:
                row = dict(existing)
                row.pop("id")
                # Validate replay identity against the current canonical header too.
                validate_graduation(event, block, self.config.factories)
                if row["block_number"] != block.number:
                    raise ValueError("Graduation identity moved without reorg reconciliation")
                result.append(row)
                continue
            token = validate_graduation(event, block, self.config.factories)
            calls = [("eth_call", [{"to": event["address"], "data": "0x" + keccak(text=sig).hex()[:8]
                       + (encode(["address"], [token]).hex() if sig == "getLaunchedToken(address)" else "")},
                       hex(block.number)]) for sig in ("getLaunchedToken(address)", "poolManager()", "memeHook()")]
            state, manager, hook = await self.rpc.batch(calls)
            result.append(decode_graduation(event, block, self.config.factories, state, manager, hook))
        return result


GRADUATION_SIGNATURE = "PoolGraduated(address,uint256,uint256,uint256)"
GRADUATION_TOPIC = "0x" + keccak(text=GRADUATION_SIGNATURE).hex()
EVENT_TOPICS = [TOPIC, GRADUATION_TOPIC]
LAUNCH_STATE_TYPES = ["address"] * 5 + ["uint256", "uint24", "int24", "uint16", "bool", "uint8",
                                                  "uint256", "uint256", "uint256", "bool"]


def validate_graduation(event, block, factories):
    if (to_checksum_address(event["address"]) not in factories or len(event["topics"]) != 2
            or event["topics"][0].lower() != GRADUATION_TOPIC or event.get("removed", False) is not False
            or hash32(event["blockHash"]) != block.hash or quantity(event["blockNumber"]) != block.number
            or len(hex_data(event["data"])) != 96):
        raise ValueError("Invalid graduation event or canonical block")
    token = to_checksum_address(decode(["address"], hex_data(hash32(event["topics"][1])))[0])
    if token == ZERO:
        raise ValueError("Zero graduation token")
    return token


def decode_graduation(event, block, factories, state_raw, manager_raw, hook_raw):
    token = validate_graduation(event, block, factories)
    state = decode(LAUNCH_STATE_TYPES, hex_data(state_raw))
    addresses = [to_checksum_address(a) for a in state[:5]]
    manager = to_checksum_address(decode(["address"], hex_data(manager_raw))[0])
    hook = to_checksum_address(decode(["address"], hex_data(hook_raw))[0])
    if (addresses[0] != token or not state[14] or state[10] != 2 or state[7] <= 0
            or ZERO in (addresses[1], addresses[2], manager, hook) or token == addresses[4]):
        raise ValueError("Factory state contradicts graduation")
    currency0, currency1 = sorted((token, addresses[4]), key=lambda a: int(a, 16))
    pool_id = "0x" + keccak(encode(["address", "address", "uint24", "int24", "address"],
                                   [currency0, currency1, state[6], state[7], hook])).hex()
    position, tokens, quote = decode(["uint256"] * 3, hex_data(event["data"]))
    return dict(tx_hash=hash32(event["transactionHash"]), log_index=quantity(event["logIndex"]),
                block_number=block.number, block_timestamp=block.timestamp,
                factory_address=to_checksum_address(event["address"]), token_address=token,
                quote_asset_address=addresses[4], curve_address=addresses[1], creator_address=addresses[2],
                pool_id=pool_id, pool_manager_address=manager, currency0=currency0, currency1=currency1,
                fee=state[6], tick_spacing=state[7], hooks=hook, position_id=str(position),
                token_amount=str(tokens), quote_amount=str(quote), raw_event_name="PoolGraduated", detected_at=utc_now())
