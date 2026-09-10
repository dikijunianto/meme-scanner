# Pons V2 graduation evidence

Verified 2026-09-09 UTC from [Pons documentation](https://docs.ponsfamily.com/v2),
[factory source](https://github.com/ponsdotdev/ponsfamily/blob/8b9bf371030279133017b5c1b713823f5889c5d2/contractsV2/src/v2/PonsV2LaunchFactory.sol),
[launch state ABI](https://github.com/ponsdotdev/ponsfamily/blob/8b9bf371030279133017b5c1b713823f5889c5d2/contractsV2/src/v2/interfaces/ILaunchpadV2.sol),
and private read-only RPC. The factory source is unchanged from the Phase 1 snapshot.
The deployment runtime hash remains pinned in `app/pons.py`; no explorer source/bytecode
verification is claimed (the explorer API was unavailable).

The factory `0x7eD598BcEf8bd9Edd8C97A195C6d13f40801EC7e` emits
`LaunchSwept(address,uint256,uint256)` when reserves leave the curve. That stage alone
does not prove a pool exists. `PoolGraduated(address,uint256,uint256,uint256)` proves
the subsequent pool creation and position mint completed. Only its token is indexed;
the data words are position ID, token amount, and pairing-asset amount. Its topic is
`0x0a44ef75df69c534f43cd6c1aa3ef8983065fe5fe79ef9e79f6494e6f258c259`.

The event does not contain curve, quote, or creator. The scanner reads the factory's
`getLaunchedToken(token)` snapshot at the event block: token, curve, original deployer,
current fee recipient, quote, threshold, fee, tick spacing, creator tax, buyback flag,
phase, swept balances/time, and existence. Phase 2 in that enum means `PoolCreated`.
`creator_address` records the original deployer, not a potentially changed fee recipient.
The immutable `poolManager()` and `memeHook()` getters identify that factory's stack.

Currency addresses are sorted numerically; native ETH is address zero. The canonical
identity is `(chain_id, pool_manager_address, pool_id)`, where
`pool_id = keccak256(abi.encode(currency0, currency1, uint24 fee, int24 tickSpacing, hooks))`.
This matches the [Uniswap PoolId implementation](https://github.com/Uniswap/v4-core/blob/main/src/types/PoolId.sol).
The factory initializes the singleton manager with this key and mints a position into
its locker. A position ID is not a pool ID; there is no conventional per-pool contract
address. The [PoolManager Initialize ABI](https://github.com/Uniswap/v4-core/blob/main/src/interfaces/IPoolManager.sol)
provides independent event evidence for the key and resulting ID.

## Real historical example — LIVE VERIFIED

| Field | Value |
|---|---|
| Token | Pons X (`ponsX`), `0xb15f462B9A2204FE357EBD4624Aad858BD29a6E5` |
| Quote | Native ETH, `0x0000000000000000000000000000000000000000` |
| Curve | `0xD46f148abE8Cdba73358f8C71F85CA5aD231F914` |
| Launch block | 57,960,343 |
| Launch transaction | `0x4c5db62ef2b49926124571194de6e3921bc2ba880af89b620a88c8e77edb2bf1` |
| Graduation block | 58,013,387 |
| Graduation transaction | `0x7c27610e0be70d7ddac911cfd5056d9d982ca04d397d860b4928ecb1ab95dcf9` |
| PoolManager | `0x8366a39CC670B4001A1121B8F6A443A643e40951` |
| Hook | `0xE5e702641Ea86F4ae6cC3cDaeD2B886f976Be044` |
| Fee / tick spacing | 0 / 200 |
| PoolId | `0xd8d249b39343e9949ffdc13f1adc824e8f1757f875abb7094819d0024b62ae9a` |

The derived ID exactly matches the manager's `Initialize` event in the same transaction.
The committed fixture `tests/fixtures/graduations/ponsx.json` contains the raw graduation,
block-time factory responses, original persisted launch, and manager event. Tests independently
compare the ID, manager, fee, tick spacing, hook, curve, transaction, and duplicate storage.

Search budget and limitations: 58,723,063–58,724,062 (1,000 blocks, no graduation), then
58,013,385–58,013,614 (230 blocks, Pons X found). A stock-preferred continuation searched
58,008,615–58,013,384 (4,770 blocks): four additional graduations, all native ETH.
All 600 discovery requests used ten-block factory/topic filters, sequentially paced.
One extra single-block manager query verified Pons X. No stock-paired graduation was
found in those ranges; no claim is made that none exists elsewhere. Research stopped
at that bound. Historical launch coverage is not historical graduation completeness.

## Stock-paired example captured during production observation — LIVE VERIFIED

After deployment, the live scanner captured **Goldman Sacks (`GS`)**, paired with
the official GLD token (a tokenized ETF, included in the address-verified stock registry).
The public token name is untrusted metadata; it does not establish issuer affiliation.

| Field | Value |
|---|---|
| Token | `0x4DC46Fd943b9A46306a9278c27F7047Dd676A47A` |
| Quote | GLD, `0xC9a981FEE1F9DEc688bb123ccDeCc63D0deBFC4e` |
| Curve | `0x10ECCcCFd0EdF316981937ed61D0261483aE4f03` |
| Launch block / transaction | 58,732,651 / `0xb10c44b9f4e669229daa7d1e4d0fa4bdb9ed6cc5ab1e2941e85bfedd837b47ec` |
| Graduation block / transaction | 58,732,690 / `0x4f8f6cafc857018beb6c06e5d4c1ea8bb3436ab15047792b9f820ec3a55a8f49` |
| PoolId | `0x48a77bcb3f66c6653fc32d9d6012dd29f9a4851862a210422d5700f65bf94df6` |
| Manager / hook / fee / tick spacing | Same verified manager and hook as above / 0 / 200 |

Two additional single-block filtered log reads retrieved its graduation and matching
manager `Initialize`, plus one factory-state batch and one header read. This was
verification of an already observed event, not an expanded historical search.
`tests/fixtures/graduations/gld.json` preserves the raw evidence. Both token/quote
currency ordering and the derived pool ID are verified; no conventional pool address
is stored. Live graduations whose launches lie inside the acknowledged gap remain
valid graduation records with an unresolved launch transaction, rather than an invented link.
