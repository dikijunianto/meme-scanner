# Preflight B: deployed trade semantics

**Gate: READY for a subsequent Phase 2B raw-flow implementation with the definitions
below. No collection was implemented or enabled.** Audit date: 2026-09-20 UTC.

Run `python scripts/phase2b_preflight.py` to revalidate the committed evidence
offline. This command neither opens the production database nor makes RPC calls.
It is not a fresh live-chain health check. Fixtures are under
`tests/fixtures/phase2b/`; transaction input, raw logs, receipts, state reads,
block anchors, decoded values and model comparisons are retained. Public
transaction signatures are on-chain evidence, not private keys.

## Independent classifications

| Question | Result | Evidence class |
|---|---|---|
| Curve source/deployment equivalence | STRONGLY_SUPPORTED, not exact compilation verification | SOURCE + BEHAVIOR + runtime structure |
| Deployed buy behavior | VERIFIED for sampled trades | BEHAVIORALLY VERIFIED |
| Deployed sell behavior | VERIFIED for sampled trades | BEHAVIORALLY VERIFIED |
| Curve event semantics | VERIFIED | receipts and transfers |
| V4 direction and pool amounts | VERIFIED | receipts, PoolKey and transfers |
| Actor field meanings | VERIFIED; economic actor UNKNOWN where intermediary | source and transaction paths |
| Phase 2A curve price | VALID_MARGINAL_PRICE | historical execution and tiny eth_call |
| Phase 2B gate | READY | bounded scope, explicit identities and amount semantics |

This sign-off is for the observed deployment/runtime family. It does not prove
every protocol branch, all future deployments, or universal actor attribution.

## Source and deployment investigation

Authoritative Pons source is pinned to
[162310fbd1217717e2f5e4cde794d6a11322b469](https://github.com/ponsdotdev/pons-labs/tree/162310fbd1217717e2f5e4cde794d6a11322b469/contractsV2/src/v2).
`PonsV2LaunchDeployer.deployLaunch` uses `new PonsV2BondingCurve(...)` and then
deploys a token; the inspected V2 helper uses ordinary CREATE, not an EIP-1167
clone or a salted CREATE2 expression. Constructor inputs include quote asset,
factory, fee policy, fee recipients/shares, phantom reserve, fee/tax and threshold.
Several become immutables.

Six sampled runtimes are 10,229 bytes. They share a dispatch prefix, the same
Solidity CBOR metadata suffix, and an opcode skeleton; no DELEGATECALL opcode was
found in the executable-byte linear disassembly. This supports full per-curve
contracts, not minimal proxies. Runtime hashes differ. We did **not** strip
arbitrary differences or claim the PUSH-operand-free skeleton is equivalence.
`runtime_structure.json` records the limited structural comparison.

The CBOR compiler bytes are `00 08 23` (0.8.35), and its IPFS multihash is
`Qme3pW7ziRxcajUhZUm8QUc5Sc7cD1boFLxaZ7s1qqRQQ4`. The metadata retrieval attempt
was unsuccessful. The official repository's `contract-meta.json` explicitly
belongs to the V1 factory: its 0.8.30/300-runs settings cannot be reused for V2.
No exact V2 optimizer/viaIR/EVM settings or immutable-reference build output were
found. Therefore no guessed compilation, immutable-byte normalization, or exact
source-bytecode match is claimed. Behavioral verification supplies the independent
evidence permitted by the request.

Searches covered curve, factory, deployer, interfaces, libraries and hook in the
official source tree. Pricing helpers `getAmountOut` and `quoteAmountOut` are
internal library functions; `previewLaunchEconomics` is a launch-terms helper,
not an executable trade quoter. No deployed public quoter was established.

## Pons CurveBuy

Signature: `CurveBuy(address,address,uint256,uint256,uint256,uint256)`

Topic0: `0xec36bf571f136799e8dc0b0b8bea4b04d8bd3d43de838aab0d5fc21d4cbfc455`

Emitter: the launch's curve contract. Indexed: `buyer: address`,
`recipient: address`. Data words: `quoteIn, tokensOut, fee, tax`, all `uint256`.

Verified interpretation:

- `buyer` is the immediate curve caller/payer. It can be an intermediary.
- `recipient` receives the launch token; it can differ from caller or tx.from.
- `quoteIn` is credited gross quote **spent**, including the two fee legs.
  It excludes a partial-fill refund and can differ from the user's router input.
- `tokensOut` is launch-token output to recipient.
- `fee` is the base fee pot, later split among beneficiaries; it is not exclusively
  protocol revenue. `tax` is the separate creator tax.
- Pricing input is `quoteIn-fee-tax`. Do not call it the same quantity as gross input.

Three ordinary buy fixtures match token output, base fee, creator tax, and both
asset transfer legs exactly: SLV, SPCX and COST quotes. Two additional graduation
boundary buys match after applying reserved allocation, gross-up rounding and
refunds. All expected-versus-observed integer deltas are **0 (0%)**.

## Pons CurveSell

Signature: `CurveSell(address,address,uint256,uint256,uint256,uint256)`

Topic0: `0x8113d738abdcb6b38357e9d53a54a7157861a09031b453651f0fe7fe151f59df`

Emitter: curve. Indexed: `seller: address`, `recipient: address`. Data:
`tokensIn, quoteOut, fee, tax`, all `uint256`.

`seller` supplies launch tokens to the curve. `recipient` receives quote.
`tokensIn` is gross token input; `quoteOut` is quote output **net** of base fee
and creator tax. Gross priced quote output is `quoteOut+fee+tax`. Three different
NVDA-paired curves match output, both fee legs and the actual asset transfers
exactly: **0 raw-unit delta (0%)** for every comparison.

For all six ordinary fixtures, state was read at B-1 and no earlier curve-emitted
log exists in B before the selected trade. Parent/block hashes are recorded.
This establishes a price-relevant pre-state under the verified curve behavior;
it is not a full transaction-state reconstruction. Earlier approvals, router
state or timestamp differences can still affect replay feasibility.

The three original buy router transactions replayed successfully at B-1. The
three selected original sell replays reverted. Their specific revert causes are
unresolved; successful historical receipts/transfers and exact pricing matches
remain independent evidence. Reverts are retained, not counted as successful quotes.

## Caller versus beneficiary

In `curve_buy_2`, caller `0x9689992f5b5c09447f15906d8d11214944488341` differs
from recipient/tx.from `0x666bf9a4e5086204be436ab37111ae20b821f3c8`.
Other fixtures route both curve caller and recipient through
`0x65050a9b7e5075a2ba5ced7b1b64ee66262c40dc`, while tx.from differs.
Thus neither field is universally an end-user wallet. Raw collection can preserve
these fields without a transaction lookup per event. End-user wallet statistics
must not substitute router counts silently.

## Uniswap V4 Swap

Signature: `Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)`

Topic0: `0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f`

Emitter: `0x8366a39CC670B4001A1121B8F6A443A643e40951`. Indexed: PoolId and
sender. Data: signed `amount0`, signed `amount1`, `sqrtPriceX96`, liquidity,
tick and fee. PoolId is recomputed from sorted currency0/1, fee, tickSpacing and
hooks; both real fixtures match the previously recorded PoolKey.

Verified rule: positive launched-token delta means token output from the pool;
negative means token input. Opposite quote sign is required.

| Launched token | amount0 | amount1 | Direction |
|---|---:|---:|---|
| currency0 | positive | negative | buy |
| currency0 | negative | positive | sell |
| currency1 | negative | positive | buy |
| currency1 | positive | negative | sell |

Zero or same-sign deltas are not silently classified. Native and ERC20 quotes
use the same sign rule. Both fixture PoolIds are stock-paired; native/order
permutations are TEST VERIFIED, not additional native-chain fixtures.

Pinned [PoolManager source](https://github.com/Uniswap/v4-core/blob/46c6834698c48bc4a463a86d8420f4eb1d7f3b75/src/PoolManager.sol)
emits the core swap result before afterSwap hook adjustments. Receipt settlement
corroborates the sign convention. Sender is the immediate PoolManager caller;
fixture senders `0x8876789976decbfcbbbe364623c63652db8c0904` and
`0xf1b4f5eed918327150310c37cb6c2b85739c5726` differ from tx.from. Store
`swap_sender`; leave economic actor unknown without separately proven attribution.

## Hook effects are material

Pons hook: `0xE5e702641Ea86F4ae6cC3cDaeD2B886f976Be044`.
[Pinned hook implementation](https://github.com/ponsdotdev/pons-labs/blob/162310fbd1217717e2f5e4cde794d6a11322b469/contractsV2/src/v2/hooks/PonsV2MemeHook.sol)
charges the unspecified leg, which can be token-denominated. HookFeeCollected
and actual manager-to-hook transfers agree in both fixtures:

- Buy: core token output 7400981952089628265365696; hook takes
  148019639041792565307312 tokens; remaining pool output
  7252962313047835700058384. The transaction also contains a curve/graduation
  leg, so the end-user's total receipt is not this one Swap amount.
- Sell: core quote output 1153143725475528489; hook takes
  11531437254755284 quote units; remaining output 1141612288220773205.

Both emitted V4 fee fields are zero despite these hook charges. Do not interpret
that field as all-in fee. Hook internal conversions/buybacks may themselves emit
swaps; a swap is not proof of an external user's action.

## Future normalized fields — recommendation only

Preserve `caller_address`, `recipient_address`, `swap_sender`, optional
`transaction_from`, and nullable `economic_actor` separately. Keep token/quote
identity, raw amounts, direction, venue, PoolId, transaction/log identity and phase.

For curves, retain credited quote spent, net pricing input/output, gross priced
output, base fee and creator tax with explicit names. For V4, preserve signed
core deltas as `pool_swap_delta_before_hook`; record hook amount and its currency
separately when available. Do not equate core output with beneficiary-net output,
and do not sum incompatible curve/V4 quote-flow metrics under one volume label.
No schema has been implemented in this audit.

## Evidence inventory

| Fixture | Transaction |
|---|---|
| curve_buy_1 | `0xd2e58a450204eda8acf4f869e1d4d61919008fe381ea1ca4ccce438a54201da5` |
| curve_buy_2 | `0x92197852dd2f9eaccdfb6cfd0bed834ccc6c8a63a41a457ffdf88ecae675fd62` |
| curve_buy_3 | `0x078f520810f9c136408276e9fa17acbf57d642d4262b0b8e01275a133be27606` |
| curve_sell_1 | `0x33501c9e9c915c6900f7887f21188a659e9041cd8a6a449e6b36785db5d02aed` |
| curve_sell_2 | `0x143b5542cc52edf5b996594717da6790b06f2e6363d3ee004d02f0c7d69a189d` |
| curve_sell_3 | `0xfc49c1d8be47ca37e28e788ca5c0cc55cac532ce23c0b1f15c7a07090ec721de` |
| v4_buy | `0x6a3da8c05d39babbfde563a5f8d80200263297b788bd45aaf79162df02735b8c` |
| v4_sell | `0x20754cb549c91c7634268e5e181e36bb63ae324726f283543f9e73bdaf750952` |

## Audit cost and production safety

292 HTTP attempts: 106 getLogs, 91 eth_call, 57 transaction lookups, 15 receipts,
6 getCode, 16 block headers and 1 chain ID check. No debug/trace, subscriptions,
signing, transaction submission or paid service. Queries used known curve
addresses or a known PoolManager plus PoolId, in windows of at most 10 blocks.
The audit adapter persisted attempted calls before sending and enforced ceilings
of 500 envelopes, 200 getLogs, 500 eth_call, and 100 each transaction/receipt calls.
One audit process used the ledger at a time. Do not run concurrent processes
against that ledger. Production RPC settings were not changed.

Baseline: branch main, clean at `60ee04e6388b06626233db80b12b7ac9b6a941ff`;
40 local tests and 37 production tests passed before changes. Production is a
deployed file tree, not a Git checkout. SQLite integrity was ok, WAL active,
DB 174133248 bytes, WAL 4243632 bytes, 31 GiB disk available, service memory
104136704 bytes. Existing collector PID 842555, NRestarts 0.

The audit used read-only SQLite connections and made no production DB writes.
The running collector continued writing normally; “unchanged” does not mean the
live database file or row counts stayed frozen. Production app/service-file
hashes were checked against the baseline. No migration, configuration edit,
sampling change, deployment or restart was performed.

Final checks: **53 local tests passed**. Service remained active with PID 842555
and NRestarts 0; memory 106164224 bytes (~101 MiB). SQLite integrity remained ok,
WAL 4243632 bytes. Live collector counts advanced to 193713 launches, 2496
graduations, 12591 snapshots and 40602 targets; DB size 175616000 bytes. The
latest one-hour production report recorded connected WS, 299 processed launch
events, four completed market snapshots, 26 HTTP envelopes and no recorded 429
counter. Audit RPC calls are separately accounted above. This is a spot-check,
not a claim of continuous observation throughout the audit.
