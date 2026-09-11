# Phase 2A market model

Sources: [Pons V2 curve source](https://github.com/ponsdotdev/ponsfamily/blob/main/contractsV2/src/v2/PonsV2BondingCurve.sol),
[Pons curve math](https://github.com/ponsdotdev/ponsfamily/blob/main/contractsV2/src/v2/libraries/PonsV2BondingCurveMath.sol),
and [Uniswap V4 StateLibrary](https://github.com/Uniswap/v4-core/blob/main/src/libraries/StateLibrary.sol).

Curve price is `quoteReserve / tokenReserve`, normalized by quote decimals and the
verified Pons launcher token's 18 decimals. `quoteReserve` includes `phantomQuote`;
therefore price models Pons's actual constant-product trading reserve. It is spot price,
not an executable buy/sell quote; trade fee and creator tax change an executable quote.

`fdv_quote = price_quote * totalSupply_normalized`. Supply comes from the token's
`totalSupply()` and is cached. Curve `liquidity_quote_estimate` is `realQuoteReserve`,
the physical, fee-excluded one-sided quote reserve. `quote_reserve` remains raw tradeable
reserve for recomputation. USD fields are unavailable.

V4 pool key comes from verified `PoolGraduated` data. StateLibrary defines
`stateSlot = keccak256(poolId || bytes32(6))`; `extsload(stateSlot)` contains `sqrtPriceX96`
and `extsload(stateSlot + 3)` contains active liquidity. Raw price is
`sqrtPriceX96² / 2¹⁹²` for currency1 per currency0, then inverted when token is currency1
and normalized by decimals. Active liquidity is stored as `v4_active_liquidity` with type
`v4_active_liquidity_raw`; quote notional is deliberately NULL.

Targets persist in SQLite. A deterministic 10% address-hash cohort receives T+0 and T+5m;
its 5% subset receives long targets. This preserves an unbiased random cohort within a
free-tier budget. Target rows and snapshot unique constraints make restart/replay idempotent.
