# Deployed price validation — VALID_MARGINAL_PRICE

Preflight B resolves the behavioral gap from Preflight A. For the sampled Pons
runtime family, Phase 2A's normalized Q/T is a **pre-fee marginal market price**.
Its horizon ratios are **MARGINAL MARKET PRICE MULTIPLES**, not realized trader
ROI, executable buy-to-sell PnL, or fee-adjusted strategy returns.

Evidence: three ordinary historical buys and three sells on six curves match
Solidity integer output, base-fee rounding and creator-tax rounding exactly.
Corresponding Transfer events match both input/output legs. Two more buys at
graduation match reserved-allocation and refund arithmetic exactly. Every model
versus actual delta is zero raw units (0%). See
[trade evidence and scope](phase2b-trade-semantics.md).

## Independent tiny execution checks

These are actual deployed `eth_call` executions at historical blocks when the
curves were active, not formulas substituted for calls or current-head quotes.
Existing direct transaction senders were used with their historical balances
and approvals. No state overrides, funding, token approvals, keys or broadcasts
were used. Each call and result is in `tiny_simulations.json`.

At three distinct curves, inputs were one millionth of the relevant reserve.
Four calls succeeded: buys on two curves, sells on two curves. Two other calls
reverted under the selected actor/state and remain recorded as failures. Their
specific revert reasons are not established; they are not claimed as successful
execution or hidden from the report.

| Curve prefix | Buy adjusted difference | Sell adjusted difference |
|---|---:|---:|
| 0x8CCDe747 | +0.000099% | -0.000100% |
| 0x14FFbb0c | +0.000098% | reverted |
| 0x6EEb1d7d | reverted | -0.000100% |

All four returned amounts exactly equal independently calculated integer model
outputs. Token and quote decimals were read at the same pinned block. The quoted
relative differences compare fee-adjusted execution price against normalized
Q/T; they reflect finite trade size and rounding, not an unexplained divergence.

With combined fee fraction f, tiny buy execution price approaches P/(1-f),
while tiny sell execution price approaches P*(1-f). Therefore a flat marginal
price need not produce a flat round-trip result. Larger sizes also incur price
impact; virtual reserves are not physically withdrawable liquidity. Remaining
allocation, actual balances, approvals and transfer restrictions can prevent an
otherwise mathematically quotable trade from executing.

## Historical interpretation

No historical snapshots were changed. The evidence supports their existing
curve-price interpretation as a marginal-price study for the verified runtime
family. It does not retrospectively certify every snapshot's timing, coverage,
FDV supply assumption or outcome-report implementation. Those are separate
questions from the price semantics audited here.

Exact V2 source compilation remains unavailable because the deployment build
settings/artifacts were not recovered. Classification is STRONGLY_SUPPORTED
source equivalence and BEHAVIORALLY VERIFIED sampled execution, not exact
bytecode equivalence. This limitation is permitted by the behavioral acceptance
path in the request and does not invalidate the observed pricing matches.

**Gate: READY for the next raw-flow implementation phase, using explicit event
identities and phase-specific amount semantics. No flow collection was deployed.**
