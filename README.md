# Uniswap V3 LP PnL on Base

Do liquidity providers actually make money? This project pulls every liquidity action from the Uniswap V3 **WETH/USDC 0.05%** pool on Base, loads it into Postgres, and uses dbt to work out realized profit or loss for each LP position.

It follows the method of [Urusov et al. (2026), *Liquidity provision in CLMMs: evidence from transactions data*](https://arxiv.org/abs/2604.22069), which found that only about 1 in 6 LPs avoided losses on Base WETH/USDC pools.

## The dataset

| | |
|---|---|
| Chain | Base (chain id 8453) |
| Pool | `0xd0b53d9277642d899df5c87a3966a349a798f224` (Uniswap V3 WETH/USDC 0.05%) |
| Position manager | `0x03a520b32c04bf3beef7beb72e919cf822ed34f1` (Uniswap V3 NFT positions) |
| Block range | 53400000 to 53443199 (one day) |
| Events | Pool: Swap, Mint, Burn, Collect. Position manager: IncreaseLiquidity, DecreaseLiquidity, Collect, Transfer |
| Snapshot | `data/raw_events.jsonl.gz`, committed so the pipeline runs without an RPC |

## Prerequisites

- Docker with Docker Compose
- [uv](https://docs.astral.sh/uv/)
- Git

## Run from scratch

```bash
git clone https://github.com/Zeegaths/uniswap-lp-dbt.git && cd uniswap-lp-dbt
cp .env.example .env              # set DB_PASS, never commit .env
export UV_ENV_FILE=.env           # lets uv run pass .env to the extractor and dbt
docker compose up -d              # Postgres on localhost:5433
uv sync                           # dependencies
uv run python extract/extract.py  # snapshot → raw.uniswap_events
uv run dbt build                  # models + tests, all green
```

`UV_ENV_FILE` is needed because dbt reads its connection from environment variables (see `profiles.yml`) and does not read `.env` by itself.

To pull fresh data from the chain instead of the snapshot, see [extract/README.md](extract/README.md). To run it on a schedule, see [Orchestration with Airflow](#orchestration-with-airflow).

## Models

```
raw.uniswap_events
  └─ staging        cast + rename, one model per event type
       stg_swaps, stg_pool_mints,
       stg_position_increases, stg_position_decreases,
       stg_position_collects, stg_position_transfers
  └─ intermediate   decisions
       int_pool_prices       sqrtPriceX96 → USDC per WETH
       int_lp_positions      which NFT positions belong to this pool, owner, price range
       int_position_events   deposits, withdraws, collects priced at the moment they happened
       int_position_pnl      realized PnL, status, outcome, entry location
  └─ marts
       fct_lp_positions      one row per LP position
```

`raw.extract_runs` is bookkeeping for the incremental extractor and is not modeled.

### Grain of `fct_lp_positions`

One row per Uniswap V3 NFT liquidity position opened in the pool during the block range.

### Key decisions (all in the intermediate layer)

- **Pool match.** A position NFT belongs to the pool when its first IncreaseLiquidity sits in the same transaction as a pool Mint from the position manager with identical liquidity and amounts. That Mint gives the tick range.
- **Owner.** The address that received the NFT when it was minted.
- **Price.** The pool price after the most recent earlier swap.
- **Realized PnL.** Collects (principal plus fees) valued at collect time, minus deposits valued at deposit time, in USDC. Same capital in vs capital out approach as the paper.
- **Status.** `closed` when all liquidity was removed and collected afterwards, otherwise `open`.
- **Outcome.** `win`, `loss` or `breakeven` (within 1 cent) for closed positions, `open` for the rest.
- **Entry location.** `price_below_range`, `price_in_range` or `price_above_range`, from the pool tick at the first deposit.

### Tests

Every column in every model has `not_null`. IDs that should be unique have `unique`. Derived columns have `accepted_values`: `event_type`, `entry_location`, `position_status`, `outcome`.

## Try it

```sql
-- Share of closed positions that made money
select outcome, count(*), round(100.0 * count(*) / sum(count(*)) over (), 1) as pct
from fct_lp_positions
where position_status = 'closed'
group by outcome;
```

## Orchestration with Airflow

An Airflow 3 DAG runs the whole pipeline on a schedule:

```
extract_events ──▶ dbt_run ──▶ dbt_test ──▶ report
```

| Task | What it runs |
|---|---|
| `extract_events` | `extract/extract.py` in the mode picked at trigger time: `incremental` (default), `range` or `snapshot` |
| `dbt_run` | `dbt run`: staging, intermediate, marts |
| `dbt_test` | `dbt test` as its own task, so a data failure shows up red |
| `report` | `orchestration/report.py`: win, loss and open counts from `fct_lp_positions` |

Schedule: daily at 03:17 UTC. Each run picks up from the last extracted block, so positions opened one day and closed the next get their final PnL.

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env   # Linux: keeps files Airflow writes owned by you
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d --build
```

Open http://localhost:8080 and log in with `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` from `.env`. Unpause `uniswap_lp_pipeline`, then trigger it. Pick `snapshot` mode for a run with no RPC, or `incremental` to pull new blocks (needs `RPC_URL`).

Files: `orchestration/dags/lp_pipeline.py`, `orchestration/Dockerfile`, `docker-compose.airflow.yml`.

## Limitations

- Only NFT positions through the Uniswap position manager. Liquidity added by other contracts is skipped.
- Positions opened before the range drop out, because their NFT mint isn't in the data.
- For open positions, `realized_pnl_usdc` is only cash in vs cash out so far. They are not marked to market.
- Gas costs are not included.
