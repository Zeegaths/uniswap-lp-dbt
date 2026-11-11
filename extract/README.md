# Extract

`extract.py` loads Uniswap V3 liquidity provider events for the WETH/USDC 0.05% pool on Base into `raw.uniswap_events` in Postgres.

Built from the lesson's Uniswap swap extractor, extended with the pool's Mint, Burn and Collect events and the position manager's IncreaseLiquidity, DecreaseLiquidity, Collect and Transfer events.

## Load the committed snapshot (no RPC needed)

```bash
uv run python extract/extract.py
```

Reads `data/raw_events.jsonl.gz`. This is the command the main README uses.

## Re-extract from the chain

Add a Base mainnet `RPC_URL` to `.env`, then:

```bash
uv run python extract/extract.py --from 53400000 --to 53443199 --write-snapshot
```

That range is 43,200 blocks, one day on Base at 2 seconds per block. It loads Postgres and, with `--write-snapshot`, rewrites `data/raw_events.jsonl.gz`. On a free Alchemy key it takes about 10 to 15 minutes.

## Incremental (what Airflow runs)

```bash
uv run python extract/extract.py --incremental
```

Starts after the last block already extracted and stops 20 blocks behind the chain tip, capped at `MAX_BLOCKS_PER_RUN` blocks per run (default 43,200). Finished ranges are recorded in `raw.extract_runs`, so empty ranges are never scanned twice. Tune with `START_BLOCK`, `CONFIRMATIONS` and `MAX_BLOCKS_PER_RUN` in `.env`.

## What it does

1. One `eth_getLogs` call per 10 blocks (`CHUNK_SIZE`), for both contracts and all 8 event types at once. Retries with backoff.
2. Keeps position manager events only in transactions that also touched the pool. The position manager serves every pool on Base.
3. Decodes each log with its ABI via `process_log`. Big integers are stored as strings so nothing loses precision.
4. Block timestamps: the real timestamp is fetched every 1,000 blocks (`ANCHOR_BLOCKS`) and interpolated in between. This stays accurate if the chain's block time changes.

## Idempotent

Primary key `(tx_hash, log_index)` with `ON CONFLICT DO NOTHING`. Run the same input twice and the second run reports `0 new`.