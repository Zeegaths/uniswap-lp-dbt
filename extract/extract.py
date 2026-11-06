"""
Extract Uniswap V3 liquidity provider activity for one pool on Base into Postgres.

Two modes:

  Snapshot (default, no RPC needed):
      uv run python extract/extract.py
      uv run python extract/extract.py --snapshot data/raw_events.jsonl.gz

  Live from an RPC, fixed range (add --write-snapshot to refresh the snapshot):
      uv run python extract/extract.py --from 53400000 --to 53443199 --write-snapshot

  Live from an RPC, continue from the last loaded block (what Airflow runs):
      uv run python extract/extract.py --incremental

Every row lands in raw.uniswap_events with primary key (tx_hash, log_index)
and ON CONFLICT DO NOTHING, so re-running the same input never duplicates rows.
"""
import argparse
import gzip
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("extract")

# ---------- Config ----------

CHAIN_ID = 8453  # Base
POOL = os.getenv("POOL_ADDRESS", "0xd0b53d9277642d899df5c87a3966a349a798f224").lower()
POSITION_MANAGER = os.getenv(
    "POSITION_MANAGER_ADDRESS", "0x03a520b32c04bf3beef7beb72e919cf822ed34f1"
).lower()
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
DEFAULT_SNAPSHOT = ROOT / "data" / "raw_events.jsonl.gz"

# Incremental mode (Airflow): where to start on an empty DB, how far behind
# the tip to stop, and the most blocks one run will pull (43,200 = one day).
START_BLOCK = int(os.getenv("START_BLOCK", "53400000"))
CONFIRMATIONS = int(os.getenv("CONFIRMATIONS", "20"))
MAX_BLOCKS_PER_RUN = int(os.getenv("MAX_BLOCKS_PER_RUN", "43200"))

DB = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASS"),
    dbname=os.environ.get("DB_NAME"),
)

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.uniswap_events (
    chain_id         INTEGER     NOT NULL,
    contract_address TEXT        NOT NULL,
    event_name       TEXT        NOT NULL,
    block_number     BIGINT      NOT NULL,
    block_timestamp  TIMESTAMPTZ NOT NULL,
    tx_hash          TEXT        NOT NULL,
    log_index        INTEGER     NOT NULL,
    args             JSONB       NOT NULL,
    PRIMARY KEY (tx_hash, log_index)
);
-- One row per finished live extraction. The incremental watermark reads this,
-- so block ranges with no events are not scanned again.
CREATE TABLE IF NOT EXISTS raw.extract_runs (
    from_block  BIGINT      NOT NULL,
    to_block    BIGINT      NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (from_block, to_block)
);
"""

INSERT_SQL = """
INSERT INTO raw.uniswap_events
    (chain_id, contract_address, event_name, block_number,
     block_timestamp, tx_hash, log_index, args)
VALUES %s
ON CONFLICT (tx_hash, log_index) DO NOTHING
RETURNING tx_hash
"""

COLUMNS = ["chain_id", "contract_address", "event_name", "block_number",
           "block_timestamp", "tx_hash", "log_index", "args"]


# ---------- ABIs ----------

def _event(name, inputs):
    return {"anonymous": False, "name": name, "type": "event",
            "inputs": [{"indexed": i, "name": n, "type": t} for n, t, i in inputs]}


POOL_ABI = [
    _event("Swap", [("sender", "address", True), ("recipient", "address", True),
                    ("amount0", "int256", False), ("amount1", "int256", False),
                    ("sqrtPriceX96", "uint160", False), ("liquidity", "uint128", False),
                    ("tick", "int24", False)]),
    _event("Mint", [("sender", "address", False), ("owner", "address", True),
                    ("tickLower", "int24", True), ("tickUpper", "int24", True),
                    ("amount", "uint128", False), ("amount0", "uint256", False),
                    ("amount1", "uint256", False)]),
    _event("Burn", [("owner", "address", True), ("tickLower", "int24", True),
                    ("tickUpper", "int24", True), ("amount", "uint128", False),
                    ("amount0", "uint256", False), ("amount1", "uint256", False)]),
    _event("Collect", [("owner", "address", True), ("recipient", "address", False),
                       ("tickLower", "int24", True), ("tickUpper", "int24", True),
                       ("amount0", "uint128", False), ("amount1", "uint128", False)]),
]

POSITION_MANAGER_ABI = [
    _event("IncreaseLiquidity", [("tokenId", "uint256", True), ("liquidity", "uint128", False),
                                 ("amount0", "uint256", False), ("amount1", "uint256", False)]),
    _event("DecreaseLiquidity", [("tokenId", "uint256", True), ("liquidity", "uint128", False),
                                 ("amount0", "uint256", False), ("amount1", "uint256", False)]),
    _event("Collect", [("tokenId", "uint256", True), ("recipient", "address", False),
                       ("amount0", "uint256", False), ("amount1", "uint256", False)]),
    _event("Transfer", [("from", "address", True), ("to", "address", True),
                        ("tokenId", "uint256", True)]),
]


# ---------- Helpers ----------

def with_retry(fn, *args, **kwargs):
    """Call fn, retrying with exponential backoff (2s, 4s, 8s...)."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt == MAX_RETRIES:
                log.error("Giving up after %d attempts: %s", attempt, e)
                raise
            wait = 2 ** attempt
            log.warning("Attempt %d/%d failed: %s. Retrying in %ds", attempt, MAX_RETRIES, e, wait)
            time.sleep(wait)


def connect():
    missing = [k for k in ("DB_USER", "DB_PASS", "DB_NAME") if not os.environ.get(k)]
    if missing:
        sys.exit(f"Missing {', '.join(missing)}. Copy .env.example to .env and fill it in.")
    conn = psycopg2.connect(**DB)
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    return conn


def load(conn, rows):
    if not rows:
        return 0
    values = [
        (r["chain_id"], r["contract_address"], r["event_name"], r["block_number"],
         r["block_timestamp"], r["tx_hash"], r["log_index"], json.dumps(r["args"]))
        for r in rows
    ]
    with conn.cursor() as cur:
        inserted = execute_values(cur, INSERT_SQL, values, page_size=1000, fetch=True)
    conn.commit()
    return len(inserted)


def clean_args(args):
    """JSON-safe args: big ints as strings (no precision loss), addresses lowercase."""
    out = {}
    for k, v in args.items():
        if isinstance(v, bool):
            out[k] = v
        elif isinstance(v, int):
            out[k] = str(v)
        elif isinstance(v, str):
            out[k] = v.lower()
        else:
            out[k] = str(v)
    return out


# ---------- Snapshot mode ----------

def run_snapshot(path):
    path = Path(path)
    if not path.exists():
        sys.exit(f"Snapshot not found: {path}. Run a live extraction first (see extract/README.md).")
    conn = connect()
    total = inserted = 0
    batch = []
    with gzip.open(path, "rt") as f:
        for line in f:
            batch.append(json.loads(line))
            if len(batch) == 5000:
                inserted += load(conn, batch)
                total += len(batch)
                batch = []
    inserted += load(conn, batch)
    total += len(batch)
    conn.close()
    log.info("Snapshot %s: %d rows read, %d new, %d already in DB",
             path.name, total, inserted, total - inserted)


# ---------- Live mode ----------

def incremental_range(w3):
    """Watermark: start after the highest block already loaded, stop near the chain tip."""
    conn = connect()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT GREATEST(
                (SELECT MAX(to_block) FROM raw.extract_runs),
                (SELECT MAX(block_number) FROM raw.uniswap_events))
        """)
        last = cur.fetchone()[0]
    conn.close()
    start = last + 1 if last is not None else START_BLOCK
    tip = with_retry(lambda: w3.eth.block_number) - CONFIRMATIONS
    end = min(tip, start + MAX_BLOCKS_PER_RUN - 1)
    log.info("Watermark: DB at block %s, chain tip %d, extracting %d to %d",
             last, tip + CONFIRMATIONS, start, end)
    return start, end


def run_live(from_block, to_block, snapshot_path=None, incremental=False):
    from web3 import Web3

    rpc = os.environ.get("RPC_URL")
    if not rpc:
        sys.exit("RPC_URL is not set. Add a Base mainnet RPC URL to .env for live extraction.")
    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 30}))
    chain = w3.eth.chain_id
    if chain != CHAIN_ID:
        sys.exit(f"RPC is on chain {chain}, expected Base ({CHAIN_ID}).")
    log.info("Connected to Base, chain id %d", chain)

    if incremental:
        from_block, to_block = incremental_range(w3)
        if from_block > to_block:
            log.info("Up to date. Nothing to extract.")
            return

    pool = w3.eth.contract(address=Web3.to_checksum_address(POOL), abi=POOL_ABI)
    npm = w3.eth.contract(address=Web3.to_checksum_address(POSITION_MANAGER),
                          abi=POSITION_MANAGER_ABI)

    # (contract, topic0) -> decoder
    decoders = {}
    for addr, contract, abi in ((POOL, pool, POOL_ABI), (POSITION_MANAGER, npm, POSITION_MANAGER_ABI)):
        for ev in abi:
            sig = f"{ev['name']}({','.join(i['type'] for i in ev['inputs'])})"
            topic = Web3.to_hex(Web3.keccak(text=sig))
            decoders[(addr, topic)] = getattr(contract.events, ev["name"])()
    topics = sorted({t for _, t in decoders})

    # Base produces one block every 2 seconds, so timestamps are interpolated
    # from the range ends instead of one RPC call per block.
    t_from = with_retry(w3.eth.get_block, from_block)["timestamp"]
    t_to = with_retry(w3.eth.get_block, to_block)["timestamp"]
    spb = (t_to - t_from) / max(to_block - from_block, 1)
    log.info("Blocks %d to %d, %.2f seconds per block", from_block, to_block, spb)

    def block_ts(log_entry):
        raw = log_entry.get("blockTimestamp")
        if raw is not None:
            ts = int(raw, 16) if isinstance(raw, str) else int(raw)
        else:
            ts = round(t_from + (log_entry["blockNumber"] - from_block) * spb)
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

    conn = connect()
    all_rows = []
    found = inserted = 0
    n_chunks = (to_block - from_block) // CHUNK_SIZE + 1

    for i, start in enumerate(range(from_block, to_block + 1, CHUNK_SIZE), 1):
        end = min(start + CHUNK_SIZE - 1, to_block)
        logs = with_retry(w3.eth.get_logs, {
            "address": [Web3.to_checksum_address(POOL), Web3.to_checksum_address(POSITION_MANAGER)],
            "fromBlock": start, "toBlock": end, "topics": [topics],
        })
        # The position manager serves every pool on Base. Keep its events only
        # in transactions that also touched our pool.
        pool_txs = {Web3.to_hex(l["transactionHash"]) for l in logs if l["address"].lower() == POOL}

        rows = []
        for raw in logs:
            addr = raw["address"].lower()
            tx = Web3.to_hex(raw["transactionHash"])
            if addr == POSITION_MANAGER and tx not in pool_txs:
                continue
            decoder = decoders.get((addr, Web3.to_hex(raw["topics"][0])))
            if decoder is None:
                continue
            ev = decoder.process_log(raw)
            rows.append({
                "chain_id": CHAIN_ID,
                "contract_address": addr,
                "event_name": ev["event"],
                "block_number": ev["blockNumber"],
                "block_timestamp": block_ts(raw),
                "tx_hash": tx,
                "log_index": ev["logIndex"],
                "args": clean_args(dict(ev["args"])),
            })

        inserted += load(conn, rows)
        found += len(rows)
        all_rows.extend(rows)
        if i % 100 == 0 or i == n_chunks:
            log.info("Chunk %d/%d (block %d): %d events so far, %d new", i, n_chunks, end, found, inserted)

    with conn.cursor() as cur:
        cur.execute("INSERT INTO raw.extract_runs (from_block, to_block) VALUES (%s, %s) "
                    "ON CONFLICT DO NOTHING", (from_block, to_block))
    conn.commit()
    conn.close()
    log.info("Done: %d events found, %d new rows, %d already in DB", found, inserted, found - inserted)

    counts = {}
    for r in all_rows:
        key = ("pool" if r["contract_address"] == POOL else "position_manager", r["event_name"])
        counts[key] = counts.get(key, 0) + 1
    for (src, name), n in sorted(counts.items()):
        log.info("  %-16s %-18s %d", src, name, n)
    npm_mints = sum(1 for r in all_rows if r["event_name"] == "Mint"
                    and r["contract_address"] == POOL and r["args"]["sender"] == POSITION_MANAGER)
    if npm_mints == 0:
        log.warning("No pool Mint came from the position manager. Check POSITION_MANAGER_ADDRESS.")

    if snapshot_path is None:
        return
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    all_rows.sort(key=lambda r: (r["block_number"], r["log_index"]))
    with gzip.open(snapshot_path, "wt") as f:
        for r in all_rows:
            f.write(json.dumps(r) + "\n")
    log.info("Snapshot written to %s (%d rows)", snapshot_path, len(all_rows))


# ---------- Entry point ----------

def main():
    p = argparse.ArgumentParser(description="Load Uniswap V3 LP events into Postgres")
    p.add_argument("--from", dest="from_block", type=int, help="first block (live mode)")
    p.add_argument("--to", dest="to_block", type=int, help="last block (live mode)")
    p.add_argument("--incremental", action="store_true",
                   help="live mode: continue from the last loaded block (used by Airflow)")
    p.add_argument("--snapshot", default=str(DEFAULT_SNAPSHOT),
                   help="snapshot file to load, or to write with --write-snapshot")
    p.add_argument("--write-snapshot", action="store_true",
                   help="live mode: also write the extracted rows to --snapshot")
    args = p.parse_args()

    if (args.from_block is None) != (args.to_block is None):
        p.error("--from and --to go together")
    if args.incremental and args.from_block is not None:
        p.error("use --incremental or --from/--to, not both")
    snapshot = Path(args.snapshot) if args.write_snapshot else None
    if args.incremental:
        run_live(None, None, snapshot, incremental=True)
    elif args.from_block is not None:
        if args.from_block > args.to_block:
            p.error("--from must be <= --to")
        run_live(args.from_block, args.to_block, snapshot)
    else:
        run_snapshot(args.snapshot)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        log.exception("Extraction failed")
        sys.exit(1)
