"""Print the LP outcome summary from fct_lp_positions. Last task in the DAG."""
import os

import psycopg2

conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"), port=os.getenv("DB_PORT", "5432"),
    user=os.environ["DB_USER"], password=os.environ["DB_PASS"], dbname=os.environ["DB_NAME"],
)
with conn.cursor() as cur:
    cur.execute("""
        SELECT outcome, COUNT(*), ROUND(SUM(realized_pnl_usdc)::numeric, 2)
        FROM fct_lp_positions GROUP BY outcome ORDER BY outcome
    """)
    rows = cur.fetchall()
    cur.execute("SELECT MIN(block_number), MAX(block_number) FROM raw.uniswap_events")
    lo, hi = cur.fetchone()
conn.close()

print(f"Blocks loaded: {lo} to {hi}")
print(f"{'outcome':<10} {'positions':>9} {'realized_pnl_usdc':>18}")
for outcome, n, pnl in rows:
    print(f"{outcome:<10} {n:>9} {pnl:>18}")
closed = {o: n for o, n, _ in rows if o != "open"}
if closed:
    wins = closed.get("win", 0)
    print(f"Closed positions that made money: {wins}/{sum(closed.values())} "
          f"({100 * wins / sum(closed.values()):.1f}%)")
