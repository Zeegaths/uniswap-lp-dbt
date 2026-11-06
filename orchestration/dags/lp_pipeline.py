"""lp_pipeline.py: extract LP events, then dbt run, dbt test, report.

    extract_events ──▶ dbt_run ──▶ dbt_test ──▶ report

1. extract_events : extract/extract.py in one of three modes (trigger param `mode`)
                    incremental  continue from the last loaded block (default, used by the schedule)
                    range        a fixed block range from `from_block` / `to_block`
                    snapshot     load the committed snapshot, no RPC needed
2. dbt_run        : build staging, intermediate and marts
3. dbt_test       : run the schema tests as their own task, so a data failure shows red
4. report         : print win / loss / open counts from fct_lp_positions

Schedule: daily at 03:17 UTC. DAGs start paused, so nothing runs until you unpause it.
The repo root is mounted at /opt/airflow/project (see docker-compose.airflow.yml).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag

PROJECT_DIR = "/opt/airflow/project"

EXTRACT_CMD = (
    "{% if params.mode == 'snapshot' %}"
    "python extract/extract.py"
    "{% elif params.mode == 'range' %}"
    "python extract/extract.py --from {{ params.from_block }} --to {{ params.to_block }}"
    "{% else %}"
    "python extract/extract.py --incremental"
    "{% endif %}"
)

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="uniswap_lp_pipeline",
    description="Base WETH/USDC LP events -> Postgres -> dbt run + test -> report",
    schedule="17 3 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["uniswap", "base", "dbt"],
    params={
        "mode": Param("incremental", type="string", enum=["incremental", "range", "snapshot"]),
        "from_block": Param(53_400_000, type="integer"),
        "to_block": Param(53_443_199, type="integer"),
    },
)
def uniswap_lp_pipeline():
    extract_events = BashOperator(
        task_id="extract_events",
        cwd=PROJECT_DIR,
        bash_command=EXTRACT_CMD,
        retries=2,
        execution_timeout=timedelta(hours=1),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        cwd=PROJECT_DIR,
        bash_command="dbt run --profiles-dir . --project-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        cwd=PROJECT_DIR,
        bash_command="dbt test --profiles-dir . --project-dir .",
    )

    report = BashOperator(
        task_id="report",
        cwd=PROJECT_DIR,
        bash_command="python orchestration/report.py",
    )

    extract_events >> dbt_run >> dbt_test >> report


uniswap_lp_pipeline()
