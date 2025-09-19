from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List

from airflow.sdk import Asset, AssetWatcher, dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from include.scheduler_dag_utilities.helpers import (
    parse_sqs_message_body,
    metas_from_payloads,
    collapse_by_prefix,
    items_for_manifest,
    build_trigger_kwargs,
)

ENV = os.getenv("environment", "dev").strip().lower()
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
SQL_DIR = os.path.join(AIRFLOW_HOME, "include", "scheduler_dag_utilities", "sql")
SQS_QUEUE = "https://sqs.us-east-1.amazonaws.com/396608776041/tt-dnaint-qa-tzweb2-sqs"

ENV_CONFIG: Dict[str, Dict[str, Any]] = {
    "prod": {
        "SNOWFLAKE_CONN_ID": "dna_prod_snowflake_conn",
        "DB": "PROD_SF_TZWEB2_DB",
        "AWS_CONN_ID": "dna_prod_aws_conn",
    },
    "uat": {
        "SNOWFLAKE_CONN_ID": "dna_prod_snowflake_conn",
        "DB": "UAT_SF_TZWEB2_DB",
        "AWS_CONN_ID": "dna_prod_aws_conn",
    },
    "int": {
        "SNOWFLAKE_CONN_ID": "dna_int_snowflake_conn",
        "DB": "INT_SF_TZWEB2_DB",
        "AWS_CONN_ID": "dna_int_aws_conn",
    },
    "dev": {
        "SNOWFLAKE_CONN_ID": "dna_int_snowflake_conn",
        "DB": "DEV_SF_TZWEB2_DB",
        "AWS_CONN_ID": "dna_int_aws_conn",
    },
}
DEFAULTS = ENV_CONFIG.get(ENV, {})

trigger = MessageQueueTrigger(
    aws_conn_id=DEFAULTS["AWS_CONN_ID"],
    queue=SQS_QUEUE,
    waiter_delay=30,
)

sqs_queue_asset = Asset(
    "sqs_queue_asset", watchers=[AssetWatcher(name="sqs_watcher", trigger=trigger)]
)

@dag(
    dag_id="dna_scheduler_dag",
    schedule=[sqs_queue_asset],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    template_searchpath=[SQL_DIR],
    tags=["DNA", "SCHEDULER DAG"],
)
def dna_scheduler_dag():
    @task
    def process_messages(**context) -> List[Dict[str, str]]:
        events = context["triggering_asset_events"][sqs_queue_asset]
        if not events:
            raise AirflowSkipException("No SQS events in this batch.")

        batch = events[0].extra["payload"]["message_batch"]
        payloads = [parse_sqs_message_body(msg.get("Body", "")) for msg in batch]
        metas = metas_from_payloads(payloads)
        prefix_map = collapse_by_prefix(metas)
        return items_for_manifest(prefix_map, DEFAULTS["DB"])

    items = process_messages()

    manifest_lookup = SQLExecuteQueryOperator.partial(
        task_id="manifest_lookup",
        conn_id=DEFAULTS["SNOWFLAKE_CONN_ID"],
        sql="manifest_lookup.sql",
        do_xcom_push=True,
        split_statements=False,
        handler=build_trigger_kwargs,
    ).expand(params=items)

    TriggerDagRunOperator.partial(
        task_id="trigger_downstream",
        reset_dag_run=True,
        wait_for_completion=False,
    ).expand_kwargs(manifest_lookup.output)

dna_scheduler_dag()
