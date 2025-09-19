from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict

from airflow.sdk import dag, task, Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

ENV = os.getenv("environment", "dev").strip().lower()
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
SQL_DIR = os.path.join(AIRFLOW_HOME, "include", "data_load_dag_utilities", "sql")

BASE_DEFAULTS = {
    "SNOWFLAKE_CONN_ID": "snowflake_default",
    "STORED_PROC_DEFAULT_SCHEMA": "TMS",
}

ENV_CONFIG: Dict[str, Dict[str, Any]] = {
    "prod": {"SNOWFLAKE_CONN_ID": "dna_prod_snowflake_conn", "DB": "PROD_SF_TZWEB2_DB"},
    "uat":  {"SNOWFLAKE_CONN_ID": "dna_prod_snowflake_conn", "DB": "UAT_SF_TZWEB2_DB"},
    "int":  {"SNOWFLAKE_CONN_ID": "dna_int_snowflake_conn",  "DB": "INT_SF_TZWEB2_DB"},
    "dev":  {"SNOWFLAKE_CONN_ID": "dna_int_snowflake_conn",  "DB": "DEV_SF_TZWEB2_DB"},
}

DEFAULTS = {**BASE_DEFAULTS, **ENV_CONFIG.get(ENV, {})}

@dag(
    dag_id="dna_data_load_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    template_searchpath=[SQL_DIR],
    params={
        "FILE_PATH": Param("", type="string"),
        "TARGET_TABLE": Param("", type="string"),
        "MODULE_NAME": Param("", type="string"),
    },
    tags=["DNA", "DATA LOAD DAG"],
)
def dna_data_load_dag():
    start = EmptyOperator(task_id="start")

    call_load_proc = SQLExecuteQueryOperator(
        task_id="call_data_load",
        conn_id=DEFAULTS["SNOWFLAKE_CONN_ID"],
        sql="load_data_query.sql",
        params={
            "DB": DEFAULTS["DB"],
            "SCHEMA": DEFAULTS["STORED_PROC_DEFAULT_SCHEMA"],
        },
    )

    @task
    def send_email_slack_notification() -> str:
        return "Placeholder for email/slack notification logic."

    send_notification = send_email_slack_notification()
    end = EmptyOperator(task_id="end")

    start >> call_load_proc >> send_notification >> end

dna_data_load_dag()
