from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List

from airflow.sdk import dag, task, task_group, Param, get_current_context
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from include.custom_operators.custom_sql_deferrable_operator import CustomSqlSensor
from include.measurement_dags_utilities.helpers import (
    fetch_as_dicts,
    build_calls,
    parse_file_path_info,
    validate_inputs,
)

ENV = os.getenv("environment", "dev").strip().lower()
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
SQL_DIR = os.path.join(AIRFLOW_HOME, "include", "measurement_dags_utilities", "sql")

BASE_DEFAULTS = {
    "TARGET_ID": "E3",
    "FREQUENCY_ID": "TPO",
    "SNOWFLAKE_CONN_ID": "snowflake_default",
    "STORED_PROC_DEFAULT_SCHEMA": "TMS",
    "JOBS_SCHEMA": "AIRFLOW",
    "JOBS_TABLE": "JOBCONFIG",
}

ENV_CONFIG: Dict[str, Dict[str, Any]] = {
    "prod": {"SNOWFLAKE_CONN_ID": "dna_prod_snowflake_conn", "DB": "PROD_SF_TZWEB2_DB"},
    "uat":  {"SNOWFLAKE_CONN_ID": "dna_prod_snowflake_conn", "DB": "UAT_SF_TZWEB2_DB"},
    "int":  {"SNOWFLAKE_CONN_ID": "dna_int_snowflake_conn",  "DB": "INT_SF_TZWEB2_DB"},
    "dev":  {"SNOWFLAKE_CONN_ID": "dna_int_snowflake_conn",  "DB": "DEV_SF_TZWEB2_DB"},
}

DEFAULTS = {**BASE_DEFAULTS, **ENV_CONFIG.get(ENV, {})}

STEPS: List[Dict[str, Any]] = [
    {"label": "TMSRunSP Step 7", "steporder": 7},
    {"label": "TMSAdjustForClockFloat Step 9", "steporder": 9},
    {"label": "TMSLoadClientTradePrcE3V2 Step 10", "steporder": 10},
    {"label": "TMSRunSP Step 12", "steporder": 12},
    {"label": "TMSTransferData 16", "steporder": 16},
    {"label": "TMSTransferData Step 17", "steporder": 17},
    {"label": "TMSTransferData Step 18", "steporder": 18},
    {"label": "TMSTransferData Step 19", "steporder": 19},
    {"label": "TMSTransferData Step 20", "steporder": 20},
    {"label": "TMSTransferData Step 21", "steporder": 21},
    {"label": "TMSAWSFileTransfer Step 31", "steporder": 31},
]

@dag(
    dag_id="dna_e3_dag",
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
    tags=["DNA", "MEASUREMENT DAG"],
)
def dna_e3_dag():
    start = EmptyOperator(task_id="start")

    @task
    def extract_metadata(file_path: str, target_table: str, module_name: str) -> Dict[str, Any]:
        print(f"Extracting metadata from file_path: {file_path}, target_table: {target_table}, module_name: {module_name}")
        metadata = parse_file_path_info(file_path)
        metadata["target_table"] = target_table
        metadata["module_name"] = module_name
        return metadata

    @task
    def validate_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
        return validate_inputs(metadata)
    
    check_market_data_availability = CustomSqlSensor(
        task_id="check_market_data_availability",
        conn_id="dna_int_snowflake_conn",
        sql="check_market_data_availability.sql",
        # params={
        #     "DB": DEFAULTS["DB"],
        #     "JOBS_SCHEMA": DEFAULTS["JOBS_SCHEMA"],
        #     "JOBS_TABLE": DEFAULTS["JOBS_TABLE"],
        # },
        poll_interval=30,
        timeout=3600,
    )

    load_data = SQLExecuteQueryOperator(
        task_id="load_data",
        conn_id=DEFAULTS["SNOWFLAKE_CONN_ID"],
        sql="load_data_query.sql",
        params={
            "DB": DEFAULTS["DB"],
            "SCHEMA": DEFAULTS["STORED_PROC_DEFAULT_SCHEMA"],
        },
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    perform_validations = SQLExecuteQueryOperator(
        task_id="perform_validations",
        conn_id=DEFAULTS["SNOWFLAKE_CONN_ID"],
        sql="validation_query.sql",
        params={
            "DB": DEFAULTS["DB"],
            "JOBS_SCHEMA": DEFAULTS["JOBS_SCHEMA"],
            "JOBS_TABLE": DEFAULTS["JOBS_TABLE"],
        },
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    def build_step_group(step_label: str, steporder: int):
        group_id = step_label.lower().replace(" ", "_").replace("/", "_")

        @task_group(group_id=group_id)
        def step_group():
            fetch_config = SQLExecuteQueryOperator(
                task_id="fetch_config",
                conn_id=DEFAULTS["SNOWFLAKE_CONN_ID"],
                sql="fetch_config_query.sql",
                params={
                    "DB": DEFAULTS["DB"],
                    "JOBS_SCHEMA": DEFAULTS["JOBS_SCHEMA"],
                    "JOBS_TABLE": DEFAULTS["JOBS_TABLE"],
                    "TARGET_ID": DEFAULTS["TARGET_ID"],
                    "FREQUENCY_ID": DEFAULTS["FREQUENCY_ID"],
                    "STEPORDER": steporder,
                },
                do_xcom_push=True,
                split_statements=False,
                trigger_rule=TriggerRule.NONE_FAILED,
                handler=fetch_as_dicts,
            )


            @task(trigger_rule=TriggerRule.NONE_FAILED)
            def make_calls(rows: List[Dict[str, Any]], default_schema: str) -> List[str]:
                ctx = get_current_context()
                ti = ctx["ti"]
                metadata = ti.xcom_pull(task_ids="extract_metadata") or {}
                load_id = int(metadata.get("load_id", 0))
                return build_calls(
                    rows=rows,
                    default_schema=default_schema,
                    steporder=steporder,
                    load_id=load_id,
                    target_id=DEFAULTS["TARGET_ID"],
                )

            calls = make_calls(fetch_config.output, DEFAULTS["STORED_PROC_DEFAULT_SCHEMA"])

            # SQLExecuteQueryOperator.partial(
            #     task_id="execute_sp",
            #     conn_id=DEFAULTS["SNOWFLAKE_CONN_ID"],
            #     split_statements=True,
            #     trigger_rule=TriggerRule.NONE_FAILED,
            # ).expand(sql=calls)

            @task(trigger_rule=TriggerRule.NONE_FAILED)
            def preview_sql(sql: str) -> str:
                print("\n===== DRY RUN: Would execute SQL =====\n" + sql + "\n=====================================\n")
                return sql

            preview_sql.expand(sql=calls)

        return step_group()

    @task
    def send_email_slack_notification() -> str:
        return "Placeholder for email/slack notification logic."

    metadata_extraction = extract_metadata(
        file_path="{{ params.FILE_PATH }}",
        target_table="{{ params.TARGET_TABLE }}",
        module_name="{{ params.MODULE_NAME }}",
    )
    metadata_validation = validate_metadata(metadata_extraction)

    start >> metadata_extraction
    metadata_validation >> check_market_data_availability >> load_data

    prev_step = load_data
    for step in STEPS:
        curr_step = build_step_group(step["label"], step["steporder"])
        prev_step >> curr_step
        prev_step = curr_step

    send_notification = send_email_slack_notification()
    end = EmptyOperator(task_id="end")

    prev_step >> perform_validations >> send_notification >> end

dna_e3_dag()
