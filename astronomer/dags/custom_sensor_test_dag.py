from airflow.sdk import dag
from include.custom_operators.custom_sql_deferrable_operator import CustomSqlSensor
from datetime import datetime
import os

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

ENV_CONFIG = {
    "prod": {"SNOWFLAKE_CONN_ID": "dna_prod_snowflake_conn", "DB": "PROD_SF_TZWEB2_DB"},
    "uat":  {"SNOWFLAKE_CONN_ID": "dna_prod_snowflake_conn", "DB": "UAT_SF_TZWEB2_DB"},
    "int":  {"SNOWFLAKE_CONN_ID": "dna_int_snowflake_conn",  "DB": "INT_SF_TZWEB2_DB"},
    "dev":  {"SNOWFLAKE_CONN_ID": "dna_int_snowflake_conn",  "DB": "DEV_SF_TZWEB2_DB"},
}

DEFAULTS = {**BASE_DEFAULTS, **ENV_CONFIG.get(ENV, {})}

@dag(
    dag_id="custom_sensor_dag",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    template_searchpath=[SQL_DIR],
    tags=["DNA", "TEST DAG"],
)
def custom_sensor_dag():
    wait_for_ready_flag = CustomSqlSensor(
        task_id="wait_for_ready_flag",
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

    wait_for_ready_flag

custom_sensor_dag()
