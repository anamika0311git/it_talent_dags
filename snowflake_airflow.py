import logging
from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "Airflow", "start_date": datetime(2023,11,22,17,15)}

dag = DAG(
    dag_id="snowflake_connector", default_args=args, schedule_interval=None
)

query1 = [
    """select 1;""",
    """show tables in database ANALYTICSLAYER;""",
]


def count1(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="SHS_Snowflake_P")
    result = dwh_hook.get_first("CALL ANALYTICSLAYER.AN_ITTAF_P.SP_ROW_COUNT ();")
    logging.info("Number of rows in `ANALYTICSLAYER.AN_ITTAF_P.SP_ROW_COUNT`  - %s", result[0])


with dag:
    query1_exec = SnowflakeOperator(
        task_id="snowfalke_task1",
        sql=query1,
        snowflake_conn_id="SHS_Snowflake_P",
    )

    count_query = PythonOperator(task_id="count_query", python_callable=count1)
query1_exec >> count_query
