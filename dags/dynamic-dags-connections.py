from datetime import datetime, timedelta
from airflow import settings
from airflow.models.connection import Connection
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.decorators import task
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow_clickhouse_plugin.operators.clickhouse_to_s3 import ClickhouseToS3Operator
from airflow.datasets import Dataset
from pytz import timezone
import datetime as dt
import pytz
from airflow.hooks.mysql_hook import MySqlHook

local_timezone = timezone("Asia/Kolkata")

default_args = {
    "owner": "airflow",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2024, 9, 1, tzinfo=local_timezone),
    "sla": timedelta(minutes=10),
}

# Get the current time in UTC
now_utc = dt.datetime.now(pytz.utc)
ist = pytz.timezone("Asia/Kolkata")
start_date = now_utc.astimezone(ist).date() - timedelta(days=1)
end_date = now_utc.astimezone(ist).date() - timedelta(days=1)
start_timestamp = start_date.strftime("%Y-%m-%d 00:00:00")
end_timestamp = end_date.strftime("%Y-%m-%d 23:59:59")
start_date_display = start_date.strftime("%Y-%m-%d")

session = settings.Session()
# adjust the filter criteria to filter which of your connections to use 
# to generated your DAGs
conns = (
    session.query(Connection.conn_id)
    .filter(Connection.conn_id.ilike("%DATABASE%"))
    .all()
)

def create_dag(dag_id, table_name, query, source, conn_id):
    with DAG(
        dag_id,
        default_args=default_args,
        schedule_interval="30 03 * * *",
        catchup=False,
    ) as dag:
        start_dag = BashOperator(task_id="start_dag", bash_command="date")

        end_dag = BashOperator(
            task_id="end_dag",
            bash_command="date",
        )

        if source == "CLICKHOUSE":
            (
                start_dag
                >> ClickhouseToS3Operator(
                    task_id=f"clickhouse_to_s3_task_{table_name}",
                    clickhouse_conn_id=conn_id,
                    query=query,
                    parameters={
                        "start_timestamp": start_timestamp,
                        "end_timestamp": end_timestamp,
                    },
                    s3_bucket="{{ var.value.get('AWS_S3_BUCKET') }}",
                    file_format="parquet",
                    s3_key="{{ var.value.get('ENV') }}" + f"/DynamicDags/{table_name}/{start_date_display}/data.parquet",
                    replace=True,
                )
                >> end_dag
            )
        else:
            (
                start_dag
                >> SqlToS3Operator(
                    task_id=f"sql_to_s3_task_{table_name}",
                    sql_conn_id=conn_id,
                    query=query,
                    parameters={
                        "start_timestamp": start_timestamp,
                        "end_timestamp": end_timestamp,
                    },
                    s3_bucket="{{ var.value.get('AWS_S3_BUCKET') }}",
                    file_format="parquet",
                    s3_key="{{ var.value.get('ENV') }}" + f"/DynamicDags/{table_name}/{start_date_display}/data.parquet",
                    replace=True,
                )
                >> end_dag
            )

    return dag

for conn in conns:
    mysql_hook = MySqlHook(mysql_conn_id=conn[0])
    list_of_tables = mysql_hook.get_records(
        "SELECT id, table_name, query, source FROM dynamic_dags where is_active = 1"
    )
    for one_row in list_of_tables:
        dag_id = f"dynamic_dag_{conn[0]}_{one_row[1]}".lower()
        globals()[dag_id] = create_dag(dag_id, one_row[1], one_row[2], one_row[3], conn[0])