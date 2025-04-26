# Standard library imports
from datetime import datetime, timedelta
import datetime as dt
import pytz

# Airflow imports
from airflow import settings
from airflow.models.connection import Connection
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.hooks.mysql_hook import MySqlHook

# Third-party imports
from airflow_clickhouse_plugin.operators.clickhouse_to_s3 import ClickhouseToS3Operator
from pytz import timezone

# Define constants
local_timezone = timezone("Asia/Kolkata")
default_args = {
    "owner": "airflow",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2024, 9, 1, tzinfo=local_timezone),
    "sla": timedelta(minutes=10),
}

# Calculate start and end timestamps in IST
now_utc = dt.datetime.now(pytz.utc)
ist = pytz.timezone("Asia/Kolkata")
current_date_ist = now_utc.astimezone(ist).date()
start_date = current_date_ist - timedelta(days=1)
end_date = current_date_ist - timedelta(days=1)
start_timestamp = start_date.strftime("%Y-%m-%d 00:00:00")
end_timestamp = end_date.strftime("%Y-%m-%d 23:59:59")
start_date_display = start_date.strftime("%Y-%m-%d")

# Query connections
session = settings.Session()
conns = (
    session.query(Connection.conn_id)
    .filter(Connection.conn_id.ilike("%DATABASE%"))
    .all()
)

# Function to process date and render query
def process_date_function(**kwargs):
    """
    Processes the date and renders the query with start and end timestamps in IST.
    """
    ist = pytz.timezone("Asia/Kolkata")
    current_date_ist = dt.datetime.now(ist).date()
    date = current_date_ist - timedelta(days=1)
    date_display = date.strftime("%Y-%m-%d")
    dag_date = kwargs["params"].get("dag_date", date_display)
    start_timestamp = f"{dag_date} 00:00:00"
    end_timestamp = f"{dag_date} 23:59:59"
    
    query = kwargs["params"].get("query", "")
    query = query.replace("%(start_timestamp)s", start_timestamp)
    query = query.replace("%(end_timestamp)s", end_timestamp)
    
    # Push values to XCom
    kwargs["ti"].xcom_push(key="rendered_query", value=query)
    kwargs["ti"].xcom_push(key="start_timestamp", value=dag_date)
    print(f"Processing date: {dag_date} : Query: {query}")

# Function to create a DAG dynamically
def create_dag(dag_id, table_name, query, source, conn_id, dag_date):
    """
    Creates a DAG dynamically based on the provided parameters.
    """
    with DAG(
        dag_id,
        default_args=default_args,
        schedule_interval="30 03 * * *",
        catchup=False,
        params={"dag_date": dag_date, "query": query},
    ) as dag:
        # Define tasks
        start_dag = BashOperator(task_id="start_dag", bash_command="date")
        process_date = PythonOperator(
            task_id="process_date",
            python_callable=process_date_function,
            provide_context=True,
        )
        end_dag = BashOperator(task_id="end_dag", bash_command="date")

        # Add source-specific tasks
        if source == "CLICKHOUSE":
            start_dag >> process_date >> ClickhouseToS3Operator(
                task_id=f"clickhouse_to_object_store_task_{table_name}",
                clickhouse_conn_id=conn_id,
                query="{{ task_instance.xcom_pull(task_ids='process_date', key='rendered_query') }}",
                s3_bucket="{{ var.value.get('MINIO_BUCKET') }}",
                file_format="parquet",
                s3_key="{{ var.value.get('ENV') }}" + f"/DynamicDags/{table_name}/{{ task_instance.xcom_pull(task_ids='process_date', key='dag_date') }}/data.parquet",
                replace=True,
            ) >> end_dag
        else:
            start_dag >> process_date >> SqlToS3Operator(
                task_id=f"sql_to_object_store_task_{table_name}",
                sql_conn_id=conn_id,
                query="{{ task_instance.xcom_pull(task_ids='process_date', key='rendered_query') }}",
                s3_bucket="{{ var.value.get('MINIO_BUCKET') }}",
                file_format="parquet",
                s3_key="{{ var.value.get('ENV') }}" + f"/DynamicDags/{table_name}/{{ task_instance.xcom_pull(task_ids='process_date', key='dag_date') }}/data.parquet",
                replace=True,
            ) >> end_dag

    return dag

# Generate DAGs for each connection and table
for conn in conns:
    mysql_hook = MySqlHook(mysql_conn_id=conn[0])
    list_of_tables = mysql_hook.get_records(
        "SELECT id, table_name, query, source FROM dynamic_dags WHERE is_active = 1"
    )
    for one_row in list_of_tables:
        dag_id = f"dynamic_dag_{conn[0]}_{one_row[1]}".lower()
        ist = pytz.timezone("Asia/Kolkata")
        yesturday_date = dt.datetime.now(ist).date() - timedelta(days=1)
        dag_date = yesturday_date.strftime("%Y-%m-%d")
        globals()[dag_id] = create_dag(
            dag_id, one_row[1], one_row[2], one_row[3], conn[0], dag_date
        )