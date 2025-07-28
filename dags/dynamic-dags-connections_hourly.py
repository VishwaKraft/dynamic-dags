# Standard library imports
from datetime import datetime, timedelta
import datetime as dt
import pytz
import json

# Airflow imports
from airflow import settings
from airflow.models.connection import Connection
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.exceptions import AirflowException
from airflow.operators.dummy import DummyOperator
import boto3
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Third-party imports
from operators.clickhouse_to_s3_operator import (
    ClickhouseToS3Operator,
)
from pytz import timezone
import pyarrow.parquet as pq

# Define constants
local_timezone = timezone("Asia/Kolkata")
default_args = {
    "owner": "airflow",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2024, 9, 1, tzinfo=local_timezone)
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
    Also parses source_attributes JSON if present and pushes parsed values to XCom.
    """
    ist = pytz.timezone("Asia/Kolkata")
    now_ist = dt.datetime.now(ist) - timedelta(days=2)
    prev_hour = now_ist - timedelta(hours=1)
    dag_date = prev_hour.strftime("%Y-%m-%d")
    start_timestamp = prev_hour.strftime("%Y-%m-%d %H:00:00")
    end_timestamp = prev_hour.strftime("%Y-%m-%d %H:59:59")
    
    query = kwargs["params"].get("query", "")
    query = query.replace("%(start_timestamp)s", start_timestamp)
    query = query.replace("%(end_timestamp)s", end_timestamp)

    # Parse source_attributes JSON if present
    source_attributes = kwargs["params"].get("source_attributes")
    conn_id = None
    bucket_name = None
    bucket_key = None
    bucket_key_date_format = None
    formatted_bucket_key = None
    if source_attributes:
        try:
            attrs = json.loads(source_attributes)
            conn_id = attrs.get("conn_id")
            bucket_name = attrs.get("bucket_name")
            bucket_key = attrs.get("bucket_key")
            bucket_key_date_format = attrs.get("bucket_key_date_format")
            if bucket_key and bucket_key_date_format:
                dag_date_fmt = dt.datetime.strptime(dag_date, "%Y-%m-%d").strftime(bucket_key_date_format)
                formatted_bucket_key = bucket_key.format(date=dag_date_fmt)
        except Exception as e:
            print(f"Error parsing source_attributes JSON: {e}")

    # Parse destination_attributes JSON if present
    destination_attributes = kwargs["params"].get("destination_attributes")
    dest_conn_id = None
    dest_bucket_name = None
    dest_bucket_key = None
    dest_bucket_key_date_format = None
    formatted_dest_bucket_key = None
    dest_table_name = None
    dest_table_schema = None
    if destination_attributes:
        try:
            dest_attrs = json.loads(destination_attributes)
            print(f"Destination attributes: {dest_attrs}")
            dest_conn_id = dest_attrs.get("conn_id")
            dest_table_name = dest_attrs.get("table_name")
            dest_table_schema = dest_attrs.get("table_schema")
            dest_bucket_name = dest_attrs.get("bucket_name")
            dest_bucket_key = dest_attrs.get("bucket_key")
            dest_bucket_key_date_format = dest_attrs.get("bucket_key_date_format")
            dest_exclude_columns = dest_attrs.get("exclude_columns")
            
            if dest_bucket_key and dest_bucket_key_date_format:
                dag_date_fmt = dt.datetime.strptime(dag_date, "%Y-%m-%d").strftime(dest_bucket_key_date_format)
                formatted_dest_bucket_key = dest_bucket_key.format(date=dag_date_fmt)
        except Exception as e:
            print(f"Error parsing destination_attributes JSON: {e}")

    # Push values to XCom
    kwargs["ti"].xcom_push(key="dag_date", value=dag_date)
    kwargs["ti"].xcom_push(key="rendered_query", value=query)
    kwargs["ti"].xcom_push(key="start_timestamp", value=start_timestamp)
    kwargs["ti"].xcom_push(key="end_timestamp", value=end_timestamp)
    if conn_id:
        kwargs["ti"].xcom_push(key="source_conn_id", value=conn_id)
    if bucket_name:
        kwargs["ti"].xcom_push(key="source_bucket_name", value=bucket_name)
    if formatted_bucket_key:
        kwargs["ti"].xcom_push(key="source_bucket_key", value=formatted_bucket_key)
    if dest_conn_id:
        kwargs["ti"].xcom_push(key="dest_conn_id", value=dest_conn_id)
    if dest_bucket_name:
        kwargs["ti"].xcom_push(key="dest_bucket_name", value=dest_bucket_name)
    if formatted_dest_bucket_key:
        kwargs["ti"].xcom_push(key="dest_bucket_key", value=formatted_dest_bucket_key)
    if dest_table_name:
        kwargs["ti"].xcom_push(key="dest_table_name", value=dest_table_name)
    if dest_table_schema:
        kwargs["ti"].xcom_push(key="dest_table_schema", value=dest_table_schema)
    if dest_exclude_columns:
        kwargs["ti"].xcom_push(key="dest_exclude_columns", value=dest_exclude_columns)
    print(f"Processing date: {dag_date} : Query: {query}")
    if source_attributes:
        print(f"Parsed source_attributes: conn_id={conn_id}, bucket_name={bucket_name}, bucket_key={formatted_bucket_key}")
    if destination_attributes:
        print(f"Parsed destination_attributes: conn_id={dest_conn_id}, bucket_name={dest_bucket_name}, bucket_key={formatted_dest_bucket_key}")

# Function to create a DAG dynamically
def create_dag(dag_id, table_name, query, source, source_attributes, destination, destination_attributes, dag_date):
    """
    Creates a DAG dynamically based on the provided parameters.
    Uses conn_id from source_attributes JSON.
    """

    with DAG(
        dag_id,
        default_args=default_args,
        schedule_interval="@hourly",
        catchup=False,
        tags=["Hourly", "Data Migration", "Dynamic Dags"],
        params={"dag_date": dag_date, "query": query, "source_attributes": source_attributes, "destination_attributes": destination_attributes},
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
            extract = ClickhouseToS3Operator(
                task_id=f"clickhouse_to_object_store_task_{table_name}",
                clickhouse_conn_id="{{ task_instance.xcom_pull(task_ids='process_date', key='source_conn_id') }}",
                query="{{ task_instance.xcom_pull(task_ids='process_date', key='rendered_query') }}",
                s3_bucket="{{ var.value.get('OBJECT_STORE_BUCKET') }}",
                file_format="parquet",
                s3_key="{{ var.value.get('ENV') }}" + f"/DynamicDags/{table_name}/"+ "{{ task_instance.xcom_pull(task_ids='process_date', key='dag_date') }}" + "/data.parquet",
                replace=True,
            ) >> end_dag
        elif source == "MYSQL":
             extract = SqlToS3Operator(
                task_id=f"sql_to_object_store_task_{table_name}",
                sql_conn_id="{{ task_instance.xcom_pull(task_ids='process_date', key='source_conn_id') }}",
                query="{{ task_instance.xcom_pull(task_ids='process_date', key='rendered_query') }}",
                s3_bucket="{{ var.value.get('OBJECT_STORE_BUCKET') }}",
                file_format="parquet",
                s3_key="{{ var.value.get('ENV') }}" + f"/DynamicDags/{table_name}/"+ "{{ task_instance.xcom_pull(task_ids='process_date', key='dag_date') }}" + "/data.parquet",
                replace=True,
            )
        elif source == "OBJECT_STORE":
            list_files = S3ListOperator(
                task_id=f"s3_to_object_store_task_{table_name}_extract",
                bucket="{{ task_instance.xcom_pull(task_ids='process_date', key='source_bucket_name') }}",
                prefix="{{ task_instance.xcom_pull(task_ids='process_date', key='source_bucket_key') }}",  # Include trailing slash for directory
                aws_conn_id='aws_default',
            )

            def copy_files_from_s3(**kwargs):
                from airflow.models import Variable
                ti = kwargs['ti']
                files = ti.xcom_pull(task_ids=f"s3_to_object_store_task_{table_name}_extract", key='return_value')
                print(f"Files: {files}")
                source_bucket = ti.xcom_pull(task_ids='process_date', key='source_bucket_name')
                dag_date = ti.xcom_pull(task_ids='process_date', key='dag_date')
                dest_bucket = Variable.get('OBJECT_STORE_BUCKET')
                env = Variable.get('ENV')
                s3_hook = S3Hook(aws_conn_id='aws_default')
                for file_key in files:
                    dest_key = f"{env}/DynamicDags/{table_name}/{dag_date}/{file_key.split('/')[-1]}"
                    s3_hook.copy_object(
                        source_bucket_key=file_key,
                        dest_bucket_key=dest_key,
                        source_bucket_name=source_bucket,
                        dest_bucket_name=dest_bucket
                    )
                    print(f"Copied file: {file_key} to {dest_key}")

            copy_file_task = PythonOperator(
                task_id='copy_s3_file',
                python_callable=copy_files_from_s3,
                provide_context=True,
            )

            list_files.set_upstream(process_date)
            extract = [ list_files >> copy_file_task]
        else:
            raise AirflowException(f"Invalid source: {source}")


        if destination in ["CLICKHOUSE", "MYSQL"]:

            list_files = S3ListOperator(
                task_id=f"s3_to_object_store_task_{table_name}_load",
                bucket="{{ var.value.get('OBJECT_STORE_BUCKET') }}",
                prefix="{{ var.value.get('ENV') }}" + f"/DynamicDags/{table_name}/" + "{{ task_instance.xcom_pull(task_ids='process_date', key='dag_date') }}" + f"/",  # Include trailing slash for directory
                aws_conn_id='aws_default',
            )

    
            def load_files_to_sql(**kwargs):
                from airflow.models import Variable
                from airflow.hooks.base_hook import BaseHook
                import pyarrow.parquet as pq
                import os
                import pandas as pd
                from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
                from utils.set_if_null import set_if_null

                ti = kwargs['ti']
                files = ti.xcom_pull(task_ids=f"s3_to_object_store_task_{table_name}_load", key='return_value')
                bucket = Variable.get('OBJECT_STORE_BUCKET')
                env = Variable.get('ENV')
                dag_date = ti.xcom_pull(task_ids='process_date', key='dag_date')
                dest_conn_id = ti.xcom_pull(task_ids='process_date', key='dest_conn_id')
                dest_table_name = ti.xcom_pull(task_ids='process_date', key='dest_table_name')
                dest_exclude_columns = ti.xcom_pull(task_ids='process_date', key='dest_exclude_columns')

                s3_hook = S3Hook(aws_conn_id='aws_default')
                for file_key in files:
                    # Download file to local
                    local_path = f"/tmp/{os.path.basename(file_key)}"
                    s3_hook.get_key(file_key, bucket_name=bucket).download_file(local_path)
                    # Check if file size is greater than zero
                    if os.path.getsize(local_path) > 0:
                        # Parse parquet
                        table = pq.read_table(local_path)
                        data = table.to_pylist()
                        df = pd.DataFrame(data)
                        if dest_exclude_columns:
                            df = df.drop(columns=dest_exclude_columns)
                        if destination == "CLICKHOUSE":
                            set_if_null(df)
                            print(df.info())
                            if not df.empty:
                                values = [tuple(row) for row in df.values]
                                columns = df.columns.tolist()
                                query = f"INSERT INTO {dest_table_name} ({', '.join(columns)}) VALUES"
                                print(f"SQL Connection ID: {dest_conn_id}")
                                clickhouse_hook = ClickHouseHook(dest_conn_id)
                                print(f"Query: {query}")
                                clickhouse_hook.get_conn().execute(query, values)
                                print(f"Bulk loaded {len(values)} rows into ClickHouse from {file_key}")
                        elif destination == "MYSQL":
                            mysql_hook = MySqlHook(mysql_conn_id=dest_conn_id)
                            for row in data:
                                mysql_hook.insert_rows(table=dest_table_name, rows=[tuple(row.values())])
                                print(f"Loaded file: {file_key} to local path: {local_path}")
                    os.remove(local_path)

            load_task = PythonOperator(
                task_id=f'load_files_to_sql_{table_name}',
                python_callable=load_files_to_sql,
                provide_context=True,
            )

            load = [list_files >> load_task]

            wait = DummyOperator(
                task_id='wait_for_load',
            )

            wait.set_downstream(list_files)


        start_dag >> process_date >> extract >> wait >> load >> end_dag

    return dag

# Generate DAGs for each connection and table
for conn in conns:
    mysql_hook = MySqlHook(mysql_conn_id=conn[0])
    list_of_tables = mysql_hook.get_records(
        "SELECT id, table_name, query, source, source_attributes, destination, destination_attributes FROM dynamic_dags_hourly WHERE is_active = 1"
    )
    for one_row in list_of_tables:
        dag_id = f"dynamic_dag_hourly_{conn[0]}_{one_row[0]}".lower()
        ist = pytz.timezone("Asia/Kolkata")
        date = dt.datetime.now(ist).date()
        dag_date = date.strftime("%Y-%m-%d")
        globals()[dag_id] = create_dag(
            dag_id, one_row[1], one_row[2], one_row[3], one_row[4], one_row[5], one_row[6], dag_date
        )