from datetime import datetime, timedelta
import mysql.connector
from airflow.models.connection import Connection
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.decorators import task
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from airflow.operators.python_operator import PythonOperator
from callbacks.ms_teams_callback_functions_for_data_warehouse import (
    failure_callback,
    dag_success_callback,
)
from operators.clickhouse_to_s3_operator import (
    ClickhouseToS3Operator,
)
from airflow.datasets import Dataset
from pytz import timezone

local_timezone = timezone("Asia/Kolkata")

EMAIL = ["5aacb26a.infoedge.com@apac.teams.ms"]

default_args = {
    "owner": "Amol",
    "retries": 5,
    "retry_delay": timedelta(minutes=15),
    "on_failure_callback": failure_callback,
    "start_date": datetime(2024, 9, 1, tzinfo=local_timezone),
    "sla": timedelta(minutes=10),
    "email_on_success": True,
    "email_on_retry": True,
    "email_on_failure": True,
    "email": EMAIL,
}

with DAG(
    "dynamic_dag",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="30 03 * * *",
    catchup=False,
    tags=["Data Migration", "MySQL", "Clickhouse", "P1"],
) as dag:
    start_dag = BashOperator(task_id="start_dag", bash_command="date")

    end_dag = BashOperator(
        task_id="end_dag",
        on_success_callback=dag_success_callback,
        bash_command="date",
    )

    @task(task_id="create_connections")
    def create_connection(**kwargs):
        """Creating Connections for Dags"""
        from data_lake.clickhouse import create_clickhouse_connection
        from data_lake.database_mysql import create_database_connection
        from data_lake.aws_s3 import create_aws_connection
        from data_lake.email import create_email_connection

        create_clickhouse_connection()
        create_database_connection()
        create_aws_connection()
        create_email_connection()

    def add_data_to_clickhouse(fileUrl, table_name):
        import pandas as pd
        import boto3
        from io import BytesIO
        from airflow.models import Variable

        s3 = boto3.client(
            "s3",
            aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
            region_name=Variable.get("AWS_REGION"),
        )
        obj = s3.get_object(Bucket=Variable.get("AWS_S3_BUCKET"), Key=fileUrl)

        df = pd.read_parquet(BytesIO(obj["Body"].read()))
        clickhouse_conn_id = "clickhouse_con"

        df.info()

        clickhouse_hook = ClickHouseHook(clickhouse_conn_id)

        # Insert New Data to Clickhouse
        condition = df["createdAt"] == df["updatedAt"]
        filtered_data = df[condition]
        values = [tuple(row) for row in filtered_data.values]
        columns = filtered_data.columns.tolist()
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES"
        if len(df.index) != 0:
            clickhouse_hook.get_conn().execute(query, values)

        # Update Older Data in Clickhouse
        condition = df["createdAt"] != df["updatedAt"]
        filtered_data = df[condition]
        if len(filtered_data.index) != 0:
            # Check if 'id' is a string and quote each ID if necessary
            ids = ", ".join(
                f"'{str(i)}'" if isinstance(i, str) else str(i)
                for i in filtered_data["id"].tolist()
            )
            # DELETE query with appropriate handling for string IDs
            query = f"DELETE FROM {table_name} WHERE id IN ({ids}) SETTINGS mutations_sync=2"
            clickhouse_hook.get_conn().execute(query)
            values = [tuple(row) for row in filtered_data.values]
            columns = filtered_data.columns.tolist()
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES"
            clickhouse_hook.get_conn().execute(query, values)

    import datetime
    import pytz

    # Change as per need
    # Get the current time in UTC
    now_utc = datetime.datetime.now(pytz.utc)

    # Convert to the desired timezone (e.g., US/Eastern)
    ist = pytz.timezone("Asia/Kolkata")

    start_date = now_utc.astimezone(ist).date()
    start_date -= timedelta(days=1)

    end_date = now_utc.astimezone(ist).date()
    end_date -= timedelta(days=1)

    start_timestamp = start_date.strftime("%Y-%m-%d 00:00:00")
    end_timestamp = end_date.strftime("%Y-%m-%d 23:59:59")
    start_date_display = start_date.strftime("%Y-%m-%d")

    t1 = create_connection()

    mysql_connection = Connection.get_connection_from_secrets("mysql_con")

    list_of_tables = []

    db_conn = mysql.connector.connect(
        host=mysql_connection.host,
        user=mysql_connection.login,
        password=mysql_connection.password,
        database="job_hai",
    )
    cursor = db_conn.cursor()
    cursor.execute(
        "SELECT id, destination_table_name as table_name, query as `sql`, source, destination_schema FROM job_hai.AdhocDags where is_active = 1"
    )

    rows = cursor.fetchall()

    if rows is None:
        rows = []

    if len(rows) > 0:
        list_of_tables = rows

    for one_row in list_of_tables:

        load_dataset_in_clickhouse = PythonOperator(
            task_id=f"load_task_{one_row[1]}",
            python_callable=add_data_to_clickhouse,
            op_kwargs={
                "fileUrl": "{{ var.value.get('ENV') }}"
                + f"/DynamicDags/{one_row[1]}/{start_date_display}/data.parquet",
                "table_name": one_row[1],
            },
            outlets=[Dataset("s3://DynamicDag/" + one_row[1])],
            dag=dag,
        )

        if one_row[3] == "CLICKHOUSE":
            (
                t1
                >> ClickhouseToS3Operator(
                    task_id=f"clickhouse_to_s3_task_{one_row[1]}",
                    clickhouse_conn_id="clickhouse_con",
                    query=one_row[2],
                    parameters={
                        "start_timestamp": start_timestamp,
                        "end_timestamp": end_timestamp,
                    },
                    s3_bucket="{{ var.value.get('AWS_S3_BUCKET') }}",
                    file_format="parquet",
                    s3_key="{{ var.value.get('ENV') }}"
                    + f"/DynamicDags/{one_row[1]}/{start_date_display}/data.parquet",
                    replace=True,
                )
                >> load_dataset_in_clickhouse
            )
        else:
            (
                t1
                >> SqlToS3Operator(
                    task_id=f"sql_to_s3_task_{one_row[1]}",
                    sql_conn_id="mysql_con",
                    query=one_row[2],
                    parameters={
                        "start_timestamp": start_timestamp,
                        "end_timestamp": end_timestamp,
                    },
                    s3_bucket="{{ var.value.get('AWS_S3_BUCKET') }}",
                    file_format="parquet",
                    s3_key="{{ var.value.get('ENV') }}"
                    + f"/DynamicDags/{one_row[1]}/{start_date_display}/data.parquet",
                    replace=True,
                )
                >> load_dataset_in_clickhouse
            )

        load_dataset_in_clickhouse.set_downstream(end_dag)

    start_dag >> t1
    end_dag
