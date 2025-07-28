from datetime import datetime, timedelta
from pytz import timezone

from airflow import DAG
from airflow.decorators import task
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from callbacks.ms_teams_callback_functions_for_data_warehouse import (
    failure_callback,
    dag_success_callback,
)
from operators.clickhouse_to_s3_operator import (
    ClickhouseToS3Operator,
)

local_timezone = timezone("Asia/Kolkata")

default_args = {
    "owner": "Amol",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": failure_callback,
    "sla": timedelta(minutes=10),
}

with DAG(
    dag_id="dynamic_dag_one_time_clickhouse_sample",
    description="One time dump for sql query to s3",
    schedule_interval=None,
    start_date=datetime(2023, 4, 1, 10, tzinfo=local_timezone),
    catchup=False,
    params={
        "clickhouse_table_name": "",
        "query": "",
    },
    default_args=default_args,
    max_active_runs=1,
) as dag:

    start_dag = BashOperator(task_id="start_dag", bash_command="date")

    end_dag = DummyOperator(task_id="end_dag", on_success_callback=dag_success_callback)

    @task(task_id="create_connections")
    def create_connection(**kwargs):
        """Creating Connections for Dags"""
        from data_lake.database_mysql import create_database_connection
        from data_lake.aws_s3 import create_aws_connection
        from data_lake.email import create_email_connection

        create_database_connection()
        create_aws_connection()
        create_email_connection()

    def add_data_to_clickhouse(fileUrl, table_name):
        import pandas as pd
        import boto3
        from io import BytesIO
        from airflow.models import Variable
        from utils.set_if_null import set_if_null

        s3 = boto3.client(
            "s3",
            aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
            region_name=Variable.get("AWS_REGION"),
        )
        obj = s3.get_object(Bucket=Variable.get("AWS_S3_BUCKET"), Key=fileUrl)

        df = pd.read_parquet(BytesIO(obj["Body"].read()))
        clickhouse_conn_id = "clickhouse_con"

        set_if_null(df)

        df.info()

        clickhouse_hook = ClickHouseHook(clickhouse_conn_id)

        # Insert New Data to Clickhouse
        values = [tuple(row) for row in df.values]
        columns = df.columns.tolist()
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES"
        if len(df.index) != 0:
            clickhouse_hook.get_conn().execute(query, values)

    t1 = create_connection()

    # Define your Python function to wrap around the S3 task
    def process_data(**kwargs):
        import datetime

        date = datetime.date.today()
        # Access the params from the DAG config
        param = kwargs["params"].get("date", "1970-01-01")

        query = kwargs["params"].get("query", "")

        start_timestamp = date.strftime("{0} 00:00:00".format(param))
        end_timestamp = date.strftime("{0} 23:59:59".format(param))

        query = query.replace("%(start_timestamp)s", start_timestamp)
        query = query.replace("%(end_timestamp)s", end_timestamp)

        print(query)

        # Push XComs
        kwargs["ti"].xcom_push(key="rendered_query", value=query)
        kwargs["ti"].xcom_push(key="date", value=param)

    # Define the Python task
    python_task = PythonOperator(
        task_id="process_date_task", python_callable=process_data
    )

    get_dataset_to_s3 = ClickhouseToS3Operator(
        task_id="clickhouse_to_s3_task",
        clickhouse_conn_id="clickhouse_con",
        query="{{ task_instance.xcom_pull(task_ids='process_date_task', key='rendered_query') }}",
        parameters={},
        s3_bucket="{{ var.value.get('AWS_S3_BUCKET') }}",
        file_format="parquet",
        s3_key="{{ var.value.get('ENV') }}"
        + "/DynamicDags/"
        + "{{ params.clickhouse_table_name }}"
        + "/{{ task_instance.xcom_pull(task_ids='process_date_task', key='date') }}/data.parquet",
        replace=True,
    )

    load_data_to_clickhouse = PythonOperator(
        task_id="load_task",
        python_callable=add_data_to_clickhouse,
        op_kwargs={
            "fileUrl": "{{ var.value.get('ENV') }}"
            + "/DynamicDags/"
            + "{{ params.clickhouse_table_name }}"
            + "/{{ task_instance.xcom_pull(task_ids='process_date_task', key='date') }}/data.parquet",
            "table_name": "{{ params.clickhouse_table_name }}",
        },
        dag=dag,
    )
    python_task.set_upstream(t1)
    python_task >> get_dataset_to_s3 >> load_data_to_clickhouse
    load_data_to_clickhouse.set_downstream(end_dag)

    start_dag >> t1
    end_dag
