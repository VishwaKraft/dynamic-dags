from airflow import settings
from airflow.decorators import dag, task
from typing import List, Dict
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
from airflow.models import Connection
from pendulum import datetime

def fetch_tasks_from_db(conn_id: str) -> List[Dict[str, str]]:
    """Retrieve tasks from the respective database."""
    conn = BaseHook.get_connection(conn_id)
    
    # Construct the database connection URL
    db_url = f"{conn.conn_type}://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = create_engine(db_url)
    
    query = "SELECT table_name, owner, retry FROM dynamic_dags WHERE is_active = 1 and schedule = '@daily' "  # Modify based on your schema
    
    with engine.connect() as connection:
        result = connection.execute(text(query))
        tasks = [
            {
                'table_name': row[0], 
                'owner': row[1], 
                'retry' : row[2],
            } for row in result
        ]
    return tasks

def create_dag(dag_id, schedule, dag_number, default_args):
    @dag(dag_id=dag_id, schedule=schedule, default_args=default_args, catchup=False)
    def hello_world_dag():
        @task()
        def hello_world():
            print("Hello World")
            print("This is DAG: {}".format(str(dag_number)))

        hello_world()

    generated_dag = hello_world_dag()

    return generated_dag


session = settings.Session()

# adjust the filter criteria to filter which of your connections to use 
# to generated your DAGs
conns = (
    session.query(Connection.conn_id)
    .filter(Connection.conn_id.ilike("%DATABASE%"))
    .all()
)

for conn in conns:
    dag_number = conn
    tasks = fetch_tasks_from_db(conn[0])
    for t in tasks:

        default_args = { 
            'start_date': datetime(2023, 1, 1),
        }

        default_args = {**default_args, **t}

        schedule = '@daily'

        dag_id = "dynamic_dag_{}_{}".format(conn[0], t['table_name']).lower()
        globals()[dag_id] = create_dag(dag_id, schedule, dag_number, default_args)
