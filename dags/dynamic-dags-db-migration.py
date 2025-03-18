from airflow import DAG
import mysql.connector
from airflow import settings
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.models import Connection
from datetime import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook
from typing import List, Dict, Any
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
from airflow.models import Connection
from pendulum import datetime

def create_table_and_insert(connection):    
    try:
        conn = MySqlHook(mysql_conn_id=connection).get_conn()
        cursor = conn.cursor()
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS `dynamic_dags` (
                `id` int NOT NULL AUTO_INCREMENT,
                `table_name` varchar(255) DEFAULT NULL,
                `query` text,
                `source` enum('MYSQL') DEFAULT 'MYSQL',
                `owner` varchar(255) DEFAULT NULL,
                `schedule` enum('@daily', '@hourly') DEFAULT '@daily',
                `retry` int Default 0,
                `is_active` tinyint(1) DEFAULT 0,
                `createdAt` datetime NOT NULL,
                `updatedAt` datetime NOT NULL,
                PRIMARY KEY (`id`),
                UNIQUE KEY `table_name` (`table_name`)
            ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        """
        cursor.execute(create_table_query)
        
        insert_query = '''
            INSERT INTO `dynamic_dags` (
                `table_name`, 
                `query`, 
                `source`, 
                `owner`,
                `schedule`, 
                `retry`, 
                `is_active`, 
                `createdAt`, 
                `updatedAt`
            ) VALUES (
                'job_data_sync', 
                'SELECT now()', 
                'MYSQL', 
                'admin', 
                '@daily',
                3, 
                1, 
                NOW(), 
                NOW()
            );
        '''
        values = ()
        cursor.execute(insert_query, values)
        
        conn.commit()
        print("Table created and one entry added successfully.")
    
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'dynamic_dag_table_migration',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
)

session = settings.Session()

# adjust the filter criteria to filter which of your connections to use 
# to generated your DAGs
conns = (
    session.query(Connection.conn_id)
    .filter(Connection.conn_id.ilike("%DATABASE%"))
    .all()
)

for conn in conns:
    task_id = "create_table_and_insert_{}".format(conn[0])
    task = PythonOperator(
        task_id=task_id,
        python_callable=create_table_and_insert,
        op_kwargs={'connection':conn[0]},
        dag=dag,
    )
