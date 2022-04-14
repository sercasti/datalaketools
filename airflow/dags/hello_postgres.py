from airflow import DAG
# path for Airflow version 1.10.9:
from airflow.operators.postgres_operator import PostgresOperator  
# path for Airflow version 2.4.0:
# from airflow.providers.postgres.operators.postgres import PostgresOperator  
import datetime

with DAG(
    dag_id="hello_postgres_postgres_operator",
    start_date=datetime.datetime(2021, 12, 17),
    schedule_interval=None,
    catchup=False,
) as dag:
    drop_table_if_exists = PostgresOperator(
        postgres_conn_id='mypostgres_connection',
        task_id="drop_table_if_exists",
        autocommit=True,
        sql="""
           DROP TABLE IF EXISTS testing_connection;
          """,
    )
    create_table = PostgresOperator(
        postgres_conn_id='mypostgres_connection',
        task_id="create_table",
        autocommit=True,
        sql="""
           CREATE TABLE testing_connection (
                dummy_column INTEGER NOT NULL PRIMARY KEY
            );
           
          """,
    ) 
    drop_table_if_exists > create_table