from airflow import DAG
from postgres_ge import DataTransferPostgres
from datetime import datetime


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 5, 27),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": True,
}

with DAG(
    dag_id="pg-data-flow-ge",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    max_active_runs=1,
    tags=['data-flow-ge'],
) as dag1:
    t1 = DataTransferPostgres(
        config={'table': 'public.customer'},
        query='select * from customer',
        task_id='customer_ge',

        source_pg_conn_str="host='10.217.3.235' port='5433' dbname='srcdb' user='root' password='postgres'", # 'localhost' port='5433'
        pg_conn_str="host='10.217.3.235' port='5433' dbname='dstdb' user='root' password='postgres'",
        pg_meta_conn_str="host='10.217.3.235' port='5433' dbname='dstdb' user='root' password='postgres'",
    )
