import airflow.utils.dates

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from custom_dags.dag_s3_to_postgres import S3ToPostgresTransfer

default_args = {
    'owner': 'oscar.garcia',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG('dag_insert_data', default_args = default_args, schedule_interval = '@daily')

process_dag = S3ToPostgresTransfer(
    task_id = 'dag_s3_to_postgres',
    schema = 'bronze',
    table= 'user_purchase',
    s3_bucket = 'oscar-airflow-bucket',
    s3_key =  'user_purchase.csv',
    aws_conn_postgres_id = 'postgres_default',
    aws_conn_id = 'aws_default',   
    dag = dag
)

process_dag