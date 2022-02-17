import airflow.utils.dates


from airflow import DAG



default_args = {
    'owner': 'oscar.garcia',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG('dag_golden_redshift', default_args = default_args, schedule_interval = '@daily')

