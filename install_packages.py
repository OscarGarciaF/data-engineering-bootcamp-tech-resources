import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'oscar.garcia',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG('dag_install_packages', default_args = default_args, schedule_interval = '@daily')

task_pip = BashOperator(
            task_id='task_pip',
            bash_command="pip install 'apache-airflow-providers-amazon'",
            dag = dag
        )

task_pip3 = BashOperator(
    task_id='task_pip3',
    bash_command="pip3 install 'apache-airflow-providers-amazon'",
    dag = dag
)

task_pip_p = BashOperator(
    task_id='task_pip3_python',
    bash_command='python3 -m pip install "apache-airflow-providers-amazon"',
    dag = dag
)

task_pip_apache = BashOperator(
            task_id='task_pip_apache',
            bash_command="pip install apache-airflow[amazon]",
            dag = dag
        )

task_pip3_apache = BashOperator(
    task_id='task_pip3_apache',
    bash_command="pip3 install apache-airflow[amazon]",
    dag = dag
)



task_pip >> task_pip3 >> task_pip_p >> task_pip_apache >> task_pip3_apache

