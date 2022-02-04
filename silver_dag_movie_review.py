import airflow.utils.dates

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from custom_dags.dag_s3_to_postgres import S3ToPostgresTransfer
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor

default_args = {
    'owner': 'oscar.garcia',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG('silver_dag_movie_review', default_args = default_args, schedule_interval = '@daily')



SPARK_STEPS = [ # Note the params values are supplied to the operator
    {
        "Name": "Move raw data from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp ",
                "--src=s3://{{ params.BUCKET_NAME }}/{{ params.input_schema}} ",
                "--dest=/input ",
            ],
        },
    },
    {
        "Name": "Process silver data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit ",
                "--deploy-mode ",
                "client ",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }} ",
            ],
        },
    },
    {
        "Name": "Move clean data from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp ",
                "--src=/output ",
                "--dest=s3://{{ params.BUCKET_NAME }}/{{ params.output_schema}} ",
            ],
        },
    }
]


JOB_FLOW_OVERRIDES = {
    "Name": "Process silver schema",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], # We want our EMR cluster to have HDFS and Spark
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}, # by default EMR uses py2, change it to py3
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "t2.small",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "t2.small",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False, 
    },
    'Steps': SPARK_STEPS,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

# Create an EMR cluster

BUCKET_NAME = "oscar-airflow-bucket"
s3_data = "bronze/movie_review.csv"
s3_script = "dags/scripts/process_movie_review"
s3_clean = "silver/reviews"

create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    params={ # these params are used to fill the paramterized values in SPARK_STEPS json
        "BUCKET_NAME": BUCKET_NAME,
        "s3_data": s3_data,
        "s3_script": s3_script,
        "s3_clean": s3_clean,
    },
    dag=dag,
)

job_sensor = EmrJobFlowSensor(task_id='check_job_flow', job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}", dag = dag)

create_emr_cluster >> job_sensor

