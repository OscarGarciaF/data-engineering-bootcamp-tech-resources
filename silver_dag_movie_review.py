import airflow.utils.dates

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from custom_dags.dag_s3_to_postgres import S3ToPostgresTransfer
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from airflow.contrib.sensors.emr_step_sensor import  EmrStepSensor

default_args = {
    'owner': 'oscar.garcia',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG('silver_dag_movie_review', default_args = default_args, schedule_interval = '@daily')

BUCKET_NAME = "oscar-airflow-bucket"
s3_data = "bronze/movie_review.csv"
s3_script = "dags/scripts/process_movie_review.py"
s3_clean = "silver/reviews/"
logs_location = "logs"

SPARK_STEPS = [ 
    {
        "Name": "Process silver data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                f"s3://{BUCKET_NAME}/{s3_script}",
            ],
        },
    }
]


JOB_FLOW_OVERRIDES = {
    "Name": "Process silver schema",
    "ReleaseLabel": "emr-5.34.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], # We want our EMR cluster to have HDFS and Spark
    "LogUri" : f"s3://{BUCKET_NAME}/{logs_location}",
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
                "InstanceType": "m1.medium",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m1.medium",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
        #"Ec2SubnetIds": ['subnet-05a8630d423a915a5', 'subnet-0d30cafcd1055a14b', 'subnet-03b36045df0954da2', 'subnet-02a105a1adcdc38ac'] 
        "Ec2SubnetIds": ['subnet-0aece929207a7df91']
    },
    'Steps': SPARK_STEPS,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

# Create an EMR cluster



create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

job_sensor = EmrJobFlowSensor(task_id='check_job_flow',
 job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
 dag = dag)



create_emr_cluster >> job_sensor


