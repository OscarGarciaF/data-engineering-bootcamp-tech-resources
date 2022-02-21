import airflow.utils.dates
from airflow import DAG
import json
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.hooks.redshift import RedshiftHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks import S3Hook



s3_external_queries = """
create external schema if not exists s3_schema
from data catalog
database 's3_db'
iam_role 'arn:aws:iam::704943069631:role/service-role/AmazonRedshift-CommandsAccessRole-20220215T161638'
create external database if not exists;

drop table if exists s3_schema.log_reviews;

create external table s3_schema.log_reviews(
  logDate varchar,
  device varchar,
  location varchar,
  os varchar,
  ipAddress varchar,
  phoneNumber varchar
)
stored as PARQUET
LOCATION 's3://oscar-airflow-bucket/silver/log_reviews_parsed/';

drop table if exists s3_schema.movie_reviews;

create external table s3_schema.movie_reviews(
  cid varchar,
  positive_review integer,
  id_review varchar
)
stored as PARQUET
LOCATION 's3://oscar-airflow-bucket/silver/movie_reviews/';

drop table if exists s3_schema.user_purchase;

create external table s3_schema.user_purchase(
  invoice_number varchar,
  stock_code varchar,
  detail varchar,
  quantity int,
  invoice_date timestamp,
  unit_price numeric,                           
  customer_id int,
  country varchar
)
row format delimited
fields terminated by ','
stored as textfile
location 's3://oscar-airflow-bucket/bronze/user_purchase_manifest.json'
table properties ('skip.header.line.count'='1');
"""

golden_view_query = """
CREATE OR REPLACE VIEW golden_aggs AS

WITH review_analytics AS( 
SELECT CAST(cid AS INTEGER), SUM(positive_review) AS review_score , COUNT(cid) AS review_count
FROM s3_schema.movie_reviews
WHERE cid IS NOT NULL 
GROUP BY cid),

user_analytics AS(
SELECT customer_id, CAST(SUM(quantity * unit_price) AS DECIMAL(18, 5)) AS amount_spent 
FROM s3_schema.user_purchase 
WHERE customer_id IS NOT NULL GROUP BY customer_id)

SELECT COALESCE(ua.customer_id, ra.cid) AS customer_id, COALESCE(amount_spent, 0) AS amount_spent,
COALESCE(review_score, 0) AS review_score, COALESCE(review_count, 0) AS review_count, CURRENT_DATE AS insert_date                      
FROM review_analytics ra
FULL JOIN user_analytics ua ON ra.cid = ua.customer_id
WITH NO SCHEMA BINDING;
"""

q1_query = """
SELECT COUNT(*), location
FROM s3_schema.log_reviews
WHERE location IN ('California', 'New York', 'Texas')
GROUP BY location
ORDER BY location;

"""

q2_query = """
SELECT COUNT(*), os, location
FROM s3_schema.log_reviews
WHERE location IN ('California', 'New York', 'Texas')
GROUP BY os, location
ORDER BY os, location;
"""

q3_query = """
SELECT COUNT(*), device, location 
FROM s3_schema.log_reviews
WHERE device = 'Computer'
GROUP BY location, device
ORDER BY count DESC;
"""

q4_query = """
WITH count_year AS (
SELECT COUNT(*), EXTRACT(YEAR from TO_DATE(logDate, 'MM-DD-YYYY')) AS year, location 
FROM s3_schema.log_reviews
WHERE year = 2021
GROUP BY location, year)

(SELECT * 
FROM count_year
ORDER BY count DESC
LIMIT 1) 
UNION
(SELECT * 
FROM count_year
ORDER BY count ASC
LIMIT 1);

"""

q5_query = """
WITH log_region AS (
  SELECT COUNT(*), device, 
  CASE 
      WHEN location IN ('Alaska',
                       'Arizona',
                       'California',
                       'Colorado',
                       'Hawaii',
                       'Idaho',
                       'Montana',
                       'Nevada',
                       'New Mexico',
                       'Oregon',
                       'Utah',
                       'Washington',
                       'Wyoming')

                                  THEN 'West'
          ELSE 'East'
  END
  AS region
  FROM s3_schema.log_reviews
  GROUP BY device, region
  )
  

SELECT l.count, l.device, l.region
FROM (
  SELECT MAX(count) AS max, region
  FROM log_region
  GROUP BY region) m 
JOIN log_region l on l.count = m.max
ORDER BY count DESC;
"""


s3_key = ""
s3_bucket = ""

default_args = {
    'owner': 'oscar.garcia',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG('dag_golden_redshift', default_args = default_args, schedule_interval = '@daily')


def _query_redshift(sql_query, **context):
    """
    Queries Postgres and returns a cursor to the results.
    """

    postgres = RedshiftHook()
    conn = postgres.get_conn()
    cursor = conn.cursor()
    executed_cursor = cursor.execute(sql_query)

    # iterate over to get a list of dicts
    details_dicts = [doc for doc in cursor]

    # serialize to json string
    details_json_string = json.dumps(details_dicts)


    return details_json_string

def save_ans_s3(**context):
    ans =  {"Q1" :  context['ti'].xcom_pull(task_ids='task_Q1'),
            "Q2" :  context['ti'].xcom_pull(task_ids='task_Q2'),
            "Q3" :  context['ti'].xcom_pull(task_ids='task_Q3'),
            "Q4" :  context['ti'].xcom_pull(task_ids='task_Q4'),
            "Q5" :  context['ti'].xcom_pull(task_ids='task_Q5')
            }

    hook = S3Hook(aws_conn_id="s3_connection")

    hook.load_string(json.dumps(ans), s3_key, bucket_name=s3_bucket, replace=True)


    

task_setup_external_tables = RedshiftSQLOperator(
        task_id='setup_external_tables',
        redshift_conn_id="redshift_default",
        sql= s3_external_queries,
        dag = dag
        )

task_setup_metrics_view = RedshiftSQLOperator(
        task_id='setup_metrics_view',
        redshift_conn_id="redshift_default",
        sql= golden_view_query,
        dag = dag
        )

t_q1 = PythonOperator(
    task_id='task_Q1',
    python_callable= _query_redshift,
    op_kwargs = {"sql_query" : q1_query},
    dag=dag,
)

t_q2 = PythonOperator(
    task_id='task_Q2',
    python_callable= _query_redshift,
    op_kwargs = {"sql_query" : q2_query},
    dag=dag,
)

t_q3 = PythonOperator(
    task_id='task_Q1',
    python_callable= _query_redshift,
    op_kwargs = {"sql_query" : q3_query},
    dag=dag,
)

t_q4 = PythonOperator(
    task_id='task_Q1',
    python_callable= _query_redshift,
    op_kwargs = {"sql_query" : q4_query},
    dag=dag,
)

t_q5 = PythonOperator(
    task_id='task_Q1',
    python_callable= _query_redshift,
    op_kwargs = {"sql_query" : q5_query},
    dag=dag,
)

t_save_ans = PythonOperator(
    task_id='task_save_answers_s3',
    python_callable= save_ans_s3,
    dag=dag,
)

task_setup_external_tables >> task_setup_metrics_view >> t_q1 >> t_q2 >> t_q2 >> t_q3 >> t_q4 >> t_q5 >> t_save_ans