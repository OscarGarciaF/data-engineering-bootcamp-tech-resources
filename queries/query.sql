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


--select * from s3_schema.log_reviews limit 5;
--select * from s3_schema.movie_reviews limit 5;
--select * from s3_schema.user_purchase limit 5;


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
FULL JOIN user_analytics ua ON ra.cid = ua.customer_id;