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
