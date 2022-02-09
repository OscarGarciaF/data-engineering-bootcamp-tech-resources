# pyspark
import argparse
from os import listdir

from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import array_contains


def pyspark_script(input_loc, output_loc):

    df_raw = spark.read.format("com.databricks.spark.xml").option("header", True).xml(input_loc)
    df_out = df_raw.drop("log")
    df_out.printSchema()

    
    df_out.write.mode("overwrite").parquet(output_loc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    BUCKET_NAME = "oscar-airflow-bucket"
    s3_data = "bronze/log_reviews.csv"
    s3_clean = "silver/log_reviews/"
    parser.add_argument("--input", type=str, help="HDFS input", default=f"s3://{BUCKET_NAME}/{s3_data}")
    parser.add_argument("--output", type=str, help="HDFS output", default=f"s3://{BUCKET_NAME}/{s3_clean}")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("Text classifier").getOrCreate()
    pyspark_script(input_loc=args.input, output_loc=args.output)
