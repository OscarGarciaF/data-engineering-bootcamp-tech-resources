# pyspark
import argparse
from os import listdir

from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import array_contains


def pyspark_script(input_loc, output_loc):

    df_raw = spark.read.option("header", True).csv(input_loc)

    tokenizer = Tokenizer(outputCol="words")
    tokenizer.setInputCol("review_str")

    remover = StopWordsRemover()
    remover.setInputCol("words")
    remover.setOutputCol("clean_words")

    df = tokenizer.transform(df_raw)
    df = remover.transform(df)

    df = df.withColumn("positive_review", array_contains(df.clean_words, "good").cast('integer'))

    df_out = df.select("cid", "positive_review")
    df_out.write.mode("overwrite").parquet(output_loc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    BUCKET_NAME = "oscar-airflow-bucket"
    s3_data = "bronze/movie_review.csv"
    s3_clean = "silver/movie_reviews"
    parser.add_argument("--input", type=str, help="HDFS input", default=f"s3://{BUCKET_NAME}/{s3_data}")
    parser.add_argument("--output", type=str, help="HDFS output", default=f"s3://{BUCKET_NAME}/{s3_clean}")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("process movie review script").getOrCreate()
    pyspark_script(input_loc=args.input, output_loc=args.output)
