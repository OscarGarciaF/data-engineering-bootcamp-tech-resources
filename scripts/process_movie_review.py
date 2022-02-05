# pyspark
import argparse

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

    df_out = df.withColumn("positive_review", array_contains(df.clean_words, "good").cast('integer'))

    filename = "reviews"
    df_out.write.mode("overwrite").parquet(output_loc + "/" + filename)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, help="HDFS input", default="/input")
    parser.add_argument("--output", type=str, help="HDFS output", default="/output")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("Text classifier").getOrCreate()
    pyspark_script(input_loc=args.input, output_loc=args.output)
