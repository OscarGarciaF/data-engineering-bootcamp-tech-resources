package com.processing
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import com.databricks.spark.xml.functions.from_xml
import com.databricks.spark.xml.schema_of_xml


object log_reviews {
  def main(args: Array[String]) {
        val input_loc = "s3://oscar-airflow-bucket/bronze/log_reviews.csv"
        val output_loc = "s3://oscar-airflow-bucket/silver/log_reviews_parsed"
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._

        val df_raw = spark.read.option("header", "true").csv(input_loc)

        val payloadSchema = schema_of_xml(df_raw.select("log").as[String])
        val df_parsed = df_raw.withColumn("parsed", from_xml($"log", payloadSchema))

        val df_parsed_clean = df_parsed.drop("log")
        val df_out = df_parsed_clean.select("id_review", "parsed.log.*")

        df_out.write.mode("overwrite").parquet(output_loc)
  }
}