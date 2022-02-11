# pyspark
import argparse
from os import listdir

from pyspark.sql import *
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import *
from os import environ
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import _parse_datatype_json_string

def ext_schema_of_xml_df(df, options={}):
    assert len(df.columns) == 1

    scala_options = spark._jvm.PythonUtils.toScalaMap(options)
    java_xml_module = getattr(getattr(
        spark._jvm.com.databricks.spark.xml, "package$"), "MODULE$")
    java_schema = java_xml_module.schema_of_xml_df(df._jdf, scala_options)
    return _parse_datatype_json_string(java_schema.json())

def ext_from_xml(xml_column, schema, options={}):
    java_column = _to_java_column(xml_column.cast('string'))
    java_schema = spark._jsparkSession.parseDataType(schema.json())
    scala_map = spark._jvm.org.apache.spark.api.python.PythonUtils.toScalaMap(options)
    jc = spark._jvm.com.databricks.spark.xml.functions.from_xml(
        java_column, java_schema, scala_map)
    return Column(jc)





def pyspark_script(input_loc, output_loc):

    print("starting")

    df_raw = spark.read.options(header = True).csv(input_loc)

    #df_raw.printSchema()
    #print(df_raw.head(5))

    payloadSchema = ext_schema_of_xml_df(df_raw.select("log"))
    df_raw = df_raw.withColumn("parsed", ext_from_xml(df_raw.log, payloadSchema))
    df_raw = df_raw.drop("log")
    df_out = df_raw.select("id_review", "parsed.log.*")

    #df_out.printSchema()
    #print(df_out.head(5))

    df_out.write.mode("overwrite").parquet(output_loc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    BUCKET_NAME = "oscar-airflow-bucket"
    s3_data = "bronze/log_reviews.csv"
    s3_clean = "silver/log_reviews_parsed/"
    parser.add_argument("--input", type=str, help="HDFS input", default=f"s3://{BUCKET_NAME}/{s3_data}")
    parser.add_argument("--output", type=str, help="HDFS output", default=f"s3://{BUCKET_NAME}/{s3_clean}")
    args = parser.parse_args()
    environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.14.0 pyspark-shell' 
    spark = SparkSession.builder.appName("process log reviews script").getOrCreate()
    pyspark_script(input_loc=args.input, output_loc=args.output)
