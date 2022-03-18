name := "process_log_reviews_scala"

version := "1"

scalaVersion := "2.12.15"


libraryDependencies ++= Seq(
  "org.apache.spark"             %% "spark-core"           % "3.1.2",
  "org.apache.spark"             %% "spark-sql"            % "3.1.2",
  "com.fasterxml.jackson.core"    % "jackson-databind"     % "2.12.2",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.2",
  "org.apache.hadoop"             % "hadoop-client"        % "3.2.1",
  "org.apache.hadoop"             % "hadoop-aws"           % "3.2.1",
  "com.amazonaws"                 % "aws-java-sdk-s3"      % "1.11.375",
  "com.amazonaws"                 % "aws-java-sdk-core"    % "1.11.375",
  "com.databricks"                % "spark-xml_2.12"       % "0.14.0" 
)