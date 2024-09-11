import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

import boto3
import io
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import round

# Get arguments
args = getResolvedOptions(sys.argv, ['name', 'bucket'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Parameters from the event or predefined
processing_name = args['name']
processing_bucket = args['bucket']
bucket_output = 'euler-trusted-bucket'
temporary_folder = 'temporary_output_directory/'
parquet_filename = processing_name.replace(".csv", ".parquet")

# Initialize boto3 client
s3 = boto3.client('s3')

# Read the CSV file from S3 into a Spark DataFrame
input_path = f"s3://{processing_bucket}/{processing_name}"
df = spark.read.csv(input_path, 
                     header=True, 
                     inferSchema=True, 
                     escape='"', 
                     quote='"')

# Pre-process data
alert_df = df.select(
    "_index",
    F.from_json(F.col("alert"), "struct<_id:long, alert_id:string, timestamp:string, device_product:string, count:string, logsource:string>").alias("alert")
)

alert_df = alert_df.withColumn("logsource", F.col("alert.logsource")) \
                   .withColumn("device_product", F.col("alert.device_product")) \
                   .withColumn("timestamp", F.from_unixtime(F.col("alert.timestamp") / 1000)) \
                   .drop("alert") \
                   .withColumn("logsource", F.when(F.col("logsource").isNull(), "none").otherwise(F.col("logsource"))) \
                   .withColumnRenamed("logsource", "logsource_kian")

# Process the "events" column
events_schema = ArrayType(StructType([
    StructField("category", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("computer_name", StringType(), True),
    StructField("customer_group", StringType(), True),
    StructField("device_product", StringType(), True),
    StructField("duser", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("extra_data", StringType(), True),
    StructField("group_domain", StringType(), True),
    StructField("group_name", StringType(), True),
    StructField("group_sid", StringType(), True),
    StructField("last_updated", StringType(), True),
    StructField("local_timestamp", StringType(), True),
    StructField("log_name", StringType(), True),
    StructField("log_parser", StringType(), True),
    StructField("log_source", StringType(), True),
    StructField("message", StringType(), True),
    StructField("organization_group", StringType(), True),
    StructField("process_id", StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("reference", StringType(), True),
    StructField("referer", StringType(), True),
    StructField("release_level", StringType(), True),
    StructField("server_id", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("shift", StringType(), True),
    StructField("signature_id", StringType(), True),
    StructField("source_log", StringType(), True),
    StructField("source_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("target_user_name", StringType(), True),
    StructField("target_user_sid", StringType(), True),
    StructField("user_domain", StringType(), True),
    StructField("user_logon_id", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("user_privilege_list", StringType(), True),
    StructField("user_sid", StringType(), True),
    StructField("user_update", StringType(), True)
]))

events_df = df.select(
    "_index",
    F.from_json(F.col("events"), events_schema).alias("parsed_events")
)

events_df = events_df.selectExpr("_index", "explode(parsed_events) as event").select("_index", "event.*") \
                     .withColumnRenamed("device_product", "origin_device_product") \
                     .filter(F.col("origin_device_product") != "KIAN")

# Concatenate DataFrames
selected_df = df.select("_index", "alert_id", "rule_id", "tagged")
selected_alert_df = alert_df.select("_index", "timestamp", "device_product", "logsource_kian")

df_ = selected_df.join(events_df, on="_index", how="outer") \
                 .join(selected_alert_df, on=["_index"], how="outer") \
                 .dropna(how='all')

# Select columns for reporting task
df_report = df_.select("timestamp", "alert_id", "rule_id", "origin_device_product", "device_product", "logsource_kian", "tagged").dropDuplicates()


# Repartition to a single partition for a single output file
df_report = df_report.coalesce(1)

# Write the DataFrame back to S3 as Parquet (to a temporary directory)
output_path = f"s3://{bucket_output}/{temporary_folder}"
df_report.write.parquet(output_path, mode='overwrite')

# Rename the part file to the desired file name
s3 = boto3.client('s3')
response = s3.list_objects_v2(Bucket=bucket_output, Prefix=temporary_folder)

# Check if any objects were returned
if 'Contents' in response:
    # Get the part file name (Parquet files usually have the extension .parquet)
    part_file = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')][0]

    # Define the new file name
    output_file = "processed_" + parquet_filename

    # Copy the part file to the desired file name
    s3.copy_object(Bucket=bucket_output, CopySource=f'{bucket_output}/{part_file}', Key=output_file)

    # Delete the temporary folder and files
    for obj in response['Contents']:
        s3.delete_object(Bucket=bucket_output, Key=obj['Key'])
