import pandas as pd
import boto3
import io
import json

def lambda_handler(event, context):
    processing_name = event['name']
    processing_bucket = event['bucket']
    processing_version_id = event['versionId']
    bucket_output = 'euler-trusted-bucket'
    
    #Load data
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket = processing_bucket, Key = processing_name, VersionId=processing_version_id)
    content = response['Body'].read().decode('utf-8')
    
    # # Extract details from the event
    # size = event['Records'][0]['s3']['object']['size']
    # name = event["Records"][0]["s3"]["object"]["key"]
    # bucket = event["Records"][0]["s3"]["bucket"]["name"]
    # arn = event['Records'][0]['s3']['bucket']['arn']

    # Read the CSV file into a DataFrame
    df = pd.read_csv(io.StringIO(content))

    # Parse the 'alert' column into a structured DataFrame
    alert_df = pd.DataFrame(json.loads(e) for e in df["alert"])

    # Select only the 'logsource' and 'device product' columns
    alert_df = alert_df[["timestamp","logsource", "device_product"]]
    
    # cast type 'timestamp' from unix to datetime
    alert_df['timestamp'] = pd.to_datetime(alert_df['timestamp'], unit='ms')

    # Rename the 'logsource' column to 'logsource_kian'
    alert_df = alert_df.rename(columns={'logsource': 'logsource_kian'})

    # Fill missing values in 'logsource_kian' with 'none'
    alert_df['logsource_kian'] = alert_df['logsource_kian'].fillna('none')

    # Extract events from the alert
    events_df = pd.DataFrame([json.loads(e)[0] if len(json.loads(e))>0 else {} for e in df["events"]])
    
    # Rename the "device_product" column to avoid duplicates
    events_df.rename(columns={'device_product': 'origin_device_product'}, inplace=True)

    # Select relevant columns
    selected_df = df[['alert_id', 'rule_id', 'tagged']]
    selected_alert_df = alert_df[['timestamp','device_product', 'logsource_kian']]
    selected_events_df = events_df[['origin_device_product']]

    # Concatenate DataFrames
    df_report = pd.concat([selected_df, selected_alert_df, selected_events_df], axis=1)
    df_report = df_report[["timestamp", "alert_id", "rule_id", "origin_device_product", "device_product", "logsource_kian", "tagged"]].drop_duplicates()

    #Export
    # Convert DataFrame to Parquet and store in a BytesIO buffer
    parquet_buffer = io.BytesIO()
    df_report.to_parquet(parquet_buffer, index=False)
    # Upload parquet to S3
    parquet_filename = processing_name.replace(".csv", ".parquet")
    output_name = "processed_" + parquet_filename
    s3.put_object(Bucket=bucket_output, Key=output_name, Body=parquet_buffer.getvalue())
    
    return {
        'status': 200
    }
