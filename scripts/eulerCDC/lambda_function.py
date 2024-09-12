import boto3
import json
import pandas as pd
import io
import uuid

s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')
glue_client = boto3.client('glue')

RAW_BUCKET_NAME = 'euler-data-lake'
TEMP_FOLDER = 'temp/'

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        name = record['s3']['object']['key']
        version_id = record['s3']['object'].get('versionId', 'null')
        
        event_name = record['eventName']
        
        if 'Put' in event_name:
            size = record['s3']['object']['size']
            if size >= 0.0001 * 1024 * 1024:  # 25MB
                process_large_file(bucket, name, version_id)
            else:
                process_small_file(bucket, name, size, version_id)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }

def process_large_file(bucket, name, version_id):
    print(f"Processing large file: {name}")
    
    # Get the current version of the file
    current_df = get_dataframe(bucket, name, version_id)
    
    # Get the nearest previous version
    previous_version = get_previous_version(bucket, name, version_id)
    if previous_version:
        previous_df = get_dataframe(bucket, name, previous_version)
        
        # Find new alert_ids
        new_alerts = current_df[~current_df['alert_id'].isin(previous_df['alert_id'])]
        
        if not new_alerts.empty:
            # Save new alerts to temp file
            temp_filename = TEMP_FOLDER + 'new_alerts_' + str(uuid.uuid4()) + '_' + name.split('/')[-1]
            save_to_temp(new_alerts, bucket, temp_filename)
            
            # Call Glue job to process the temp file
            call_glue_job(bucket, temp_filename)
        else:
            print("No new alerts found.")
    else:
        print("No previous version found. Processing entire file.")
        call_glue_job(bucket, name)

def process_small_file(bucket, name, size, version_id):
    print(f"Processing small file: {name}")
    input_data = {
        'versionId': version_id,
        'name': name,
        'bucket': bucket
    }
    response = lambda_client.invoke(
        FunctionName='arn:aws:lambda:us-east-1:666243375423:function:eulerprocessdata',
        Payload=json.dumps(input_data)
    )
    print(f"Invoked Lambda function for small file: {name}")

def get_dataframe(bucket, key, version_id):
    response = s3.get_object(Bucket=bucket, Key=key, VersionId=version_id)
    return pd.read_csv(io.BytesIO(response['Body'].read()))

def get_previous_version(bucket, key, current_version_id):
    versions = s3.list_object_versions(Bucket=bucket, Prefix=key)['Versions']
    for version in versions:
        if version['VersionId'] != current_version_id:
            return version['VersionId']
    return None

def save_to_temp(df, bucket, filename):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=filename, Body=csv_buffer.getvalue())
    print(f"Saved temp file: {filename}")

def call_glue_job(bucket, filename):
    glue_client.start_job_run(
        JobName='eulergluejob',
        Arguments={
            '--name': filename,
            '--bucket': bucket
        }
    )
    print(f"Started Glue job for file: {filename}")
    
    # Delete temp file after Glue job is started
    delete_temp_file(bucket, filename)

def delete_temp_file(bucket, filename):
    s3.delete_object(Bucket=bucket, Key=filename)
    print(f"Deleted temp file: {filename}")
