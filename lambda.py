import json
import boto3
import pandas as pd
import io
import os

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

DEFAULT_SNS_TOPIC_ARN = 'arn:aws:sns:ap-south-1:674938244258:doordash-raw-data'
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN', DEFAULT_SNS_TOPIC_ARN)
TARGET_BUCKET = 'doordash-target-demo-1'

def lambda_handler(event, context):
    try:
        # Get the uploaded file's bucket and key
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        
        print(f"Processing file: s3://{source_bucket}/{object_key}")
        
        # Download the file from S3
        response = s3_client.get_object(Bucket=source_bucket, Key=object_key)
        content = response['Body'].read()

        # Load JSON into pandas DataFrame
        df = pd.read_json(io.BytesIO(content), lines=True)

        # Filter where status == delivered
        filtered_df = df[df['status'] == 'delivered']

        # Write filtered data to a new JSON
        output_key = object_key.replace('.json', '_delivered.json')
        output_buffer = io.BytesIO()
        filtered_df.to_json(output_buffer, orient='records', lines=True)
        output_buffer.seek(0)

        # Upload to target bucket
        s3_client.put_object(Bucket=TARGET_BUCKET, Key=output_key, Body=output_buffer)

        # Publish success message
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject='Doordash File Processed Successfully',
            Message=f"File '{object_key}' was filtered and stored as '{output_key}' in bucket '{TARGET_BUCKET}'."
        )

        return {
            'statusCode': 200,
            'body': f"Successfully processed {object_key}"
        }

    except Exception as e:
        error_msg = f"Error processing file {object_key}: {str(e)}"
        print(error_msg)

        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject='Doordash File Processing Failed',
            Message=error_msg
        )

        return {
            'statusCode': 500,
            'body': error_msg
        }
