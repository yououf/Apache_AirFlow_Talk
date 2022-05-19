from datetime import timedelta
from textwrap import dedent
from utils import access_key,secret_access_key
from datetime import datetime
import boto3

def send_data_to_sqs(**kwargs):
    sqs = boto3.client(
        "sqs",
        aws_access_key_id='AKIAQM37ZSX4KZC75XMB',
        aws_secret_access_key='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',
        endpoint_url="https://sqs.us-east-1.amazonaws.com/027648431608/airflow-demo-queue",
        region_name="us-east-1",
    )
    queue_url = "https://sqs.us-east-1.amazonaws.com/027648431608/airflow-demo-queue"
    resp = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=(str(kwargs["execution_date"]))
    )

if __name__ == '__main__':
    send_data_to_sqs()