from datetime import timedelta
from textwrap import dedent
from utils import access_key,secret_access_key
from datetime import datetime
import time
# The DAG object we'll need this to instantiate a DAG
from airflow import DAG
# Operators we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context, PythonOperator
import boto3


def send_data_to_sqs(**kwargs):
    sqs = boto3.client(
        "sqs",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key,
        endpoint_url="https://sqs.us-east-1.amazonaws.com/027648431608/airflow-demo-queue",
        region_name="us-east-1",
    )
    queue_url = "https://sqs.us-east-1.amazonaws.com/027648431608/airflow-demo-queue"
    time.sleep(3)
    resp = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=(str(kwargs["execution_date"]))
    )


default_args = {
    "owner": "@Yououf Younes OUFRID",
    "depends_on_past": False,
    "start_date": datetime(2022, 4, 20),
    "email": ["oufridyounes@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "demo_aws_sqs_dag",
    default_args=default_args,
    description="AWS SQS Send messages to queue , DEMO by @Yououf !",
    schedule_interval="0 1 * * * ",
    catchup=True,
    max_active_runs=1,
) as dag:

    send_to_sqs = PythonOperator(
        task_id="send_to_sqs",
        provide_context=True,
        python_callable=send_data_to_sqs,
        dag=dag,
    )
