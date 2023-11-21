from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.transfers.s3_to_s3 import S3ToS3Operator
from airflow.providers.amazon.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.operators.glue import AWSGlueJobOperator
from airflow.utils.dates import days_ago
import boto3

# Default_args dictionary to pass to the DAG constructor
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Defining the DAG
dag = DAG(
    'financial_data_pipeline',
    default_args=default_args,
    description='DAG for processing financial data',
    schedule_interval=timedelta(days=1),  # Run daily
)

# Defining functions to be used as tasks in the DAG
def trigger_glue_crawler():
    glue_client = boto3.client('glue', region_name='my-region')
    response = glue_client.start_crawler(Name='my-crawler-name')
    return response

def trigger_glue_job():
    job_params = {
        'MyJobParameterKey': 'MyJobParameterValue',
    }
    return AWSGlueJobOperator(
        task_id='run_glue_job',
        job_name='my-glue-job-name',
        job_script_location='s3://my-bucket/path/to/your/script.py',
        script_args=job_params,
        dag=dag,
    ).execute(context=None)

# Defining the tasks
s3_key_sensor_task = S3KeySensor(
    task_id='check_for_csv_upload',
    bucket_name='my-s3-bucket',
    bucket_key='path/to/my/csv/file.csv',
    poke_interval=600,  # Check every 10 minutes
    timeout=600*5,  # Timeout after 5 retries
    mode='poke',
    dag=dag,
)

s3_to_s3_transfer_task = S3ToS3Operator(
    task_id='move_csv_to_processing_folder',
    source_bucket_key='path/to/my/csv/file.csv',
    dest_bucket_key='path/to/processing/folder/file.csv',
    source_bucket_name='my-s3-bucket',
    dest_bucket_name='my-s3-bucket',
    replace=True,
    dag=dag,
)

glue_crawler_task = PythonOperator(
    task_id='run_glue_crawler',
    python_callable=trigger_glue_crawler,
    dag=dag,
)

glue_job_task = PythonOperator(
    task_id='run_glue_job',
    python_callable=trigger_glue_job,
    dag=dag,
)

# Set up task dependencies
s3_key_sensor_task >> s3_to_s3_transfer_task >> glue_crawler_task >> glue_job_task
