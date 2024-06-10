from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
import requests
import csv
import os

# 기본 DAG 인자 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'api_to_bq_upsert',
    default_args=default_args,
    description='Fetch API data, save as CSV, upload to GCS, and upsert into BigQuery',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# API 데이터에서 CSV 파일 생성하는 함수
def fetch_and_save_csv(**kwargs):
    url = "https://api.openf1.org/v1/sessions?date_start%3E2023-06-01&session_type=Race"
    response = requests.get(url)
    sessions = response.json()
    
    # 파일 경로와 이름 설정
    file_path = '/tmp/sessions.csv'
    
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["location", "country_key", "country_code", "country_name", "circuit_key",
                         "circuit_short_name", "session_type", "session_name", "date_start",
                         "date_end", "gmt_offset", "session_key", "meeting_key", "year"])
        
        for session in sessions:
            writer.writerow([
                session["location"], session["country_key"], session["country_code"], session["country_name"],
                session["circuit_key"], session["circuit_short_name"], session["session_type"], session["session_name"],
                session["date_start"], session["date_end"], session["gmt_offset"], session["session_key"],
                session["meeting_key"], session["year"]
            ])

# PythonOperator를 사용하여 CSV 파일 생성
fetch_and_save_task = PythonOperator(
    task_id='fetch_and_save_csv',
    python_callable=fetch_and_save_csv,
    provide_context=True,
    dag=dag,
)

# CSV 파일을 GCS로 업로드
upload_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs',
    src='/tmp/sessions.csv',
    dst=f'gs://your-bucket-name/sessions/{{ ds }}/sessions.csv',
    bucket_name='your-bucket-name',
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

# GCS의 데이터를 BigQuery 테이블로 업서트
load_to_bq_task = GCSToBigQueryOperator(
    task_id='load_to_bq',
    bucket='your-bucket-name',
    source_objects=[f'sessions/{{ ds }}/sessions.csv'],
    destination_project_dataset_table='your-project.your_dataset.your_table',
    schema_fields=[
        {"name": "location", "type": "STRING", "mode": "NULLABLE"},
        {"name": "country_key", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "country_code", "type": "STRING", "mode": "NULLABLE"},
        {"name": "country_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "circuit_key", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "circuit_short_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "session_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "session_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "date_start", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "date_end", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "gmt_offset", "type": "STRING", "mode": "NULLABLE"},
        {"name": "session_key", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "meeting_key", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "year", "type": "INTEGER", "mode": "NULLABLE"}
    ],
    write_disposition='WRITE_APPEND',
    source_format='CSV',
    skip_leading_rows=1,
    field_delimiter=',',
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

# task 순서 정의
fetch_and_save_task >> upload_to_gcs_task >> load_to_bq_task