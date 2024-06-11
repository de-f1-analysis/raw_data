from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
import requests
import pandas as pd
import os

# 스크립트 파일의 위치를 기준으로 경로 설정
csv_file_path = "/opt/airflow/data/pit/basic_pit_stop_data.csv"


def fetch_and_save_pit_data(**kwargs):
    # 파일 존재 여부 확인
    if not os.path.isfile(csv_file_path):
        raise FileNotFoundError(f"The file {csv_file_path} does not exist.")

    existing_df = pd.read_csv(csv_file_path)

    def fetch_session_keys():
        url = "https://api.openf1.org/v1/sessions?date_start%3E2023-06-01&session_type=Race"
        response = requests.get(url)
        if response.status_code == 200:
            sessions_data = response.json()
            return [session["session_key"] for session in sessions_data]
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            return []

    session_keys = fetch_session_keys()
    new_pit_data = []
    url2 = "https://api.openf1.org/v1/pit?session_key={}"

    for session_key in session_keys:
        response2 = requests.get(url2.format(session_key))
        if response2.status_code == 200:
            pit_data = response2.json()
            for pit in pit_data:
                new_pit_data.append(
                    {
                        "driver_number": pit["driver_number"],
                        "lap_number": pit["lap_number"],
                        "pit_duration": pit["pit_duration"],
                        "meeting_key": pit["meeting_key"],
                        "session_key": pit["session_key"],
                    }
                )
        else:
            print(
                f"Failed to fetch data for session_key {session_key}. Status code: {response2.status_code}"
            )

    new_df = pd.DataFrame(new_pit_data)
    combined_df = pd.concat([existing_df, new_df], ignore_index=True).drop_duplicates()

    # 새로운 CSV 파일 이름 설정
    date_str = datetime.now().strftime("%Y%m%d")
    new_file_path = f"/opt/airflow/data/pit/pit_stop_data_{date_str}.csv"

    # 새로운 CSV 파일로 저장
    combined_df.to_csv(new_file_path, index=False)
    return new_file_path


with DAG(
    dag_id="f1_pit_stop_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    fetch_and_save_pit_data_task = PythonOperator(
        task_id="fetch_and_save_pit_data",
        python_callable=fetch_and_save_pit_data,
        provide_context=True,
    )

    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="{{ task_instance.xcom_pull(task_ids='fetch_and_save_pit_data') }}",  # 이전 작업에서 생성된 파일 경로를 가져옴
        dst="data/pit/{{ execution_date.strftime('%Y%m%d') }}/pit_stop_data.csv",  # GCS에 저장될 파일 경로 및 이름 (execution_date에 따라 폴더 생성)
        bucket="{{ var.value.gcs_bucket_name }}",  # GCS bucket 이름, (Web UI에서 Variable 등록)
        gcp_conn_id="gcs_connection",  # GCS connection 정보 (Web UI에서 Connection 등록)
    )

    fetch_and_save_pit_data_task >> upload_to_gcs_task
