from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
from airflow.providers.google.cloud.operators.gcs import GCSHook
import os

def fetch_and_upload_pit_data(bucket_name, **kwargs):
    # 기존 데이터를 로드
    base_path = os.path.dirname(os.path.abspath(__file__))
    csv_file_path = os.path.join(base_path, "data/pit/basic_pit_stop_data.csv")
    
    if not os.path.isfile(csv_file_path):
        raise FileNotFoundError(f"The file {csv_file_path} does not exist.")
    
    existing_df = pd.read_csv(csv_file_path)

    # 세션 키를 가져오는 함수
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
            print(f"Failed to fetch data for session_key {session_key}. Status code: {response2.status_code}")

    new_df = pd.DataFrame(new_pit_data)
    combined_df = pd.concat([existing_df, new_df], ignore_index=True).drop_duplicates()

    new_pit_csv = combined_df.to_csv(index=False)
    date_str = datetime.now().strftime("%Y%m%d")
    file_path = f"data/pit/pit_stop_data_{date_str}.csv"

    gcs_hook = GCSHook(gcp_conn_id="gcs_connection")
    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=file_path,
        data=new_pit_csv,
        mime_type="text/csv",
    )

    print("GCS upload complete!")

with DAG(
    dag_id="f1_pit_stop_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    pit_stop_data_task = PythonOperator(
        task_id="fetch_and_upload_pit_data",
        python_callable=fetch_and_upload_pit_data,
        op_kwargs={"bucket_name": "{{ var.value.gcs_bucket_name }}"},
    )