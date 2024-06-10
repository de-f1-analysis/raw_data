import os
import requests
import pandas as pd
from datetime import datetime

# 스크립트 파일의 위치를 기준으로 경로 설정
base_path = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(base_path, 'data/pit/basic_pit_stop_data.csv')

# 파일 존재 여부 확인
if not os.path.isfile(csv_file_path):
    raise FileNotFoundError(f"The file {csv_file_path} does not exist.")

existing_df = pd.read_csv(csv_file_path)

# Race session_key 리스트 리턴
def fetch_session_keys():
    url = "https://api.openf1.org/v1/sessions?date_start%3E2023-06-01&session_type=Race"
    response = requests.get(url)

    if response.status_code == 200:
        sessions_data = response.json()
        return [session['session_key'] for session in sessions_data]
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return []

# pit_stop_data 수집 및 CSV 파일로 저장하는 함수
def fetch_and_append_pit_data(existing_df):
    session_keys = fetch_session_keys()
    
    new_pit_data = []
    url2 = "https://api.openf1.org/v1/pit?session_key={}"
    
    for session_key in session_keys:
        response2 = requests.get(url2.format(session_key))
        if response2.status_code == 200:
            pit_data = response2.json()
            for pit in pit_data:
                # 'date' 필드를 제외하고 필요한 데이터만 추가
                new_pit_data.append({
                    'driver_number': pit['driver_number'],
                    'lap_number': pit['lap_number'],
                    'pit_duration': pit['pit_duration'],
                    'meeting_key': pit['meeting_key'],
                    'session_key': pit['session_key']
                })
        else:
            print(f"Failed to fetch data for session_key {session_key}. Status code: {response2.status_code}")
    
    # 새로운 pit_stop_data를 DataFrame으로 변환
    new_df = pd.DataFrame(new_pit_data)
    
    # 기존 데이터와 새로운 데이터를 합침
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    
    # 새로운 CSV 파일 이름 설정
    date_str = datetime.now().strftime("%Y%m%d")
    new_file_path = os.path.join(base_path, f'data/pit/pit_stop_data_{date_str}.csv')
    
    # 새로운 CSV 파일로 저장
    combined_df.to_csv(new_file_path, index=False)
    print(f"Data saved to {new_file_path}")
    
    return new_file_path

# 기존 CSV 파일과 새로운 데이터를 합친 후 저장
new_file_path = fetch_and_append_pit_data(existing_df)