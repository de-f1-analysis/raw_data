import requests
import pandas as pd
import json
import datetime


# 경기 session_key 값
def session_input(session_result):
    session_df = pd.DataFrame(session_result)
    session_key = session_df["session_key"]
    return session_key


# 경기 마지막 position 데이터만 가져오기(선수별)
def get_latest_records(position_df):
    latest_records = (
        position_df.sort_values(by="date", ascending=False)
        .groupby("driver_number")
        .first()
    )
    return latest_records


def position(session_key):
    dfs = []  # 경기 결과(최종 데이터만 저장)
    for key in session_key:
        response = requests.get(
            "https://api.openf1.org/v1/position?session_key={}".format(key)
        ).json()
        position_df = pd.DataFrame(response)
        # 선수별로 가장 최근 기록
        lastest_records = get_latest_records(position_df)
        dfs.append(lastest_records)

    # 경기 결과(ex. 2023 퀄리파잉)
    combined_df = pd.concat(dfs)

    return combined_df


params = [
    {"session_name": "Qualifying", "year": "2023"},
    {"session_name": "Race", "year": "2023"},
    {"session_name": "Qualifying", "year": "2024"},
    {"session_name": "Race", "year": "2024"},
]

# 경기별 데이터 가져오기
result = []  # 전체 결과
for param in params:
    url = "https://api.openf1.org/v1/sessions?session_name={}&year={}".format(
        param["session_name"], param["year"]
    )
    session_result = requests.get(url).json()
    key = session_input(session_result)
    merge = position(key)
    result.append(merge)

final_combined_df = pd.concat(result)
final_combined_df.to_csv("postion.csv")
