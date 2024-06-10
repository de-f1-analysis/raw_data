import requests
import pandas as pd
from lxml import etree

url_template = "http://ergast.com/api/f1/{year}/{round}/pitstops"

all_pit_stops = []
years = [2023, 2024]
rounds = range(1, 25)

# API 요청
for year in years:
    for round in rounds:
        
        url = url_template.format(year=year, round=round)
        response = requests.get(url)
        
        if response.status_code == 200 and response.content:
            try:                
                xml_data = response.content
                tree = etree.fromstring(xml_data)
                
                # 네임스페이스 정의
                namespaces = {'mrd': 'http://ergast.com/mrd/1.5'}
                
                # XPath를 사용하여 PitStop 데이터 추출
                pit_stops = tree.xpath('//mrd:PitStop', namespaces=namespaces)
                
                for pit_stop in pit_stops:
                    pit_stop_data = {
                        'driverId': pit_stop.get('driverId'),
                        'stop': pit_stop.get('stop'),
                        'lap': pit_stop.get('lap'),
                        'time': pit_stop.get('time'),
                        'duration': pit_stop.get('duration'),
                        'year': year,
                        'round': round
                    }
                    all_pit_stops.append(pit_stop_data)

            except Exception as e:
                print(f"Failed to process data for {year} round {round}: {e}")
        else:
            print(f"No data for {year} round {round}")

# 데이터프레임으로 변환
pit_stops_df = pd.DataFrame(all_pit_stops)

# CSV 파일로 저장
csv_filename = "f1_pit_stops.csv"
pit_stops_df.to_csv(csv_filename, index=False)

print(f"Data saved to {csv_filename}")