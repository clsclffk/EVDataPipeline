import pandas as pd
from pathlib import Path

data_dir = Path("data")

files = {
    "ev_charging_stations.xlsx": "ev_charging_stations.csv",
    "ev_charging_sessions.xlsx": "ev_charging_sessions.csv",
    "ev_charging_times.xlsx": "ev_charging_times.csv"
}

for src, dst in files.items():
    src_path = data_dir / src
    dst_path = data_dir / dst
    print(f"변환 중: {src_path.name} → {dst_path.name}")

    # 실제 데이터가 시작하는 행 번호 지정 
    df = pd.read_excel(src_path, header=3)

    # 완전히 빈 열 제거
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    # CSV로 저장
    df.to_csv(dst_path, index=False, encoding="utf-8-sig")

    print(f"완료: {dst_path.name}")

print("모든 XLSX → CSV 변환 완료")
