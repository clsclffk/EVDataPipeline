USE ev_data_pipeline;

-- 1. 서울시 소유 전기차 충전소 정보
CREATE TABLE IF NOT EXISTS ev_charging_stations (
    station_name VARCHAR(100),
    address VARCHAR(255),
    latitude DOUBLE,
    longitude DOUBLE,
    capacity_kW FLOAT,
    total_chargers INT,
    slow_chargers INT,
    fast_chargers INT,
    outlet_chargers INT
);

-- 2. 서울시 소유 전기차 충전소의 충전량
CREATE TABLE IF NOT EXISTS ev_charging_sessions (
    charge_date DATE,
    station_name VARCHAR(100),
    charger_type VARCHAR(20),
    energy_kwh FLOAT
);

-- 3. 서울시 소유 전기차 충전기 일별 시간별 충전현황
CREATE TABLE IF NOT EXISTS ev_charging_times (
    station_name VARCHAR(100),
    start_time DATETIME,
    end_time DATETIME,
    energy_kwh FLOAT
);
