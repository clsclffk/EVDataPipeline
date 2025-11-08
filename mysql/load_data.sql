USE ev_data_pipeline;

LOAD DATA LOCAL INFILE '/data/ev_charging_stations.csv'
INTO TABLE ev_charging_stations
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 ROWS
(station_name, address, latitude, longitude, capacity_kW, total_count, slow_count, fast_count, outlet_count);

LOAD DATA LOCAL INFILE '/data/ev_charging_sessions.csv'
INTO TABLE ev_charging_sessions
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 ROWS
(charge_date, station_name, charger_type, energy_kwh);

LOAD DATA LOCAL INFILE '/data/ev_charging_times.csv'
INTO TABLE ev_charging_times
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 ROWS
(station_name, start_time, end_time, energy_kW);