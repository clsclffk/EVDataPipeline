from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, hour, monotonically_increasing_id, count, sum as _sum,
    split, regexp_replace, regexp_extract, to_timestamp, udf
)
from pyspark.sql.types import FloatType, IntegerType, StringType, ArrayType
import re 

spark = SparkSession.builder \
    .appName("ETL_HDFS_to_Postgres_Clean") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.sql.catalogImplementation", "in-memory") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

HDFS_BASE_PATH = "hdfs://namenode:8020/data/raw"
POSTGRES_URL = "jdbc:postgresql://ev_postgres:5432/ev_dw"
POSTGRES_USER = "admin"
POSTGRES_PASSWORD = "admin"

# -------------------------------
# 1. 충전소 테이블 파싱
# -------------------------------
raw_df = spark.read.text(f"{HDFS_BASE_PATH}/ev_charging_stations")

# 괄호 안 쉼표 깨짐 방지
# cleaned_df = raw_df.withColumn("value", regexp_replace(col("value"), r"\(([^)]*),([^)]*)\)", r"(\1·\2)"))
# split_df = cleaned_df.withColumn("parts", split(col("value"), ","))
def replace_commas_inside_parentheses(text):
    if text is None:
        return None
    # 괄호 안에 있는 모든 쉼표를 안전하게 치환
    return re.sub(r'\([^)]*\)', lambda m: m.group(0).replace(',', '·'), text)

replace_commas_udf = udf(replace_commas_inside_parentheses, StringType())

# 괄호 안 쉼표 처리 적용
cleaned_df = raw_df.withColumn("value", replace_commas_udf(col("value")))

# 그다음 안전하게 split
split_df = cleaned_df.withColumn("parts", split(col("value"), ","))

stations_df = split_df.select(
    col("parts").getItem(0).alias("station_name"),
    regexp_replace(col("parts").getItem(1), "·", ",").alias("address"),
    col("parts").getItem(2).alias("latitude"),
    col("parts").getItem(3).alias("longitude"),
    col("parts").getItem(4).alias("capacity_kw"),
    col("parts").getItem(5).alias("total_chargers"),
    col("parts").getItem(6).alias("slow_chargers"),
    col("parts").getItem(7).alias("fast_chargers"),
    col("parts").getItem(8).alias("outlet_chargers")
)

# 숫자형 컬럼 캐스팅
stations_df = stations_df \
    .withColumn("latitude", col("latitude").cast(FloatType())) \
    .withColumn("longitude", col("longitude").cast(FloatType())) \
    .withColumn("capacity_kw", col("capacity_kw").cast(FloatType())) \
    .withColumn("total_chargers", col("total_chargers").cast(IntegerType())) \
    .withColumn("slow_chargers", col("slow_chargers").cast(IntegerType())) \
    .withColumn("fast_chargers", col("fast_chargers").cast(IntegerType())) \
    .withColumn("outlet_chargers", col("outlet_chargers").cast(IntegerType()))

# 구 컬럼 추가
stations_df = stations_df.withColumn("gu", regexp_extract(col("address"), "([가-힣]+구)", 1))
dim_stations = stations_df.dropDuplicates(["station_name"]).withColumn("station_id", monotonically_increasing_id())

# -------------------------------
# 2. 세션/시간 데이터 로드
# -------------------------------
sessions_df = spark.read.csv(f"{HDFS_BASE_PATH}/ev_charging_sessions", header=False)
times_df = spark.read.csv(f"{HDFS_BASE_PATH}/ev_charging_times", header=False)

sessions_df = sessions_df.toDF("charge_date", "station_name", "charger_type", "energy_kwh")
times_df = times_df.toDF("station_name", "start_time", "end_time", "energy_kwh")

# 문자열을 timestamp로 변환
times_df = times_df \
    .withColumn("start_time", to_timestamp(col("start_time"), "yyyy-MM-dd HH:mm:ss.S")) \
    .withColumn("end_time", to_timestamp(col("end_time"), "yyyy-MM-dd HH:mm:ss.S"))

# -------------------------------
# 3. 차원 테이블 생성
# -------------------------------
dim_charger_type = sessions_df.select("charger_type").dropDuplicates().withColumn("charger_type_id", monotonically_increasing_id())
dim_date = sessions_df.select(to_date(col("charge_date")).alias("date")).dropDuplicates().withColumn("date_id", monotonically_increasing_id())
dim_time = times_df.filter(col("start_time").isNotNull()) \
                   .select(hour(col("start_time")).alias("hour")) \
                   .dropDuplicates() \
                   .withColumn("hour_id", monotonically_increasing_id())

# -------------------------------
# 4. 사실 테이블 생성
# -------------------------------
fact_sessions = sessions_df \
    .join(dim_stations, "station_name") \
    .join(dim_charger_type, "charger_type") \
    .join(dim_date, to_date(col("charge_date")) == dim_date["date"]) \
    .groupBy("station_id", "charger_type_id", "date_id") \
    .agg(
        _sum(col("energy_kwh").cast(FloatType())).alias("energy_kwh"),
        count("*").alias("session_count")
    ) \
    .withColumn("session_id", monotonically_increasing_id())

fact_sessions = fact_sessions.select(
    col("station_id").cast("bigint"),
    col("charger_type_id").cast("bigint"),
    col("date_id").cast("bigint"),
    col("energy_kwh").cast("float"),  
    col("session_count").cast("bigint"),
    col("session_id").cast("bigint")
)

fact_times = times_df \
    .join(dim_stations, "station_name") \
    .join(dim_date, to_date(col("start_time")) == dim_date["date"]) \
    .join(dim_time, hour(col("start_time")) == dim_time["hour"]) \
    .select(
        "station_id", "hour_id", "date_id",
        col("energy_kwh").cast(FloatType()).alias("energy_kwh")
    ) \
    .withColumn("time_log_id", monotonically_increasing_id())

# -------------------------------
# 5. Postgres 적재
# -------------------------------
def write_to_postgres(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

for name, df in {
    "dim_stations": dim_stations,
    "dim_charger_type": dim_charger_type,
    "dim_date": dim_date,
    "dim_time": dim_time,
    "fact_sessions": fact_sessions,
    "fact_times": fact_times
}.items():
    print(f"Writing {name} to Postgres...")
    write_to_postgres(df, name)

spark.stop()
