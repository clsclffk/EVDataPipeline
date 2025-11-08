#!/bin/bash
# MySQL -> HDFS

# 실행법
# docker exec -it ev_sqoop bash
# bash /usr/local/sqoop/scripts/import_all.sh
# hdfs dfs -ls /data/raw

DB_NAME="ev_data_pipeline"
DB_USER="admin"
DB_PASS="admin"
DB_HOST="ev_mysql"
JDBC_URL="jdbc:mysql://${DB_HOST}:3306/${DB_NAME}"

HDFS_BASE_DIR="/data/raw"
LOG_DIR="/opt/sqoop/logs"
LIB_PATH="/sqoop-1.4.6-cdh5.13.0/lib/mysql-connector-java-5.1.49.jar"

TABLES=(
  "ev_charging_stations"
  "ev_charging_sessions"
  "ev_charging_times"
)

LOG_DIR="/opt/sqoop/logs"
mkdir -p "$LOG_DIR"
hdfs dfs -mkdir -p "$HDFS_BASE_DIR"

export JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF-8"
export HADOOP_OPTS="-Dfile.encoding=UTF-8"

for TABLE in "${TABLES[@]}"; do
  echo "Importing table: ${TABLE} ..."
  sqoop import \
    --libjars "$LIB_PATH" \
    --connect "${JDBC_URL}?useSSL=false&serverTimezone=Asia/Seoul&useUnicode=true&characterEncoding=UTF-8" \
    --username "$DB_USER" \
    --password "$DB_PASS" \
    --driver com.mysql.jdbc.Driver \
    --table "$TABLE" \
    --target-dir "${HDFS_BASE_DIR}/${TABLE}" \
    --as-textfile \
    --fields-terminated-by ',' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    -m 1 \
    > "${LOG_DIR}/${TABLE}.log" 2>&1

  if [ $? -eq 0 ]; then
    echo "✅ Successfully imported ${TABLE}"
  else
    echo "❌ Failed to import ${TABLE}, check log: ${LOG_DIR}/${TABLE}.log"
  fi
  echo "--------------------------------------------"
done

hdfs dfs -ls "$HDFS_BASE_DIR"
