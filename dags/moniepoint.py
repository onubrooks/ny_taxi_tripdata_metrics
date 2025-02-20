from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from datetime import datetime
import requests
import pandas as pd
import os

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
HEADERS = {
    "accept": "*/*",
    "authorization": "Basic ZGVtbzpkZW1v",
    "content-type": "text/plain;charset=UTF-8",
}
CLICKHOUSE_HOST = "https://github.demo.trial.altinity.cloud:8443/?add_http_cors_header=1&default_format=JSONCompact&max_result_rows=1000&max_result_bytes=10000000&result_overflow_mode=break"

METRICS_SQL = """
with base_data as (
    SELECT
        pickup_datetime,
        fare_amount,
        TIMEDIFF(pickup_datetime,dropoff_datetime) AS duration,  
        COUNT(*) OVER (PARTITION BY DATE(pickup_datetime)) as trip_count -- trips per day
    FROM
        tripdata
    WHERE
        pickup_datetime BETWEEN '2014-01-01' AND '2016-12-31'
)

SELECT
    EXTRACT(MONTH FROM pickup_datetime) AS month,
    AVG(CASE WHEN DAYOFWEEK(pickup_datetime) = 6 THEN trip_count ELSE NULL END) AS sat_mean_trip_count,
    AVG(CASE WHEN DAYOFWEEK(pickup_datetime) = 6 THEN fare_amount ELSE NULL END) AS sat_mean_fare_per_trip,
    AVG(CASE WHEN DAYOFWEEK(pickup_datetime) = 6 THEN duration ELSE NULL END) AS sat_mean_duration_per_trip,
    AVG(CASE WHEN DAYOFWEEK(pickup_datetime) = 7 THEN trip_count ELSE NULL END) AS sun_mean_trip_count,
    AVG(CASE WHEN DAYOFWEEK(pickup_datetime) = 7 THEN fare_amount ELSE NULL END) AS sun_mean_fare_per_trip,
    AVG(CASE WHEN DAYOFWEEK(pickup_datetime) = 7 THEN duration ELSE NULL END) AS sun_mean_duration_per_trip
FROM base_data
GROUP BY
    month
ORDER BY
    month;

"""
TABLE_NAME = "tripdata_metrics"

SCHEDULE_INTERVAL = "@once"

CONN_STRING = f"sqlite:///{AIRFLOW_HOME}/dags/moniepoint_nyc_taxi_data.db"

def query_clickhouse(query, headers):
    response = requests.post(CLICKHOUSE_HOST, data=query, headers=headers)
    response.raise_for_status()
    return response.json()

def prepare_dataframe(data):
    return pd.DataFrame(data['data'], columns=[column['name'] for column in data['meta']])

# Function to write metrics to the database
def write_metrics_to_db(metrics, table_name, conn_string):
    engine = create_engine(conn_string)
    metrics.to_sql(table_name, engine, index=False, if_exists="replace")

dag = DAG(
    "moniepoint_write_metrics_to_sqlite",
    start_date=datetime(2024, 1, 1),
    schedule_interval=SCHEDULE_INTERVAL,
    tags=['moniepoint', 'etl_dag_run', 'metrics_etl'],
)

# Extract metrics task
extract_metrics_task = PythonOperator(
    task_id="query_clickhouse",
    python_callable=query_clickhouse,
    op_args=[METRICS_SQL, HEADERS],
    dag=dag,
)

# Transform to DataFrame task
transform_data_task = PythonOperator(
    task_id="prepare_dataframe",
    python_callable=prepare_dataframe,
    op_args=[extract_metrics_task.output],
    dag=dag,
)

# Write metrics to database task
write_metrics_task = PythonOperator(
    task_id="write_metrics_to_db",
    python_callable=write_metrics_to_db,
    op_args=[transform_data_task.output, TABLE_NAME, CONN_STRING],
    dag=dag,
)

# Task dependencies
extract_metrics_task >> transform_data_task >> write_metrics_task
