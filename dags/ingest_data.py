from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.bash import BashOperator


from datetime import datetime

default_args = {
    'start_date':datetime(2022,2,1)
}

with DAG('ingest_data',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:

    is_dataset_available = HttpSensor(
        default_args=default_args,
        task_id='is_dataset_available',
        http_conn_id='ny_taxi_api',
        endpoint='yellow_tripdata_2021-01.csv')

    download_dataset = BashOperator(
        task_id='download_dataset',
        bash_command='wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv -o /tmp/yellow_tripdata.csv'
    )


