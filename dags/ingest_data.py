from airflow.models import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from sqlalchemy import create_engine
from datetime import datetime
from time import perf_counter
import pandas as pd

default_args = {
    'start_date': datetime(2021, 0, 1)
}


def transform_datetime(df):
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


def _ingest_data(table_name, csv_name, user, password, host, port, db):
    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    # create schema and export into database
    transform_datetime(df)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
    df.to_sql(name=table_name, con=engine, if_exists="append")

    while True:
        try:
            t_start = perf_counter()

            df = next(df_iter)

            # same transformation
            transform_datetime(df)

            df.to_sql(name=table_name, con=engine, if_exists="append")

            t_end = perf_counter()

            print("inserted another chunk, took %.3f second" %
                  (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


with DAG('ingest_data',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:

    # look at the connections in airflow
    connection = BaseHook.get_connection("ny_taxi_api")
    # print(connection.host)

    # is_dataset_available = HttpSensor(
    #     default_args=default_args,
    #     task_id='is_dataset_available',
    #     http_conn_id='ny_taxi_api',
    #     endpoint='yellow_tripdata_2021-01.csv')

    # download_dataset = BashOperator(
    #     task_id='download_dataset',
    #     bash_command='wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv -O /tmp/yellow_tripdata.csv'
    # )

    # ingest_data = PythonOperator(
    #     task_id='ingest_data',
    #     python_callable=_ingest_data,
    #     op_kwargs={
    #         'table_name': 'ny_taxi',
    #         'csv_name': '/tmp/yellow_tripdata.csv',
    #         'user': 'root',
    #         'password': 'root',
    #         'host': 'taxibase',
    #         'port': 5432,
    #         'db': 'ny_taxi',
    #     }
    # )
