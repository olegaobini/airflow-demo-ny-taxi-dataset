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
    'start_date': datetime(2021, 1, 1),
}


def transform_datetime(df):
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


def _ingest_data(table_name, csv_name, user, password, host, port, db):
    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    # DATE column transformation
    transform_datetime(df)

    # create schema and export into database
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
    df.to_sql(name=table_name, con=engine, if_exists="append")

    while True:
        try:
            t_start = perf_counter()

            df = next(df_iter)

            # column transformation
            transform_datetime(df)

            df.to_sql(name=table_name, con=engine, if_exists="append")

            t_end = perf_counter()

            print("inserted another chunk, took %.3f second" %
                  (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


def _test():
    return '\nhi\n'


with DAG('ingest_data',
         default_args=default_args,
         schedule_interval='0 0 2 * *',
         catchup=True,
         user_defined_macros={
             'test': _test,
         }
         ) as dag:

    _DATE = '{{ds}}'
    # _DATE = '{{ds.strptime(ds,"")[:ds.rfind("-")]}}'
    # _DATE = '{{ds[:ds.rfind("-")]}}'
    # ENDPOINT = f'yellow_tripdata_{_DATE}.csv'
    # NY_TAXI_API = BaseHook.get_connection("ny_taxi_api").host

    # look at the connections in airflow
    # print(connection.host)

    test = BashOperator(
        task_id='test',
        bash_command='echo {{test}}'
        # bash_command=f'echo {NY_TAXI_API}{ENDPOINT}'
    )

    # is_dataset_available = HttpSensor(
    #     default_args=default_args,
    #     task_id='is_dataset_available',
    #     http_conn_id='ny_taxi_api',
    #     endpoint=ENDPOINT

    # download_dataset = BashOperator(
    #     task_id='download_dataset',
    #     bash_command=f'wget {NY_TAXI_API}{ENDPOINT} -O /tmp/yellow_tripdata.csv'
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
