import os
import argparse
from re import A

from time import time

import pandas as pd
from sqlalchemy import create_engine

# docker run --network ny_taxi_default taxi_ingest:0.1 \
#     --user root \
#     --password root \
#     --host localhost \
#     --port 5432 \
#     --db ny_tax \
#     --table_name yellow_taxi_trips \
#     --url https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv \


def main(params):
    #params
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = "output.csv"


    #get taxi download
    os.system(f"wget {url} -O {csv_name}")



    #connct to postgres db
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    #import schema into postgres db
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
    df.to_sql(name=table_name, con=engine, if_exists="append")


    #ingest data into db
    while True:
        try:
            t_start = time()

            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists="append")

            t_end = time()

            print("inserted another chunk, took %.3f second" % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


#args
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument("--user", required=True, help="user name for postgres")
    parser.add_argument("--password", required=True, help="password for postgres")
    parser.add_argument("--host", required=True, help="host for postgres")
    parser.add_argument("--port", required=True, help="port for postgres")
    parser.add_argument("--db", required=True, help="database name for postgres")
    parser.add_argument(
        "--table_name",
        required=True,
        help="name of the table where we will write the results to",
    )
    parser.add_argument("--url", required=True, help="url of the csv file")

    args = parser.parse_args()

    main(args)
