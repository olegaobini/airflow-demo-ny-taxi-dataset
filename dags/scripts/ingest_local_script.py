import pandas as pd
from time import perf_counter
from sqlalchemy import create_engine


def _ingest_data(table_name, csv_name, user, password, host, port, db):

    def transform_datetime(df):
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=99999)
    df = next(df_iter)

    # DATE column transformation
    transform_datetime(df)

    # create schema and export into database
    df.head(n=-1).to_sql(name=table_name, con=engine, if_exists="replace")
    df.to_sql(name=table_name, con=engine, if_exists="append")

    while True:
        try:
            t_start = perf_counter()

            df = next(df_iter)

            # column transformation
            transform_datetime(df)

            df.to_sql(name=table_name, con=engine, if_exists="append")

            t_end = perf_counter()

            print("inserted another chunk, took %.2f second" %
                  (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break
