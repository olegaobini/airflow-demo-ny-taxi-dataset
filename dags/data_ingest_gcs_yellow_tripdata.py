import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from scripts.ingest_gcs_script import _format_to_parquet, upload_to_gcs

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_file = "yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}"
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'

URL_TEMPLATE = f'{URL_PREFIX}/{dataset_file}.csv'

OUTPUT_FILE_TEMPLATE = f'{AIRFLOW_HOME}/{dataset_file}.csv'


PARQUET_FILE = OUTPUT_FILE_TEMPLATE.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


default_args = {
    "owner": "airflow",
    "start_date": '2019-01-01',
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingest_gcs_yellow_tripdata",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['yellow taxi tripdata'],
) as dag:

    download_dataset = BashOperator(
        task_id="download_dataset",
        bash_command=f"curl -sSLf {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}"
    )

    format_to_parquet = PythonOperator(
        task_id="format_to_parquet",
        python_callable=_format_to_parquet,
        op_kwargs={
            "src_file": f"{OUTPUT_FILE_TEMPLATE}",
        },
    )

    local_to_gcs = PythonOperator(
        task_id="local_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{dataset_file}.parquet",
            "local_file": f"{PARQUET_FILE}",
        },
    )

    remove_local_data = BashOperator(
        task_id="remove_local_data",
        bash_command=f"rm -rf {AIRFLOW_HOME}/{dataset_file}.*"
    )

    bigquery_external_table = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{dataset_file}.parquet"],
            },
        },
    )

    download_dataset >> format_to_parquet >> local_to_gcs >> remove_local_data >> bigquery_external_table
