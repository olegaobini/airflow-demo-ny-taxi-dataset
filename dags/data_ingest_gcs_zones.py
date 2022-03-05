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


'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
URL_TEMPLATE = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'

OUTPUT_FILE_TEMPLATE = f'{AIRFLOW_HOME}/zones.csv'

TABLE_NAME_TEMPLATE = 'zones'

PARQUET_FILE = OUTPUT_FILE_TEMPLATE.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingest_gcs_zones",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['location zones'],
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
            "object_name": f"raw/zones.parquet",
            "local_file": f"{PARQUET_FILE}",
        },
    )

    remove_local_data = BashOperator(
        task_id="remove_local_data",
        bash_command=f"rm -rf {AIRFLOW_HOME}/zones.*"
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
                "sourceUris": [f"gs://{BUCKET}/raw/zones.parquet"],
            },
        },
    )

    download_dataset >> format_to_parquet >> local_to_gcs >> remove_local_data >> bigquery_external_table
