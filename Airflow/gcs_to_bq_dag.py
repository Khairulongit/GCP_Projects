from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator



with DAG(
    dag_id="gcs_to_bq_dag",
    description='gcs to Big Query Loading dag',
    schedule='*/10 * * * *',
    max_active_runs=2,
    start_date=datetime(2024, 1, 1),
     # manual trigger
    catchup=False,
) as dag:

    start_task = EmptyOperator(
        task_id="start"
    )

    wait_for_file = GCSObjectsWithPrefixExistenceSensor(
    task_id="wait_for_gcs_prefix",
    bucket="airflow_data_gcs",
    prefix="incoming/data_",  # matches data_20260207.csv etc
    poke_interval=60,
    timeout=600,
    mode="reschedule",
    )

    fetch_data = GCSToBigQueryOperator(
        task_id="fetch_data",
        bucket="airflow_data_gcs",
        source_objects=["incoming/data_*.csv"],  # ✅ wildcard here
        destination_project_dataset_table="buoyant-striker-376011.staging_dataset.cust_table",
        write_disposition="WRITE_TRUNCATE",
        source_format="CSV",
        skip_leading_rows=1,          # if your CSV has headers
        autodetect=True,              # let BQ infer schema (or define schema_fields instead)
    )


    archive_files = GCSToGCSOperator(
        task_id="archive_files",
        source_bucket="airflow_data_gcs",
        source_object="incoming/data_*.csv",   # same pattern you loaded
        destination_bucket="airflow_data_gcs",
        destination_object="archive/csv/",         # folder where files go
        move_object=True,                      # 🔥 this deletes the source after copy
        gcp_conn_id="google_cloud_default",
        trigger_rule="all_success", 
    )



#     run_stored_procedure = BigQueryInsertJobOperator(
#     task_id="run_stored_procedure",
#     configuration={
#         "query": {
#             "query": "CALL `buoyant-striker-376011.staging_dataset.my_procedure`()",
#             "useLegacySql": False,
#         }
#     },
#     location="US",  # must match your dataset location
#     gcp_conn_id="google_cloud_default",
# )


    end_task = EmptyOperator(
        task_id="end"
    )

    start_task >> wait_for_file >> fetch_data >> archive_files >> end_task