import os
import logging
from datetime import datetime,timedelta

import pandas as pd
import json
from nba_api.stats.static import teams

from airflow.decorators import dag,task
from common_tasks import get_teams, convert_to_parquet, upload_to_gcs, remove_local_file, create_external_table_bq, insert_external_table_bq, delete_external_table_bq

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID','not-found')
GCP_GCS_BUCKET = os.getenv('GCP_GCS_BUCKET','not-found')
GCP_GCS_BUCKET_TEAMS_FOLDER = os.getenv('GCP_GCS_BUCKET_TEAMS_FOLDER','not-found')
GCP_BQ_DATASET = os.getenv('GCP_BQ_DATASET_RAW','not-found')
TEMP_STORAGE_PATH = os.getenv('TEMP_STORAGE_PATH', 'not-found')
TODAYS_DATE = datetime.today().strftime('%Y_%m_%d')
FILE_NAME = f'teams_{TODAYS_DATE}'
FILE_FORMAT = '.parquet'
LOCAL_FILE_PATH = f'{TEMP_STORAGE_PATH}/{FILE_NAME}{FILE_FORMAT}'
GCS_OBJECT_PATH = f'{GCP_GCS_BUCKET_TEAMS_FOLDER}{FILE_NAME}'
TABLE_NAME = 'teams'

@dag(
    start_date = datetime.now() - timedelta(days=1),
    schedule_interval = '30 17 * * *',
    catchup = False,
    max_active_runs = 1,
    tags = ["nba_data"],
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1
    }
)

def ingest_teams():
    
    # Functions to tasks are in common_tasks.py file
    teams_json = get_teams()
    teams_pq = convert_to_parquet(teams_json,LOCAL_FILE_PATH)
    teams_upload = upload_to_gcs(GCP_GCS_BUCKET,GCS_OBJECT_PATH,LOCAL_FILE_PATH)
    teams_remove_local = remove_local_file(LOCAL_FILE_PATH)
    teams_create_external_table = create_external_table_bq(GCP_GCS_BUCKET,GCP_PROJECT_ID,GCP_BQ_DATASET,FILE_NAME,GCS_OBJECT_PATH)
    teams_insert_external_table = insert_external_table_bq(GCP_BQ_DATASET,TABLE_NAME,FILE_NAME)
    teams_delete_external_table = delete_external_table_bq(GCP_PROJECT_ID,GCP_BQ_DATASET,FILE_NAME)

    teams_json >> teams_pq >> teams_upload >> teams_remove_local >> teams_create_external_table >> teams_insert_external_table >> teams_delete_external_table
 
ingest_teams()

