import os
import logging
from datetime import datetime,timedelta
import time
from random import randint

import pandas as pd
from nba_api.stats.endpoints import commonteamroster

from airflow.decorators import dag,task
from common_tasks import get_teams, get_current_season, convert_to_parquet, upload_to_gcs, remove_local_file, create_external_table_bq, insert_external_table_bq, delete_external_table_bq


GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID','not-found')
GCP_GCS_BUCKET = os.getenv('GCP_GCS_BUCKET', 'not-found')
GCP_GCS_BUCKET_PLAYER_INFO_FOLDER = os.getenv('GCP_GCS_BUCKET_PLAYER_INFO_FOLDER','not-found')
GCP_BQ_DATASET = os.getenv('GCP_BQ_DATASET_RAW','not-found')
TEMP_STORAGE_PATH = os.getenv('TEMP_STORAGE_PATH', 'not-found')
TODAYS_DATE = datetime.today().strftime('%Y_%m_%d')
FILE_NAME = f'player_info_{TODAYS_DATE}'
FILE_FORMAT = '.parquet'
LOCAL_FILE_PATH = f'{TEMP_STORAGE_PATH}/{FILE_NAME}{FILE_FORMAT}'
GCS_OBJECT_PATH = f'{GCP_GCS_BUCKET_PLAYER_INFO_FOLDER}{FILE_NAME}'
TABLE_NAME = 'player_info'

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

def ingest_player_info():
    
    @task()
    def get_player_info(season,teams):
        """
        Retrieve player dimensions (position,height,weight) from a given team and season

        Params:
            seasons(list): The list of seasons needed to pass as a parameter
            teams(json): Read in team json and convert team ID column to list
        
        Returns:
            player_info_df(DataFrame): DataFrame of player characteristics
        """
        teams_df = pd.read_json(teams)

        teams_id = teams_df['id'].to_list()

        team_roster_data = []

        for id in teams_id:
            try:
                logging.info(f"Starting request for team_id:{id}")
                common_team_roster_response = commonteamroster.CommonTeamRoster(season=season,team_id=id)
            except Exception as e:
                logging.info(f"Error with CommonTeamRoster endpoint.. Error: {e} with team_id:{id}")
            df = common_team_roster_response.common_team_roster.get_data_frame()
            team_roster_data.append(df)
            logging.info(f"Finishing request for team_id:{id}")
            time.sleep(randint(1,5))

        team_roster_df = pd.concat(team_roster_data)

        team_roster_df['SEASON_ID'] = season

        team_roster_json = team_roster_df.to_json(orient='records')

        return team_roster_json
    
    # Functions (tasks) referenced below that are not defined above are in the common_tasks.py file
    teams_json = get_teams()
    current_season = get_current_season(teams_json)
    player_info_json = get_player_info(current_season,teams_json)
    player_info_pq = convert_to_parquet(player_info_json,LOCAL_FILE_PATH)
    player_info_upload = upload_to_gcs(GCP_GCS_BUCKET,GCS_OBJECT_PATH,LOCAL_FILE_PATH)
    player_info_remove_local = remove_local_file(LOCAL_FILE_PATH)
    player_info_create_external_table = create_external_table_bq(GCP_GCS_BUCKET,GCP_PROJECT_ID,GCP_BQ_DATASET,FILE_NAME,GCS_OBJECT_PATH)
    player_info_insert_external_table = insert_external_table_bq(GCP_BQ_DATASET,TABLE_NAME,FILE_NAME)
    player_info_delete_external_table = delete_external_table_bq(GCP_PROJECT_ID,GCP_BQ_DATASET,FILE_NAME)

    teams_json >> current_season >> player_info_json >> player_info_pq >> player_info_upload >> player_info_remove_local >> \
    player_info_create_external_table >> player_info_insert_external_table >> player_info_delete_external_table

ingest_player_info()