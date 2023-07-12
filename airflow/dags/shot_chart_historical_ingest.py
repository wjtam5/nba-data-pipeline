import os
import logging
from datetime import datetime, timedelta
from random import randint
import time

import pandas as pd
import json
import asyncio
#from nba_api.stats.endpoints import leaguedashplayerbiostats
from nba_api.stats.endpoints import commonteamroster
from nba_api.stats.endpoints import shotchartdetail

from airflow.decorators import dag,task
from common_tasks import get_teams, create_seasons_list, convert_to_parquet, upload_to_gcs, remove_local_file, create_external_table_bq, insert_external_table_bq, delete_external_table_bq

START_SEASON = 2018
END_SEASON = 2020

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID','not-found')
GCP_GCS_BUCKET = os.getenv('GCP_GCS_BUCKET', 'not-found')
GCP_GCS_BUCKET_SHOT_CHARTS_FOLDER = os.getenv('GCP_GCS_BUCKET_SHOT_CHARTS_FOLDER','not-found')
GCP_BQ_DATASET = os.getenv('GCP_BQ_DATASET_RAW','not-found')
TEMP_STORAGE_PATH = os.getenv('TEMP_STORAGE_PATH', 'not-found')
TODAYS_DATE = datetime.today().strftime('%Y_%m_%d')
FILE_NAME = f'shot_charts_{START_SEASON}_{END_SEASON}'
FILE_FORMAT = '.parquet'
LOCAL_FILE_PATH = f'{TEMP_STORAGE_PATH}/{FILE_NAME}{FILE_FORMAT}'
GCS_OBJECT_PATH = f'{GCP_GCS_BUCKET_SHOT_CHARTS_FOLDER}{FILE_NAME}'
TABLE_NAME = 'shot_charts'
CUSTOM_HEADERS = {
    'Host': 'stats.nba.com',
    'Connection': 'keep-alive',
    'Accept': 'application/json, text/plain, */*',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
 #   'Origin': 'https://stats.nba.com',
    'Referer': 'https://stats.nba.com/',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'x-nba-stats-origin': 'stats',
    'x-nba-stats-token': 'true'
 #   'Sec-Fetch-Site': 'same-origin',
 #   'Sec-Fetch-Mode': 'cors'
}

@dag(
    start_date = datetime.now() - timedelta(days=1),
    schedule_interval = '@once',
    catchup = False,
    max_active_runs = 1,
    tags = ["nba_data"],
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1
    }
)

def ingest_shot_charts_hist():
    
    @task()
    def get_player_info(seasons,teams,custom_headers):
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

        for season in seasons:
            for id in teams_id:
                try:
                    logging.info(f"Starting request for team_id:{id} on {season}")
                    common_team_roster_response = commonteamroster.CommonTeamRoster(season=season,team_id=id,headers=custom_headers)
                except Exception as e:
                    logging.info(f"Error with CommonTeamRoster endpoint.. Error: {e} with team_id:{id} on {season}")
                df = common_team_roster_response.common_team_roster.get_data_frame()
                df['SEASON_ID'] = season
                team_roster_data.append(df)
                logging.info(f"Finishing request for team_id:{id} on {season}")
                time.sleep(randint(1,5))

        team_roster_df = pd.concat(team_roster_data)

        team_roster_df = team_roster_df[['PLAYER_ID','SEASON_ID','TeamID']]

        players_dict = team_roster_df.to_dict('records')

        return players_dict

    async def shot_charts_response(sem,value,count,custom_headers):
        """
        Pass parameters to shot chart detail response

        Params:
            sem(int): Semaphore value to set number of concurrent workers
            value(str): Value from dict to access PLAYER_ID, SEASON_ID, & TEAM_ID for all active players in a given season
        
        Returns:
            df(DataFrame): DataFrame of shot chart metrics for a given player and game
        """
        async with sem:
            try:
                logging.info(f"Starting request for player_id:{value['PLAYER_ID']} on {value['SEASON_ID']}")

                shot_chart_detail_response = shotchartdetail.ShotChartDetail(team_id= value['TeamID'],
                                                player_id=value['PLAYER_ID'],
                                                season_nullable=value['SEASON_ID'],
                                                season_type_all_star=['Regular Season'],
                                                context_measure_simple="FGA",
                                                headers=custom_headers
                                                )

                df = shot_chart_detail_response.shot_chart_detail.get_data_frame()

                df['SEASON_ID'] = value['SEASON_ID']

                return df

            except (json.decoder.JSONDecodeError,Exception) as e:
                logging.info(f"Error with ShotChartDetail endpoint.. Error: {e} with player_id:{value['PLAYER_ID']} on {value['SEASON_ID']}") 
                pass

            finally:
                # Sleep when one worker is finished 
                random_time = randint(5,10)   
                logging.info(f"Sleeping for {random_time}")
                await asyncio.sleep(random_time)

                # Sleep every 50 requests to reduce endpoint throttling
                if (count % 50 == 0) & (count != 0):
                    logging.info(f"Iterated over {count} requests taking a longer break")
                    await asyncio.sleep(30)

                logging.info(f"Finishing request for player_id:{value['PLAYER_ID']} on {value['SEASON_ID']}")

    async def get_shot_charts(players,custom_headers):
        """
        Returns shooting metrics for a given player and their field goal attempts in any season

        Params:
            players(dict): Dict of PLAYER_ID, SEASON_ID, & TEAM_ID for all active players in a given season
        
        Returns:
             shot_chart_json(json): JSON file of shot chart metrics for a given player and game
        """
        shot_chart_data = []

        tasks = []
        
        # Pass in semaphore value to indicate number of concurrent workers
        sem = asyncio.BoundedSemaphore(value=3)

        for k,v in enumerate(players):
            shot_charts = shot_charts_response(sem,v,k,custom_headers)
            tasks.append(shot_charts)

        results = await asyncio.gather(*tasks)

        for result in results:
            shot_chart_data.append(result)
        
        shot_charts_df = pd.concat(shot_chart_data)

        shot_charts_json = shot_charts_df.to_json(orient='records')

        return shot_charts_json

    @task()
    def get_shot_charts_callable(players,custom_headers):
        """
        Execute asyncio.run() in function to call in airflow

        Params:
            players(dict): Dict of PLAYER_ID, SEASON_ID, & TEAM_ID for all active players in a given season
        
        Returns:
             shot_chart_json(json): JSON file of shot chart metrics for a given player and game
        """
        shot_charts_json = asyncio.run(get_shot_charts(players,custom_headers))

        return shot_charts_json
    
    # Functions (tasks) referenced below that are not defined above are in the common_tasks.py file
    teams_json = get_teams()
    seasons_list = create_seasons_list(START_SEASON,END_SEASON)
    players_dict = get_player_info(seasons_list,teams_json,CUSTOM_HEADERS)
    shot_charts_json = get_shot_charts_callable(players_dict,CUSTOM_HEADERS)
    shot_charts_pq = convert_to_parquet(shot_charts_json,LOCAL_FILE_PATH)
    shot_charts_upload = upload_to_gcs(GCP_GCS_BUCKET,GCS_OBJECT_PATH,LOCAL_FILE_PATH)
    shot_charts_remove_local = remove_local_file(LOCAL_FILE_PATH)
    shot_charts_create_external_table = create_external_table_bq(GCP_GCS_BUCKET,GCP_PROJECT_ID,GCP_BQ_DATASET,FILE_NAME,GCS_OBJECT_PATH)
    shot_charts_insert_external_table = insert_external_table_bq(GCP_BQ_DATASET,TABLE_NAME,FILE_NAME)
    shot_charts_delete_external_table = delete_external_table_bq(GCP_PROJECT_ID,GCP_BQ_DATASET,FILE_NAME)

    teams_json >> seasons_list >> players_dict >> shot_charts_json >> shot_charts_pq >> shot_charts_upload >> \
    shot_charts_remove_local >> shot_charts_create_external_table >> shot_charts_insert_external_table >> shot_charts_delete_external_table

ingest_shot_charts_hist()
