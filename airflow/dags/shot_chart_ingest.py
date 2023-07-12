import os
import logging
from datetime import datetime, timedelta
from random import randint
import time

import pandas as pd
import json
import asyncio
#from nba_api.stats.endpoints import leaguedashplayerbiostats
from nba_api.stats.endpoints import scoreboardv2
from nba_api.stats.endpoints import boxscoretraditionalv2
from nba_api.stats.endpoints import shotchartdetail
from nba_api.stats.endpoints import commonteamroster

from airflow.decorators import dag,task
from common_tasks import get_teams, get_current_season, convert_to_parquet, upload_to_gcs, remove_local_file, create_external_table_bq, insert_external_table_bq, delete_external_table_bq

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID','not-found')
GCP_GCS_BUCKET = os.getenv('GCP_GCS_BUCKET', 'not-found')
GCP_GCS_BUCKET_SHOT_CHARTS_FOLDER = os.getenv('GCP_GCS_BUCKET_SHOT_CHARTS_FOLDER','not-found')
GCP_BQ_DATASET = os.getenv('GCP_BQ_DATASET_RAW','not-found')
TEMP_STORAGE_PATH = os.getenv('TEMP_STORAGE_PATH', 'not-found')
TODAYS_DATE = datetime.today().strftime('%Y_%m_%d')
FILE_NAME = f'shot_charts_{TODAYS_DATE}'
FILE_FORMAT = '.parquet'
LOCAL_FILE_PATH = f'{TEMP_STORAGE_PATH}/{FILE_NAME}{FILE_FORMAT}'
GCS_OBJECT_PATH = f'{GCP_GCS_BUCKET_SHOT_CHARTS_FOLDER}{FILE_NAME}'
TABLE_NAME = 'shot_charts'

@dag(
    start_date = datetime.now() - timedelta(days=1),
    schedule_interval = '40 17 * * *',
    catchup = False,
    max_active_runs = 1,
    tags = ["nba_data"],
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1
    }
)

def ingest_shot_charts():

    @task()
    def get_games():
        """
        Retrieve all game ids from the previous day

        Params:
            None
        
        Returns:
            current_season (str): Most recent NBA season
        """
        today_date = datetime.today().strftime('%Y-%m-%d')

        try:
            scoreboard_response = scoreboardv2.ScoreboardV2(day_offset=-1,game_date=today_date,league_id='00')

        except Exception as e:
            logging.info(f"Error with Scoreboard endpoint.. Error: {e}")

        scoreboard_df = scoreboard_response.available.get_data_frame()

        game_id_list = scoreboard_df['GAME_ID'].to_list()

        return game_id_list
    
    @task()
    def get_players(games):
        """
        Pass a list of game ids to retrieve player ids and team ids associated to those games

        Params: 
            games(list): The list of game ids from previous day
        
        Returns:
            box_score_json (json): All players in associated games including respective team id
        """   
        box_score_data = []

        for game in games:
            try:
                logging.info(f"Starting request for game_id:{game}")
                box_score_response = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game)
            except Exception as e:
                logging.info(f"Error with BoxScoreTraditional endpoint.. Error: {e} on game_id:{game}")
            df = box_score_response.player_stats.get_data_frame()
            box_score_data.append(df)
            logging.info(f"Finishing request for game_id:{game}")
            time.sleep(randint(1,5))

        box_score_df = pd.concat(box_score_data)

        # Filter out inactive/injured players and players with 0 FG attempts
        box_score_df = box_score_df[(box_score_df['FGA'].notna() & box_score_df['FGA'] != 0)]

        box_score_df = box_score_df[['PLAYER_ID','TEAM_ID']]

        # Return output as dictionary to loop in ShotChartDetail endpoint
        #players_dict = box_score_df.to_dict('records')

        box_score_json = box_score_df.to_json(orient='records')

        #return players_dict

        return box_score_json
        
    @task()
    def check_exists_player_info(teams,season,players):
        """
        Check against CommonTeamRoster endpoint to determine if players in associated games are on the team roster (ex: not on 10-day contract)

        Params: 
            teams(json): The list of NBA teams
            season(str): The current season
            players(json): All players in associated games including respective team id 
        
        Returns:
            players_dict (dict): All players in associated games if player is on the team roster
        """ 

        teams_df = pd.read_json(teams)

        teams_id = teams_df['id'].to_list()

        team_roster_data = []

        for id in teams_id:
            try:
                logging.info(f"Starting request for team_id:{id} on {season}")
                common_team_roster_response = commonteamroster.CommonTeamRoster(season=season,team_id=id)
            except Exception as e:
                logging.info(f"Error with CommonTeamRoster endpoint.. Error: {e} with team_id:{id}")
            df = common_team_roster_response.common_team_roster.get_data_frame()
            team_roster_data.append(df)
            logging.info(f"Finishing request for team_id:{id} on {season}")
            time.sleep(randint(1,5))

        team_roster_df = pd.concat(team_roster_data)

        team_roster_df = team_roster_df[['PLAYER_ID']]

        player_check_df = pd.read_json(players)

        # Create flag that checks if player_id exists in teamroster endpoint
        player_check_df['EXISTS'] = player_check_df['PLAYER_ID'].isin(team_roster_df['PLAYER_ID']).astype(int)

        player_check_df = player_check_df[player_check_df['EXISTS'] == 1]

        players_dict = player_check_df.to_dict('records')

        return players_dict
    
    async def shot_charts_response(sem,value,count):
        """
        Pass parameters to shot chart detail response

        Params:
            sem(int): Semaphore value to set number of concurrent workers
            value(str): Value from dict to access PLAYER_ID, SEASON_ID, & TEAM_ID for all active players 
        
        Returns:
            df(DataFrame): DataFrame of shot chart metrics for a given player and game
        """
        async with sem:

            # Retrieve only yesterday's games
            yesterday_date = datetime.today() - timedelta(days=1)

            yesterday_date = yesterday_date.strftime('%Y-%m-%d')

            try:
                logging.info(f"Starting request to retrieve player_id:{value['PLAYER_ID']}")

                shot_chart_detail_response = shotchartdetail.ShotChartDetail(team_id= value['TEAM_ID'],
                                                player_id=value['PLAYER_ID'],
                                                season_type_all_star=['Regular Season'],
                                                date_from_nullable=yesterday_date,
                                                date_to_nullable=yesterday_date,
                                                context_measure_simple="FGA")

                df = shot_chart_detail_response.shot_chart_detail.get_data_frame()

                return df
            
            except (json.decoder.JSONDecodeError,Exception) as e:
                logging.info(f"Error with ShotChartDetail endpoint.. Error: {e} with {value['PLAYER_ID']}") 
                pass

            finally:        
                # Sleep when one worker is finished 
                random_time = randint(5,10)   
                logging.info(f'Sleeping for {random_time} seconds')
                await asyncio.sleep(random_time)

                # Sleep every 50 requests to reduce endpoint throttling
                if (count % 50 == 0) & (count != 0):
                    logging.info(f'Iterated over {count} requests taking a longer break')
                    await asyncio.sleep(30)
                    
                logging.info(f"Finishing request for player_id:{value['PLAYER_ID']}")
        

    async def get_shot_charts(players,season):
        """
        Returns shooting metrics for a given player and their field goal attempts in any season

        Params:
            players_dict(dict): Dict of PLAYER_ID & TEAM_ID for all active players
            season(str): The current NBA season
        
        Returns:
            shot_chart_json(json): JSON file of shot chart metrics for a given player and game
        """
        shot_chart_data = []

        tasks = []
        
        # Pass in semaphore value to indicate number of concurrent workers
        sem = asyncio.BoundedSemaphore(value=3)

        for k,v in enumerate(players):
            shot_charts = shot_charts_response(sem,v,k)
            tasks.append(shot_charts)
    
        results = await asyncio.gather(*tasks)

        for result in results:
            shot_chart_data.append(result)
        
        shot_charts_df = pd.concat(shot_chart_data)

        shot_charts_df['SEASON_ID'] = season

        shot_charts_json = shot_charts_df.to_json(orient='records')

        return shot_charts_json

    @task()
    def get_shot_charts_callable(players,season):
        """
        Execute asyncio.run() in function to call in airflow

        Params:
            players(dict): Dict of PLAYER_ID, SEASON_ID, & TEAM_ID for all active players
        
        Returns:
            shot_chart_json(json): JSON file of shot chart metrics for a given player and game
        """
        shot_charts_json = asyncio.run(get_shot_charts(players,season))

        return shot_charts_json

    # Functions (tasks) referenced below that are not defined above are in the common_tasks.py file
    teams_json = get_teams()
    current_season = get_current_season(teams_json)
    games_list = get_games()
    box_score_json = get_players(games_list)
    players_dict = check_exists_player_info(teams_json,current_season,box_score_json)
    shot_charts_json = get_shot_charts_callable(players_dict,current_season)
    shot_charts_pq = convert_to_parquet(shot_charts_json,LOCAL_FILE_PATH)
    shot_charts_upload = upload_to_gcs(GCP_GCS_BUCKET,GCS_OBJECT_PATH,LOCAL_FILE_PATH)
    shot_charts_remove_local = remove_local_file(LOCAL_FILE_PATH)
    shot_charts_create_external_table = create_external_table_bq(GCP_GCS_BUCKET,GCP_PROJECT_ID,GCP_BQ_DATASET,FILE_NAME,GCS_OBJECT_PATH)
    shot_charts_insert_external_table = insert_external_table_bq(GCP_BQ_DATASET,TABLE_NAME,FILE_NAME)
    shot_charts_delete_external_table = delete_external_table_bq(GCP_PROJECT_ID,GCP_BQ_DATASET,FILE_NAME)

    teams_json >> current_season >> games_list >> players_dict >> shot_charts_json >> shot_charts_pq >> shot_charts_upload >> shot_charts_remove_local >> \
    shot_charts_create_external_table >> shot_charts_insert_external_table >> shot_charts_delete_external_table

ingest_shot_charts()