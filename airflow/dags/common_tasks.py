from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator,BigQueryInsertJobOperator,BigQueryDeleteTableOperator

import os
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

@task()
def get_teams():
    """
    Returns a static DataFrame of all active NBA teams 

    Params:
        None

    Returns:
        nba_teams_df(json): DataFrame of all NBA teams
    """
    import json
    from nba_api.stats.static import teams

    nba_teams = teams.get_teams()

    json_object = json.dumps(nba_teams)
        
    return json_object

@task()
def get_current_season(teams):
    """
    Return the most recent season using the TeamInfoCommon endpoint that tracks teams' performance for the current season (no other alternative endpoints to return nba schedule)

    Params: 
            teams (DataFrame): The list of NBA teams 
        
    Returns:
        current_season (str): Most recent NBA season
    """
    import time
    from random import randint
    from nba_api.stats.endpoints import teaminfocommon

    teams_df = pd.read_json(teams)

    teams_id = teams_df['id'].to_list()

    team_info_data = []

    for id in teams_id:
        try:
            logging.info(f"Starting request for team_id:{id}")
            team_info_common_response = teaminfocommon.TeamInfoCommon(league_id='00',team_id=id)
        except Exception as e:
            logging.info(f"Error with TeamInfoCommon endpoint.. Error: {e} with team_id:{id}")
        df = team_info_common_response.team_info_common.get_data_frame()
        team_info_data.append(df)
        logging.info(f"Finishing request for team_id:{id}")
        time.sleep(randint(1,5))

    team_info_df = pd.concat(team_info_data)

    current_season = team_info_df['SEASON_YEAR'].max()

    logging.info(f"The current season returned is {current_season}")

    return current_season

@task()
def create_seasons_list(start_season,end_season):
    """
    Return list of season(s) needed to pass in season_id parameter in various endpoints from the nba-api package

    Params: 
        start_season (int): The min year from desired season range

        end_season (int): The max year from desired season range
        
    Returns:
        season_list (list): A list of seasons based on start_season and end_season range
    """
    seasons_list = []

    for i in range(start_season,end_season):
        # Convert int to string and remove the first 2 characters in the second part of range (e.g. 2022 -> 22)
        season = str(i) + "-" + str(i+1)[2:]
        seasons_list.append(season)

    return seasons_list

@task()
def convert_to_parquet(json,file_path):
    """
    Convert DataFrame to Parquet file in airflow data folder

    Params:
        json(json): JSON file being converted to parquet
        file_path(str): The file path where file is being stored locally
    Returns:
        Parquet file from converted DataFrame
    """
    df = pd.read_json(json)

    table = pa.Table.from_pandas(df)

    return pq.write_table(table,file_path)

@task()
def upload_to_gcs(bucket,object_path,file_path):
    """
    Uploads parquet file in temp storage folder to GCS bucket

    Params:
        bucket(str): The GCS bucket that parquet file is being uploaded to
        object_path(str): The file path where object is stored in GCS
        file_path(str): The file path where file is being stored locally

    Returns:
        Parquet file uploaded to GCS
    """
    client = storage.Client()

    bucket = client.bucket(bucket)

    blob = bucket.blob(object_path)

    return blob.upload_from_filename(file_path)

@task()
def remove_local_file(file_path):
    """
    Removes file in local file directory

    Params:
        file_path(str): The file path where file is being stored locally

    Returns:
        Removes targeted file in the local temp storage folder
    """
    return os.remove(file_path)

def create_external_table_bq(bucket,project_id,dataset,file_name,object_path):
    """
    Creates an external table in BigQuery from GCS file

    Params:
        bucket(str): The DataFrame that is being converted
        project_id(str): The GCP project ID
        dataset(str): The name of BigQuery dataset
        file_name(str): The name of parquet file
        object_path(str): The file path where object is stored in GCS

    Returns:
        Creates an external table using file in GCS to BigQuery
    """

    return BigQueryCreateExternalTableOperator(
        task_id = f'create_external_table_bq',
        bucket = bucket,
        table_resource={
            "tableReference": {
                "projectId": project_id,
                "datasetId": dataset,
                "tableId": f'external_table_{file_name}',
            },
            "externalDataConfiguration": {
                'sourceFormat': "PARQUET",
                'sourceUris': [f'gs://{bucket}/{object_path}']
            }
        }
    )

def insert_external_table_bq(dataset,table_name,file_name):
    """
    Creates table in dataset using external table generated

    Params:
        dataset(str): The name of BigQuery dataset
        table_name(str): The name of the table in dataset
        file_name(str): The name of parquet file

    Returns:
        Insert data using external table into table in BQ
    """
    return BigQueryInsertJobOperator(
        task_id = f'insert_external_table_query',
        configuration = {
            'query': {
                'query': f'CREATE OR REPLACE TABLE {dataset}.{table_name} AS SELECT * FROM {dataset}.external_table_{file_name};',
                'useLegacySql': False
            }
        }
    )

def delete_external_table_bq(project_id,dataset,file_name):
    """
    Delete external table in bigquery dataset

    Params:
        project_id(str): The GCP project ID
        dataset(str): The name of BigQuery dataset
        table_name(str): The name of the table in dataset

    Returns:
        Removes external table generated in bigquery dataset
    """
    return BigQueryDeleteTableOperator(
        task_id = f'delete_external_table_bq',
        deletion_dataset_table = f'{project_id}.{dataset}.external_table_{file_name}',
        ignore_if_missing = True
    )

