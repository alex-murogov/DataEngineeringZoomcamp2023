from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect import logging
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True, retries=3)
def load_from_url(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix some dtype issues"""
    if color == "yellow":
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    else:
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    return df


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f'data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
    gcs_block = GcsBucket.load('zoom-gcs')
    gcs_block.get_directory(from_path=gcs_path, local_path='../data')

    return Path(gcs_path)


@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame locally as parquet file"""
    data_dir = f'data/{color}'
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    path = Path(f'{data_dir}/{dataset_file}.parquet')
    df.to_parquet(path, compression='gzip')
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)


@task()
def write_bq(df: pd.DataFrame, destination_table) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load('zoom-gcp-creds')

    df.to_gbq(
        destination_table=destination_table,
        project_id='datatalks-data-course',
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists='append'
    )


@flow(log_prints=True)
def run_etl_git_gcs_bq(year: str, month: int, color: int, if_clean_dataset: bool) -> None:
    """The main ETL function"""

    logger = logging.get_run_logger()

    # 1 Extract data from DataTalksClub repo
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'
    df = load_from_url(dataset_url)
    logger.info(f"ETL for {dataset_file} dataset.")
    logger.info(f"NUMBER OF ROWS IN RAW FILE: {len(df)}.")

    # 2 Clean data
    if if_clean_dataset:
        df = clean(df, color)
        logger.info(f"NUMBER OF ROWS IN CLEANED DATASET: {len(df)}.")
    else:
        logger.info(f"CLEANING SKIPPED")

    # 3 Write DataFrame locally as parquet file
    path = write_local(df, color, dataset_file)

    # 4 Load data to Google Cloup Storage:
    write_gcs(path)
    logger.info(f"Data loaded to Google Cloup Storage. Path: {path}")

    # 5 LOAD FROM GCS AND SAVE TO BQ
    # 5.1 LOAD FROM GCS
    path = extract_from_gcs(color, year, month)
    df = pd.read_parquet(path)
    logger.info(f"PARQUET FILE LOADED FROM GCS")

    # 5.2 SAVE TO DWH table
    bq_table = 'dezoomcamp.homework'
    write_bq(df, bq_table)
    logger.info(f"DATA IS APPENDED TO DWH TABLE")


@flow()
def main_flow(months:int, year:list[int], color:str, cleaning:bool) -> None:
    """The main ETL flow"""
    for month in months:
        run_etl_git_gcs_bq(year, month, color, cleaning)


if __name__ == '__main__':
    color = 'green'
    months = [1,2,3,4]
    year = 2019
    cleaning = False

    main_flow(months, year, color, cleaning)
