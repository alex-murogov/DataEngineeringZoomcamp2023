from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect import logging
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True, retries=3)
def load_from_url(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url, compression='gzip')
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

    # Change the dtypes setting so int values can be 0
    df = df.convert_dtypes()
    # defining the schema
    try:
        df = df.astype({'VendorID': 'int64',
                        'store_and_fwd_flag': 'string',
                        'passenger_count': 'int64',
                        'trip_distance': 'float64',
                        'RatecodeID': 'int64',
                        'PULocationID': 'int64',
                        'DOLocationID': 'int64',
                        'payment_type': 'int64',
                        'fare_amount': 'float64',
                        'ehail_fee': 'float64',
                        'extra': 'float64',
                        'mta_tax': 'float64',
                        'tip_amount': 'float64',
                        'trip_type': 'int64',
                        'mta_tax': 'float64',
                        'tolls_amount': 'float64',
                        'improvement_surcharge': 'float64',
                        'total_amount': 'float64',
                        'congestion_surcharge': 'float64'}, errors='ignore')
    except:
        print('error!', df.columns)

    if 'ehail_fee' in df.columns:
        df = df.drop('ehail_fee', axis=1)

    return df


@task()
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f'data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
    gcs_block = GcsBucket.load('zoom-gcs')
    gcs_block.get_directory(from_path=gcs_path, local_path='../data')

    return Path(gcs_path)


@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame locally as parquet file"""

    Path(f'data/{color}').mkdir(parents=True, exist_ok=True)
    path = Path(f'data/{color}/{dataset_file}.parquet')
    df.to_parquet(path, compression='gzip')


@task()
def write_to_gcs(path: Path) -> None:
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
def run_etl_git_gcs_bq(year, month, color, file_name, if_clean_dataset) -> None:
    """The main ETL function"""

    # logger = logging.get_run_logger()

    # 1 Extract data from DataTalksClub repo
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    # dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz'
    df = load_from_url(file_name)

    print(df.size)
    # logger.info(f"ETL for {dataset_file} dataset.")
    # logger.info(f"NUMBER OF ROWS IN RAW FILE: {len(df)}.")

    if color in ('green', 'yellow'):
        df = clean(df, color)
    else:
        df["PUlocationID"] = df["PUlocationID"].astype(float)
        df["DOlocationID"] = df["PUlocationID"].astype(float)

    # 3 Write DataFrame locally as parquet file
    write_local(df, color, dataset_file)
    path = f"data/{color}/{dataset_file}.parquet"

    # # 4 Load data to Google Cloup Storage:
    write_to_gcs(path)
    # logger.info(f"Data loaded to Google Cloup Storage. Path: {path}")




@flow()
def main(color, year, month, file_name, cleaning=False) -> None:
    """The main ETL flow"""
    try:
        run_etl_git_gcs_bq(year, month, color, file_name, cleaning)
    except Exception as e:
        print("\n\n\n", f"ERROR FOR: {year}, {month}, {color}", "\n\n\n")
        print("\n\n\n", e, "\n\n\n")


if __name__ == '__main__':

    setup = {
        # "green": {
        #     "months": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        #     "year": [2019, 2020],
        #     "link": "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/"
        # }
        # ,
        # # "yellow": {
        # #     "months": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        # #     "year": [2019, 2020],
        # #     "link": "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
        # # }
        # ,
        "fhv": {
            "months": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            "year": [2019],
            "link": "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/"
        }
    }

    # prefect orion start

    for color in setup:
        # print(color)
        for year in setup[color]['year']:
            # print(color,year)
            for month in setup[color]['months']:
                file_name = f"{setup[color]['link']}{color}_tripdata_{year}-{month:02}.csv.gz"
                print(color, year, month, '---', file_name)

                main(color, year, month, file_name)
