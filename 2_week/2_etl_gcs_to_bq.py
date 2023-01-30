# from pathlib import Path
#
# import pandas as pd
# from prefect import flow, task
# from prefect_gcp.cloud_storage import GcsBucket
#
#
# @task(retries=3)
# def load_data_from_url(dataset_url: str) -> pd.DataFrame:
#     """Read taxi data from web into pandas DataFrame"""
#     df = pd.read_csv(dataset_url)
#     return df
#
#
# @task(log_prints=True)
# def clean_data(df: pd.DataFrame) -> pd.DataFrame:
#     """Fix dtype issues"""
#     df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
#     df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
#     # print(df.head(5))
#     print(f'columns: {df.dtypes}')
#     print(f'rows: {len(df)}')
#     return df
#
#
# @task()
# def write_local(df: pd.DataFrame, sub_dir: str, dataset_file: str) -> Path:
#     """Write DataFrame out as parquet file"""
#     data_dir = f'data/{sub_dir}'
#     Path(data_dir).mkdir(parents=True, exist_ok=True)
#     path = Path(f'{data_dir}/{dataset_file}.parquet')
#     df.to_parquet(path, compression='gzip')
#     return path
#
#
# @task()
# def write_gcs(path: Path) -> None:
#     """Upload local parquet file to GCS"""
#     # gcp_bucket_name = "dtc_data_lake_datatalks-data-course"
#     # gcp_cloud_storage_bucket_block = GcsBucket.load(gcp_bucket_name)
#     gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
#     gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)
#
#
# @flow()
# def etl_web_to_gcs(color, year, month) -> None:
#     """The main ETL function"""
#     dataset_file = f'{color}_tripdata_{year}-{month:02}'
#     dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'
#     # print(dataset_url)
#
#     df_raw = load_data_from_url(dataset_url)
#     df_clean = clean_data(df_raw)
#
#     path = write_local(df_clean, color, dataset_file)
#     # print(path)
#
#     write_gcs(path)
#
#
# if __name__ == '__main__':
#     color = 'yellow'
#     year = 2021
#     month = 1
#     etl_web_to_gcs(color, year, month)
import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f'data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
    gcs_block = GcsBucket.load('zoom-gcs')
    gcs_block.get_directory(from_path=gcs_path, local_path='../data')

    return Path(gcs_path)


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load('zoom-gcp-creds')

    df.to_gbq(
        destination_table='dezoomcamp.rides',
        project_id='datatalks-data-course',
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists='append'
    )

# from prefect_gcp import GcpCredentials
# gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

@flow()
def etl_gcs_to_bq(color, year, month):
    """Main ETL flow to load data into BigQuery"""

    path = extract_from_gcs(color, year, month)
    print(path)
    df = transform(path)
    write_bq(df)


if __name__ == '__main__':
    color = 'yellow'
    year = 2021
    month = 1
    etl_gcs_to_bq(color, year, month)
