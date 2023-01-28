import pandas as pd
from sqlalchemy import create_engine
import argparse

import wget
from os import path


def download_file(url):
    file_name = url.split('/')[-1]

    if path.exists(file_name):
        print('file already exists locally')
        downloaded_filename = file_name
    else:
        downloaded_filename = wget.download(url)
        print(f'{downloaded_filename}  downloaded!!!')

    return downloaded_filename


def load_file_to_pd(downloaded_filename):
    # df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = pd.read_csv(downloaded_filename)

    row_count, column_count = df.shape

    print('file loaded')
    print(f'row_count: {row_count}, column_count: {column_count}')

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    return df


def connect_to_db(user, password, host, port, db):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    conn_dsdb = engine.connect()
    print('connection successfull:', conn_dsdb)

    return conn_dsdb, engine


def print_df_schema(df, engine):
    print(pd.io.sql.get_schema(df, 'yellow_taxi_data', con=engine))


def write_data_to_db(df, table_name, engine):
    status = df.to_sql(name=table_name, con=engine, if_exists='replace')
    print(status)


def test_data_in_db(conn_dsdb, table_name, limit_rows):
    # select some data from DDB
    for i in conn_dsdb.exec_driver_sql(f"select * from {table_name} limit {limit_rows};"):
        print(i, '\n')


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # 1
    downloaded_filename = download_file(url)

    # 2
    df = load_file_to_pd(downloaded_filename)

    # 3
    conn_dsdb, engine = connect_to_db(user, password, host, port, db)

    # 4
    print_df_schema(df, engine)

    # 5
    write_data_to_db(df, table_name, engine)

    # 6
    test_data_in_db(conn_dsdb, table_name, 5)

    print('job completed')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='Homework1', description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)
