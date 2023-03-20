import pandas as pd
import json
import datetime as dt
from time import sleep
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
print(f'Initialized Kafka producer at {dt.datetime.utcnow()}')

counter = 0
green_filename = "/data/green_tripdata_2019-01.csv.gz"
fhv_filename = "/data/fhv_tripdata_2019-01.csv.gz"


def produce(file_name, chunk_size):
    for chunk in pd.read_csv(file_name, compression="gzip", chunksize=chunk_size):
        if file_name.exist("fhv"):
            chunk["pickup_datetime"] = pd.to_datetime(chunk["pickup_datetime"])
            chunk["dropoff_datetime"] = pd.to_datetime(chunk["dropOff_datetime"])
        else:
            chunk["pickup_datetime"] = pd.to_datetime(chunk["lpep_pickup_datetime"])
            chunk["dropoff_datetime"] = pd.to_datetime(chunk["lpep_dropoff_datetime"])
        chunk = chunk[['PULocationID', 'pickup_datetime', 'dropoff_datetime']]
        key = str(counter).encode()

        # Convert the data frame chunk into a dictionary
        chunkd = chunk.to_dict()

        # Encode the dictionary into a JSON Byte Array
        data = json.dumps(chunkd, default=str).encode('utf-8')
        producer.send(topic="trips", key=key, value=data)

        # Sleep
        sleep(0.5)

        # Increment the message counter for the message key
        counter = counter + 1
        print(f'Sent record to topic at time {dt.datetime}')
