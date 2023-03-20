from kafka import KafkaConsumer
import json
import pandas as pd

consumer = KafkaConsumer('trips',
                         group_id='test-consumer-group',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='earliest',
                         enable_auto_commit=False
                         )

for message in consumer:

    pdf = pd.DataFrame(message.value)

    total = pdf['PULocationID'].value_counts()

    print(total)
