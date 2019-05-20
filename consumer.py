from kafka import KafkaConsumer
from json import loads
import os
import pandas as pd

consumer = KafkaConsumer(
    'worldcitiesdata', #topic name
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')))

folder_path = os.path.join(os.getcwd(), 'dataset-after-kafka')
file_path = os.path.join(folder_path, 'result.txt')
writefile = open(file_path, "w", encoding="utf-8")
for message in consumer:
    message = message.value
    writefile.write(message)
    print(message)