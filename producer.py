from time import sleep
from json import dumps
from kafka import KafkaProducer
import os
import pandas as pd

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

# writefile = open('result.txt', "w")
#Load Dataset
counter = -1
dataset_folder_path = os.path.join(os.getcwd(), 'dataset')
dataset_file_path = os.path.join(dataset_folder_path, 'worldcitiespop.csv')
with open(dataset_file_path,"rt", encoding="utf-8") as f:
    for row in f:
        counter += 1
        if counter == 0:
            continue
        # writefile.write(row)
        producer.send('worldcitiesdata', value=row)
        print(row)
        sleep(0.001)
