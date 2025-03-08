from confluent_kafka import Producer
import json
import pandas as pd
from time import sleep

conf = {'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'Y7YUKEAO3K6GAXII',
        'sasl.password': 'd/k+JLejbE7KomhegdFmQBRcR+Bzafslr5ZA/YA1aPbdcCEiq4pz/IYCqgZhITKf'
        }

producer=Producer(conf)
df=pd.read_csv('X:\kafka_project\practice\darshil_project\indexProcessed.csv')
dict_stock=df.sample(1).to_dict(orient='records')[0]
# print(dict_stock)
# producer.produce('topic_21',value=str(dict_stock))
while True:
    dict_stock=df.sample(1).to_dict(orient='records')[0]
    producer.produce('topic_22',value=str(dict_stock))
    sleep(1)
    #hdid

producer.flush()