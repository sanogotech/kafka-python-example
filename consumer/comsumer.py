
from kafka import KafkaConsumer
import json


def subscriber(topic):

    # Consumer
    consumer = KafkaConsumer(
    topic,
     bootstrap_servers=['localhost:9092', 'localhost:9093'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='saphir-group',
     value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    for message in consumer:

        print(message)
        print("Topic:  " + str(message[0]))
        

if __name__ == '__main__':


   subscriber('paiementMNPF')
   
   
   
   
   
   

# auto_offset_reset='earliest'  //earliest or latest
# enable_auto_commit=True // Auto : after consuming the message commit the offset.