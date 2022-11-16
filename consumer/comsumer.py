
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

    """
    for message in consumer:

        print(message)
        print("Topic:  " + str(message[0]))
    """
    
    try:
        for message in consumer:
            print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,message.offset, message.key,message.value))
    except KeyboardInterrupt:
        sys.exit()

if __name__ == '__main__':


   subscriber('paiementMNPF')
   
   
   
   
   
   

# auto_offset_reset='earliest'  //earliest or latest
# enable_auto_commit=True // Auto : after consuming the message commit the offset.