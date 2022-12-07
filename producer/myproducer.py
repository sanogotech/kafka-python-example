from kafka import KafkaProducer
import json

def main():

    ## Producer
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda v: json.dumps(v).encode('utf-8'),max_request_size=20971520,api_version = (3, 3, 1))
    print("api version of client python =",producer.config['api_version'])
    
    #producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda v: json.dumps(v).encode('utf-8'),api_version = (3, 3, 1))
        
   
    # Publish /Send
    #i = 0
    #while i < 2:
    producer.send(topic='supervisionmonitoring',key= b'MTN', value={'refContrat': '255358642'})
    ##ack = producer.send(topic='paiementMNPF',key= b'MTN', value={'refContrat': '255358642'})
    # metadata= ack.get()
    ##producer.flush()
    #flush() will block until the previously sent messages have been delivered (or errored), effectively making the producer synchronous
    print("Publish ... to topic supervisionmonitoring ....")
    ##print(" topic = ",metadata.topic)
    ##print(" partition = ",metadata.partition)
    
    
    # Close
    producer.close()

if __name__ == '__main__':
    main()
    
    
    
    
# https://stackoverflow.com/questions/71741647/why-do-i-get-intermittent-nobrokersavailable-errors-with-python-application
# https://stackoverflow.com/questions/38854957/nobrokersavailable-nobrokersavailable-kafka-error
# https://stackoverflow.com/questions/57076780/how-to-determine-api-version-of-kafka
"""
I also hate reading "wall of text like" Kafka documentation :P
As far as I understand:

broker-list

a full list of servers, if any missing producer may not work
related to producer commands
bootstrap-servers

one is enough to discover all others
related to consumer commands
Zookeeper involved

"""

# produce keyed messages to enable hashed partitioning
# producer.send('my-topic', key=b'foo', value=b'bar')
     
#KafkaProducer( bootstrap_servers=KAFKA_BOOSTRAP_SERVERS, sasl_mechanism=KAFKA_SASL_MECHANISM, sasl_plain_username=KAFKA_USER, sasl_plain_password=KAFKA_PASSWORD)
    
