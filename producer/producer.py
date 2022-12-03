from kafka import KafkaProducer
import json

def main():

    ## Producer
    producer = KafkaProducer(retries=5,bootstrap_servers=['localhost:9092','localhost:9093'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        
   
    # Publish /Send
    i = 0
    while i < 2:
        ack = producer.send(topic='paiementMNPF',key= b'MTN', value={'refContrat': '255358642'})
        metadata= ack.get()
        print("Publish ... to topic PaiementMNPF ....")
        print(" topic = ",metadata.topic)
        print(" partition = ",metadata.partition)
    
    
    # Close
    producer.close()

if __name__ == '__main__':
    main()
    
    
    
    

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
    
