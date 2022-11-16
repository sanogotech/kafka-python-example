from kafka import KafkaProducer
import json

def main():

    ## Producer
    producer = KafkaProducer(bootstrap_servers=['localhost:9092','localhost:9093'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        
   
    # Publish /Send

    producer.send(topic='paiementMNPF',key= b'Orange', value={'refContrat': '255358644'})
    print("Publish ... to topic PaiementMNPF ....")
    
    # Close
    producer.close()

if __name__ == '__main__':
    main()
    
    
    
    
    


# produce keyed messages to enable hashed partitioning
# producer.send('my-topic', key=b'foo', value=b'bar')
     
#KafkaProducer( bootstrap_servers=KAFKA_BOOSTRAP_SERVERS, sasl_mechanism=KAFKA_SASL_MECHANISM, sasl_plain_username=KAFKA_USER, sasl_plain_password=KAFKA_PASSWORD)
    
